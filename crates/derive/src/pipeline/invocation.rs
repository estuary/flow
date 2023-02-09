use crate::{DocCounter, StatsAccumulator};
use anyhow::Context;
use bytes::BufMut;
use futures::channel::oneshot;
use futures::{future::LocalBoxFuture, FutureExt};
use prost::Message;
use proto_flow::flow::{self, derive_api};
use serde_json::Value;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

// Invocation of a lambda which is in the process of building built.
#[derive(Clone)]
pub enum Invocation {
    Noop,
    // Trampoline invocations accumulate raw, owned buffers which are
    // trampolined over the CGO bridge for dispatch by Go.
    Trampoline {
        sources: Vec<u8>,
        registers: Vec<u8>,
    },
    Sqlite {
        lambda: Rc<RefCell<sqlite_lambda::Lambda<serde_json::Value>>>,
        sources: Vec<Value>,
        registers: Vec<Value>,
        previous_registers: Vec<Option<Value>>,
    },
    // Desired future Invocation variants:
    // WASM : accumulate within a WASM Memory for zero-copy transfer.
    // Deno : accumulate by deserializing directly into Deno V8 types -- which also resolves the present BigInt / number dilemma.
}

impl Invocation {
    pub fn new(
        spec: Option<&flow::LambdaSpec>,
        source_projections: &[flow::Projection],
        register_projections: &[flow::Projection],
    ) -> anyhow::Result<Self> {
        if let Some(spec) = spec {
            if spec.remote != "" || spec.typescript != "" {
                Ok(Self::Trampoline {
                    sources: Vec::new(),
                    registers: Vec::new(),
                })
            } else if spec.sqlite != "" {
                let lambda = sqlite_lambda::Lambda::new(
                    &spec.sqlite,
                    &source_projections
                        .iter()
                        .map(Into::into)
                        .collect::<Vec<_>>(),
                    &register_projections
                        .iter()
                        .map(Into::into)
                        .collect::<Vec<_>>(),
                )?;

                Ok(Self::Sqlite {
                    lambda: Rc::new(RefCell::new(lambda)),
                    sources: Vec::new(),
                    registers: Vec::new(),
                    previous_registers: Vec::new(),
                })
            } else {
                panic!("invalid lambda spec")
            }
        } else {
            Ok(Invocation::Noop)
        }
    }

    pub fn add_source(&mut self, data: &[u8]) {
        use Invocation::*;
        match self {
            Noop => {}
            Trampoline { sources: buf, .. } => {
                if buf.is_empty() {
                    buf.reserve(BUFFER_RESERVE);
                } else {
                    buf.put_u8(b','); // Continue column.
                }
                buf.extend_from_slice(data); // Write document row.
            }
            Sqlite { sources, .. } => {
                sources.push(serde_json::from_slice(data).unwrap());
            }
        }
    }

    pub fn begin_register(&mut self, previous: &Value) {
        use Invocation::*;
        match self {
            Noop => {}
            Trampoline { registers: buf, .. } => {
                if buf.is_empty() {
                    buf.reserve(BUFFER_RESERVE);
                } else {
                    buf.put_u8(b','); // Continue column.
                }
                buf.put_u8(b'['); // Start a new register row.
                serde_json::to_writer(buf.writer(), previous).unwrap();
            }
            Sqlite { registers, .. } => {
                registers.push(previous.clone());
            }
        }
    }

    pub fn end_register(&mut self, updated: Option<&Value>) {
        use Invocation::*;
        match self {
            Noop => {}
            Trampoline { registers: buf, .. } => {
                if let Some(updated) = updated {
                    buf.put_u8(b','); // Continue register row.
                    serde_json::to_writer(buf.writer(), updated).unwrap();
                }
                buf.put_u8(b']'); // Complete register row.
            }
            Sqlite {
                registers,
                previous_registers,
                ..
            } => {
                if let Some(updated) = updated {
                    let r = registers.pop().unwrap();
                    previous_registers.push(Some(r));
                    registers.push(updated.clone());
                } else {
                    previous_registers.push(None);
                }
            }
        }
    }

    // Invoke consumes the Invocation and returns a parsed response future.
    // Depending on the lambda runtime, the future may be immediately resolve-able.
    pub fn invoke<'a>(
        self,
        tf_index: usize,
        trampoline: &cgo::Trampoline,
    ) -> LocalBoxFuture<'a, Result<InvokeOutput, anyhow::Error>> {
        use Invocation::*;
        match self {
            Trampoline { sources, registers } if !sources.is_empty() => {
                // Start measuring the time it takes to invoke the lambdas. Technically, this
                // measurement can include some "extra" time, for example if the trampoline server
                // delays the execution of the task. It is intended for this measurement to be
                // inclusive of that type of thing, rather than try to narrow in on only the time
                // spent executing the users' code.
                let start = Instant::now();
                let request = derive_api::Invoke {
                    transform_index: tf_index as u32,
                    sources_memptr: sources.as_ptr() as u64,
                    sources_length: sources.len() as u64,
                    registers_memptr: registers.as_ptr() as u64,
                    registers_length: registers.len() as u64,
                };

                let (tx, rx) = oneshot::channel();

                trampoline.start_task(
                    derive_api::Code::TrampolineInvoke as u32,
                    move |arena: &mut Vec<u8>| request.encode_raw(arena),
                    move |result: Result<&[u8], anyhow::Error>| {
                        // Move into closure to pin until the operation completes.
                        let (_, _) = (sources, registers);
                        let total_duration = start.elapsed();
                        let result = result.and_then(|data| {
                            let parsed: Vec<Vec<Value>> = serde_json::from_slice(data)
                                .context("failed to parse lambda invocation response")?;
                            let stats = InvokeStats {
                                output: DocCounter::new(
                                    parsed.iter().map(Vec::len).sum::<usize>() as u32,
                                    // TODO: _technically_ we _could_ subtract some bytes here to
                                    // account for the square brackets used in array encoding. I'm
                                    // not sure whether we should or not.
                                    data.len() as u32,
                                ),
                                total_duration,
                            };
                            Ok(InvokeOutput { parsed, stats })
                        });
                        tx.send(result).unwrap();
                    },
                );
                rx.map(|r| r.unwrap()).boxed_local()
            }
            Sqlite {
                lambda,
                sources,
                registers,
                previous_registers,
            } => {
                assert_eq!(registers.len(), previous_registers.len());
                let mut lambda = lambda.borrow_mut();
                let start = Instant::now();

                let output = if registers.is_empty() {
                    sources
                        .iter()
                        .map(|source| Ok(lambda.invoke(source, None, None)?))
                        .collect::<anyhow::Result<Vec<Vec<Value>>>>()
                } else {
                    sources
                        .iter()
                        .zip(registers.iter().zip(previous_registers.iter()))
                        .map(|(source, (register, previous_register))| {
                            let previous_register = match previous_register.as_ref() {
                                Some(r) => r,
                                None => register,
                            };

                            Ok(lambda.invoke(source, Some(register), Some(previous_register))?)
                        })
                        .collect::<anyhow::Result<Vec<Vec<Value>>>>()
                };

                let total_duration = start.elapsed();

                let output = output.map(|parsed| {
                    let stats = InvokeStats {
                        output: DocCounter::new(
                            parsed.iter().map(Vec::len).sum::<usize>() as u32,
                            // TODO(johnny): This is *super* gross -- we're serializing
                            // these documents simply to know how long that serialization is
                            // for stats. We quite literally would otherwise never serialize
                            // these documents, since they're directly fed into registers or
                            // the combiner.
                            //
                            // I'm going to hold my nose here because we plan to deprecate
                            // this invocation mechanism as we enable V2 derivations.
                            serde_json::to_vec(&parsed).unwrap().len() as u32,
                        ),
                        total_duration,
                    };

                    InvokeOutput { parsed, stats }
                });
                async { output }.boxed_local()
            }

            Noop => async { Ok(InvokeOutput::default()) }.boxed_local(),
            // Trampoline without any documents is treated as a no-op.
            Trampoline { .. } => async { Ok(InvokeOutput::default()) }.boxed_local(),
        }
    }
}

#[derive(Default, Debug, PartialEq)]
pub struct InvokeStats {
    pub output: DocCounter,
    pub total_duration: Duration,
}

impl StatsAccumulator for InvokeStats {
    type Stats = flow::derive_api::stats::InvokeStats;

    fn drain(&mut self) -> Self::Stats {
        let total_seconds = self.total_duration.as_secs_f64();
        self.total_duration = Duration::default();
        flow::derive_api::stats::InvokeStats {
            output: Some(self.output.drain()),
            total_seconds,
        }
    }
}

impl InvokeStats {
    pub fn add(&mut self, other: &InvokeStats) {
        self.output.add(&other.output);
        self.total_duration += other.total_duration;
    }
}

#[derive(Default, Debug, PartialEq)]
pub struct InvokeOutput {
    pub parsed: Vec<Vec<Value>>,
    pub stats: InvokeStats,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::DocCounter;
    use flow::DocsAndBytes;
    use prost::Message;
    use serde_json::json;

    #[test]
    fn test_invoke_stats() {
        let mut stats = InvokeStats::default();

        stats.add(&InvokeStats {
            output: DocCounter::new(3, 99),
            total_duration: Duration::from_secs(3),
        });
        stats.add(&InvokeStats {
            output: DocCounter::new(1, 51),
            total_duration: Duration::from_secs(1),
        });

        let actual = stats.drain();
        // I really just can't be bothered to add a library or write another approximate equality
        // function, so we get this.
        assert_eq!(4, actual.total_seconds.round() as i64);
        assert_eq!(
            Some(DocsAndBytes {
                docs: 4,
                bytes: 150,
            }),
            actual.output
        );
        // drain again and assert it's all zeroed
        let actual = stats.drain();
        assert_eq!(0f64, actual.total_seconds);
        assert_eq!(Some(DocsAndBytes { docs: 0, bytes: 0 }), actual.output);
    }

    #[test]
    fn test_trampoline() {
        let mut inv = Invocation::new(
            Some(&flow::LambdaSpec {
                typescript: "/a/lambda".to_owned(),
                remote: String::new(),
                sqlite: String::new(),
            }),
            &[],
            &[],
        )
        .unwrap();

        // An empty invocation immediately resolves when invoked.
        let trampoline = cgo::Trampoline::new();
        let fut = inv.clone().invoke(42, &trampoline);
        let result = futures::executor::block_on(fut);
        assert_eq!(result.unwrap(), InvokeOutput::default());

        // Assemble the invocation body. Sources first, then registers.
        inv.add_source(json!({"a": "source"}).to_string().as_bytes());
        inv.add_source(json!({"next": "42"}).to_string().as_bytes());

        inv.begin_register(&json!("prev"));
        inv.end_register(Some(&json!("next")));
        inv.begin_register(&json!([64]));
        inv.end_register(None);

        if let Invocation::Trampoline { sources, registers } = &inv {
            assert_eq!(sources, br#"{"a":"source"},{"next":"42"}"#);
            assert_eq!(registers, br#"["prev","next"],[[64]]"#);
        } else {
            unreachable!();
        }

        // Invoke and dispatch via trampoline.
        let fut = inv.clone().invoke(42, &trampoline);

        let mut arena = Vec::new();
        let mut out = Vec::new();
        trampoline.dispatch_tasks(123, &mut arena, &mut out);

        let invoke = derive_api::Invoke::decode(&arena[12..]).unwrap();
        assert_eq!(invoke.transform_index, 42);

        // Resolve task, and poll to complete future.
        let mut data = vec![0, 0, 0, 0, 0, 0, 0, 0, 1]; // 8 bytes id, one byte status.
        data.extend_from_slice(json!([["foobar"]]).to_string().as_bytes());
        trampoline.resolve_task(&data);

        let result = futures::executor::block_on(fut);
        assert_eq!(result.unwrap().parsed, vec![vec![json!("foobar")]]);

        // Now try an error.
        let fut = inv.clone().invoke(42, &trampoline);
        trampoline.dispatch_tasks(123, &mut arena, &mut out);

        let data = vec![1, 0, 0, 0, 0, 0, 0, 0, 0, b'e', b'r', b'r'];
        trampoline.resolve_task(&data);

        let result = futures::executor::block_on(fut);
        assert_eq!(&format!("{}", result.unwrap_err()), "err");
    }

    #[test]
    fn test_noop() {
        let mut inv = Invocation::new(None, &[], &[]).unwrap();

        // Add documents to the invocation.
        inv.add_source(json!({"a": "source"}).to_string().as_bytes());
        inv.begin_register(&json!(["reg"]));
        inv.end_register(None);

        // Expect it immediately resolves to empty array.
        let trampoline = cgo::Trampoline::new();
        let fut = inv.clone().invoke(42, &trampoline);
        let result = futures::executor::block_on(fut);
        assert_eq!(result.unwrap(), InvokeOutput::default());
    }
}

// Initial reservation size for lazy reservations of invocation buffers.
const BUFFER_RESERVE: usize = 4096;
