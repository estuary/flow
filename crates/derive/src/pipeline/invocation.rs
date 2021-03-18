use anyhow::Context;
use bytes::BufMut;
use futures::channel::oneshot;
use futures::{future::LocalBoxFuture, FutureExt};
use prost::Message;
use protocol::{
    cgo,
    flow::{self, derive_api},
};
use serde_json::Value;

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
    // Desired future Invocation variants:
    // WASM : accumulate within a WASM Memory for zero-copy transfer.
    // Deno : accumulate by deserializing directly into Deno V8 types -- which also resolves the present BigInt / number dilemma.
}

impl Invocation {
    pub fn new(spec: Option<&flow::LambdaSpec>) -> Self {
        if let Some(spec) = spec {
            if spec.remote != "" || spec.typescript != "" {
                Self::Trampoline {
                    sources: Vec::new(),
                    registers: Vec::new(),
                }
            } else {
                panic!("invalid lambda spec")
            }
        } else {
            Invocation::Noop
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
        }
    }

    // Invoke consumes the Invocation and returns a parsed response future.
    // Depending on the lambda runtime, the future may be immediately resolve-able.
    pub fn invoke<'a>(
        self,
        tf_index: usize,
        trampoline: &cgo::Trampoline,
    ) -> LocalBoxFuture<'a, Result<Vec<Vec<Value>>, anyhow::Error>> {
        use Invocation::*;
        match self {
            Trampoline { sources, registers } if !sources.is_empty() => {
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

                        let result = result.and_then(|data| {
                            let parsed: Vec<Vec<Value>> = serde_json::from_slice(data)
                                .context("failed to parse lambda invocation response")?;
                            Ok(parsed)
                        });
                        tx.send(result).unwrap();
                    },
                );
                rx.map(|r| r.unwrap()).boxed_local()
            }

            Noop => async { Ok(Vec::new()) }.boxed_local(),
            // Trampoline without any documents is treated as a no-op.
            Trampoline { .. } => async { Ok(Vec::new()) }.boxed_local(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{cgo, derive_api, flow, Invocation};
    use prost::Message;
    use serde_json::{json, Value};

    #[test]
    fn test_trampoline() {
        let mut inv = Invocation::new(Some(&flow::LambdaSpec {
            typescript: "/a/lambda".to_owned(),
            remote: String::new(),
        }));

        // An empty invocation immediately resolves when invoked.
        let trampoline = cgo::Trampoline::new();
        let fut = inv.clone().invoke(42, &trampoline);
        let result = futures::executor::block_on(fut);
        assert_eq!(result.unwrap(), Vec::<Vec<Value>>::new());

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
        assert_eq!(result.unwrap(), vec![vec![json!("foobar")]]);

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
        let mut inv = Invocation::new(None);

        // Add documents to the invocation.
        inv.add_source(json!({"a": "source"}).to_string().as_bytes());
        inv.begin_register(&json!(["reg"]));
        inv.end_register(None);

        // Expect it immediately resolves to empty array.
        let trampoline = cgo::Trampoline::new();
        let fut = inv.clone().invoke(42, &trampoline);
        let result = futures::executor::block_on(fut);
        assert_eq!(result.unwrap(), Vec::<Vec<Value>>::new());
    }
}

// Initial reservation size for lazy reservations of invocation buffers.
const BUFFER_RESERVE: usize = 4096;
