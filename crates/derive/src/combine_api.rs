use super::{
    combiner::{self, Combiner},
    ValidatorGuard,
};
use crate::{DocCounter, JsonError, StatsAccumulator};
use prost::Message;
use proto_flow::flow::combine_api::{self, Code};
use serde_json::Value;
use tuple::{TupleDepth, TuplePack};

#[derive(thiserror::Error, Debug, serde::Serialize)]
pub enum Error {
    #[error("parsing URL")]
    #[serde(serialize_with = "crate::serialize_as_display")]
    Url(#[from] url::ParseError),
    #[error("schema index")]
    #[serde(serialize_with = "crate::serialize_as_display")]
    SchemaIndex(#[from] json::schema::index::Error),
    #[error(transparent)]
    Json(JsonError),
    #[error(transparent)]
    CombineError(#[from] combiner::Error),
    #[error("Protobuf decoding error")]
    #[serde(serialize_with = "crate::serialize_as_display")]
    ProtoDecode(#[from] prost::DecodeError),
    #[error(transparent)]
    #[serde(serialize_with = "crate::serialize_as_display")]
    UTF8Error(#[from] std::str::Utf8Error),
    #[error("combined key cannot be empty")]
    EmptyKey,
    #[error("protocol error (invalid state or invocation)")]
    InvalidState,
    #[error(transparent)]
    #[serde(serialize_with = "crate::serialize_as_display")]
    Anyhow(#[from] anyhow::Error),
}

/// API provides a combine capability as a cgo::Service.
pub struct API {
    state: Option<State>,
}

#[derive(Debug, Default)]
struct CombineStats {
    left: DocCounter,
    right: DocCounter,
    out: DocCounter,
}

impl StatsAccumulator for CombineStats {
    type Stats = combine_api::Stats;
    fn drain(&mut self) -> Self::Stats {
        combine_api::Stats {
            left: Some(self.left.drain()),
            right: Some(self.right.drain()),
            out: Some(self.out.drain()),
        }
    }
}

struct State {
    combiner: Combiner,
    field_ptrs: Vec<doc::Pointer>,
    guard: ValidatorGuard,
    key_ptrs: Vec<doc::Pointer>,
    stats: CombineStats,
    uuid_placeholder_ptr: Option<doc::Pointer>,
}

impl cgo::Service for API {
    type Error = Error;

    fn create() -> Self {
        Self { state: None }
    }

    fn invoke(
        &mut self,
        code: u32,
        data: &[u8],
        arena: &mut Vec<u8>,
        out: &mut Vec<cgo::Out>,
    ) -> Result<(), Self::Error> {
        let code = match Code::from_i32(code as i32) {
            Some(c) => c,
            None => return Err(Error::InvalidState),
        };

        tracing::trace!(?code, "invoke");
        match (code, std::mem::take(&mut self.state)) {
            (Code::Configure, _) => {
                let combine_api::Config {
                    schema_json,
                    key_ptrs,
                    field_ptrs,
                    uuid_placeholder_ptr,
                } = combine_api::Config::decode(data)?;
                tracing::debug!(
                    %schema_json,
                    ?key_ptrs,
                    ?field_ptrs,
                    ?uuid_placeholder_ptr,
                    "configure",
                );

                let key_ptrs: Vec<doc::Pointer> = key_ptrs.iter().map(doc::Pointer::from).collect();
                if key_ptrs.is_empty() {
                    return Err(Error::EmptyKey);
                }
                let field_ptrs: Vec<doc::Pointer> =
                    field_ptrs.iter().map(doc::Pointer::from).collect();

                let uuid_placeholder_ptr = match uuid_placeholder_ptr.as_ref() {
                    "" => None,
                    s => Some(doc::Pointer::from(s)),
                };

                let guard = ValidatorGuard::new(&schema_json)?;
                let combiner = Combiner::new(guard.schema.curi.clone(), key_ptrs.clone().into());

                self.state = Some(State {
                    guard,
                    combiner,
                    key_ptrs,
                    field_ptrs,
                    uuid_placeholder_ptr,
                    stats: CombineStats::default(),
                });
                Ok(())
            }
            (Code::ReduceLeft, Some(mut state)) => {
                state.stats.left.increment(data.len() as u32);
                let doc: Value = serde_json::from_slice(data)
                    .map_err(|e| Error::Json(JsonError::new(data, e)))?;
                state
                    .combiner
                    .reduce_left(doc, &mut state.guard.validator)?;

                self.state = Some(state);
                Ok(())
            }
            (Code::CombineRight, Some(mut state)) => {
                state.stats.right.increment(data.len() as u32);
                let doc: Value = serde_json::from_slice(data)
                    .map_err(|e| Error::Json(JsonError::new(data, e)))?;
                state
                    .combiner
                    .combine_right(doc, &mut state.guard.validator)?;

                self.state = Some(state);
                Ok(())
            }
            (Code::Drain, Some(mut state)) => {
                let drain_stats = drain_combiner(
                    &mut state.combiner,
                    &state.key_ptrs,
                    &state.field_ptrs,
                    state.uuid_placeholder_ptr.as_ref(),
                    arena,
                    out,
                );
                state.stats.out = drain_stats;
                // Send a final message with the stats that were accumulated during this
                // transaction.
                cgo::send_message(Code::Stats as u32, &state.stats.drain(), arena, out);

                self.state = Some(state);
                Ok(())
            }
            _ => Err(Error::InvalidState),
        }
    }
}

// Drains the `combiner` into the given `arena` and `out`, and returns stats on the documents that
// were output from the combiner.
pub fn drain_combiner(
    combiner: &mut Combiner,
    key_ptrs: &[doc::Pointer],
    field_ptrs: &[doc::Pointer],
    uuid_placeholder_ptr: Option<&doc::Pointer>,
    arena: &mut Vec<u8>,
    out: &mut Vec<cgo::Out>,
) -> DocCounter {
    let mut stats = DocCounter::default();

    tracing::debug!(
        arena_len = ?arena.len(),
        combiner_len = ?combiner.len(),
        ?key_ptrs,
        ?field_ptrs,
        ?uuid_placeholder_ptr,
        "drain_combiner",
    );

    for (doc, fully_reduced) in combiner.drain_entries() {
        encode_combiner_entry(
            doc,
            fully_reduced,
            key_ptrs,
            field_ptrs,
            uuid_placeholder_ptr,
            arena,
            out,
            &mut stats,
        );
    }
    stats
}

pub fn encode_combiner_entry(
    mut doc: serde_json::Value,
    fully_reduced: bool,
    key_ptrs: &[doc::Pointer],
    field_ptrs: &[doc::Pointer],
    uuid_placeholder_ptr: Option<&doc::Pointer>,
    arena: &mut Vec<u8>,
    out: &mut Vec<cgo::Out>,
    stats: &mut DocCounter,
) {
    // Optionally add a document UUID placeholder value.
    if let Some(ptr) = &uuid_placeholder_ptr {
        if let Some(uuid_value) = ptr.create(&mut doc) {
            *uuid_value = Value::String(UUID_PLACEHOLDER.to_owned());
        }
    }

    // Send serialized document.
    let begin = arena.len();
    let w: &mut Vec<u8> = &mut *arena;
    serde_json::to_writer(w, &doc).expect("encoding cannot fail");
    // Only here do we know the actual length of the document in its serialized form.
    stats.increment((arena.len() - begin) as u32);
    if fully_reduced {
        cgo::send_bytes(Code::DrainedReducedDocument as u32, begin, arena, out);
    } else {
        cgo::send_bytes(Code::DrainedCombinedDocument as u32, begin, arena, out);
    }

    // Send packed key.
    let begin = arena.len();
    // Update arena_len with each component of the key so that we can assert that every
    // component of the key extends the arena by at least one byte.
    // This was added in response to: https://github.com/estuary/flow/issues/238
    let mut prev_arena_len = begin;
    for p in key_ptrs.iter() {
        let v = p.query(&doc).unwrap_or(&Value::Null);
        // Unwrap because pack() returns io::Result, but Vec<u8> is infallible.
        let _ = v.pack(arena, TupleDepth::new().increment()).unwrap();
        if arena.len() <= prev_arena_len {
            panic!(
                "encoding key wrote 0 bytes, pointer: {:?}, extracted value: {:?}, doc: {}",
                p, v, doc
            );
        }
        prev_arena_len = arena.len();
    }
    cgo::send_bytes(Code::DrainedKey as u32, begin, arena, out);

    // Send packed additional fields.
    let begin = arena.len();
    for p in field_ptrs {
        let v = p.query(&doc).unwrap_or(&Value::Null);
        let _ = v.pack(arena, TupleDepth::new().increment()).unwrap();
    }
    cgo::send_bytes(Code::DrainedFields as u32, begin, arena, out);
}

#[cfg(test)]
pub mod test {
    use super::super::test::build_min_max_sum_schema;
    use super::{Code, Error, API};
    use cgo::Service;
    use prost::Message;
    use proto_flow::flow::{
        combine_api::{self, Stats},
        DocsAndBytes,
    };
    use serde_json::json;

    #[test]
    fn test_combine_api() {
        let mut svc = API::create();
        let mut arena = Vec::new();
        let mut out = Vec::new();

        // Configure the service.
        svc.invoke_message(
            Code::Configure as u32,
            combine_api::Config {
                schema_json: build_min_max_sum_schema(),
                key_ptrs: vec!["/key".to_owned()],
                field_ptrs: vec!["/min".to_owned(), "/max".to_owned()],
                uuid_placeholder_ptr: "/foo".to_owned(),
            },
            &mut arena,
            &mut out,
        )
        .unwrap();

        // Send documents to combine.
        for (left, doc) in &[
            (true, json!({"key": "one", "min": 3, "max": 3.3})),
            (false, json!({"key": "two", "min": 4, "max": 4.4})),
            (true, json!({"key": "two", "min": 2, "max": 2.2})),
            (false, json!({"key": "one", "min": 5, "max": 5.5})),
            (false, json!({"key": "three", "min": 6, "max": 6.6})),
        ] {
            svc.invoke(
                if *left {
                    Code::ReduceLeft
                } else {
                    Code::CombineRight
                } as u32,
                serde_json::to_vec(doc).unwrap().as_ref(),
                &mut arena,
                &mut out,
            )
            .unwrap();
        }

        // Expect nothing's been written to out / arena yet.
        assert!(arena.is_empty());
        assert!(out.is_empty());

        // Drain the combiner.
        svc.invoke(Code::Drain as u32, &[], &mut arena, &mut out)
            .unwrap();
        // The last message in out should be stats
        let stats_out = out.pop().expect("missing stats");
        assert_eq!(Code::Stats as u32, stats_out.code);

        let stats_message = Stats::decode(&arena[stats_out.begin as usize..])
            .expect("failed to decode stats message");
        let expected_stats = Stats {
            left: Some(DocsAndBytes { docs: 2, bytes: 62 }),
            right: Some(DocsAndBytes { docs: 3, bytes: 95 }),
            out: Some(DocsAndBytes {
                docs: 3,
                bytes: 230,
            }),
        };
        assert_eq!(expected_stats, stats_message);

        // Don't include the stats message in the snapshot here because it's binary encoded, and we
        // already asserted that it matches our expectations.
        insta::assert_debug_snapshot!((
            String::from_utf8_lossy(&arena[..stats_out.begin as usize]),
            out
        ));
    }

    #[test]
    fn test_combine_empty_key() {
        let mut svc = API::create();
        let mut arena = Vec::new();
        let mut out = Vec::new();

        assert!(matches!(
            svc.invoke_message(
                Code::Configure as u32,
                combine_api::Config {
                    schema_json: build_min_max_sum_schema(),
                    key_ptrs: vec![],
                    field_ptrs: vec![],
                    uuid_placeholder_ptr: String::new(),
                },
                &mut arena,
                &mut out,
            ),
            Err(Error::EmptyKey)
        ));
    }
}

// This constant is shared between Rust and Go code.
// See go/protocols/flow/document_extensions.go.
pub const UUID_PLACEHOLDER: &str = "DocUUIDPlaceholder-329Bb50aa48EAa9ef";
