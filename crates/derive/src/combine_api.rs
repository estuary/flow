use super::combiner::{self, Combiner};
use allocator::ThreadStatsReader;

use doc::{Pointer, Validator};
use prost::Message;
use protocol::{
    cgo,
    flow::combine_api::{self, Code},
};
use serde_json::Value;
use tuple::{TupleDepth, TuplePack};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("parsing URL")]
    Url(#[from] url::ParseError),
    #[error("schema index")]
    SchemaIndex(#[from] json::schema::index::Error),
    #[error("JSON error in document: {}", String::from_utf8_lossy(&.doc))]
    Json {
        doc: Vec<u8>,
        #[source]
        source: serde_json::Error,
    },
    #[error(transparent)]
    CombineError(#[from] combiner::Error),
    #[error("Protobuf decoding error")]
    ProtoDecode(#[from] prost::DecodeError),
    #[error(transparent)]
    UTF8Error(#[from] std::str::Utf8Error),
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
    #[error("combined key cannot be empty")]
    EmptyKey,
    #[error("protocol error (invalid state or invocation)")]
    InvalidState,
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

/// API provides a combine capability as a cgo::Service.
pub struct API {
    state: Option<State>,
    // Running tally of the total number of bytes allocated during all invocations of this API.
    allocated_bytes: i64,
}

struct State {
    combiner: Combiner,
    fields: Vec<Pointer>,
    uuid_placeholder_ptr: String,
    validator: Validator<'static>,
}

impl cgo::Service for API {
    type Error = Error;

    fn create() -> Self {
        Self {
            state: None,
            allocated_bytes: 0,
        }
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
        let mem_stats = ThreadStatsReader::new();
        let initial = mem_stats.current();

        let result = match (code, std::mem::take(&mut self.state)) {
            (Code::Configure, _) => {
                let combine_api::Config {
                    schema_index_memptr,
                    schema_uri,
                    key_ptr,
                    field_ptrs,
                    uuid_placeholder_ptr,
                } = combine_api::Config::decode(data)?;
                tracing::debug!(
                schema_index = ?schema_index_memptr,
                schema_uri = ?schema_uri,
                key_ptr = ?key_ptr,
                "configuring combiner"
                       );

                tracing::debug!(
                    ?schema_index_memptr,
                    ?schema_uri,
                    ?key_ptr,
                    ?field_ptrs,
                    ?uuid_placeholder_ptr,
                    "configure",
                );

                // Re-hydrate a &'static SchemaIndex from a provided memory address.
                let schema_index_memptr = schema_index_memptr as usize;
                let schema_index: &doc::SchemaIndex =
                    unsafe { std::mem::transmute(schema_index_memptr) };

                let schema = url::Url::parse(&schema_uri)?;
                schema_index.must_fetch(&schema)?;

                let key_ptrs: Vec<Pointer> = key_ptr.iter().map(Pointer::from).collect();
                if key_ptrs.is_empty() {
                    return Err(Error::EmptyKey);
                }

                self.state = Some(State {
                    combiner: Combiner::new(schema, key_ptrs.into()),
                    validator: Validator::new(schema_index),
                    fields: field_ptrs.iter().map(Pointer::from).collect(),
                    uuid_placeholder_ptr,
                });
                Ok(())
            }
            (Code::ReduceLeft, Some(mut state)) => {
                let doc: Value = serde_json::from_slice(data).map_err(|e| Error::Json {
                    doc: data.to_vec(),
                    source: e,
                })?;
                state.combiner.reduce_left(doc, &mut state.validator)?;

                self.state = Some(state);
                Ok(())
            }
            (Code::CombineRight, Some(mut state)) => {
                let doc: Value = serde_json::from_slice(data).map_err(|e| Error::Json {
                    doc: data.to_vec(),
                    source: e,
                })?;
                state.combiner.combine_right(doc, &mut state.validator)?;

                self.state = Some(state);
                Ok(())
            }
            (Code::Drain, Some(mut state)) => {
                tracing::debug!(
                allocated_before_drain = ?self.allocated_bytes,
                "memory before draining"
                           );
                drain_combiner(
                    &mut state.combiner,
                    &state.uuid_placeholder_ptr,
                    &state.fields,
                    arena,
                    out,
                );

                self.state = Some(state);
                Ok(())
            }
            _ => Err(Error::InvalidState),
        };

        // Get the difference in memory allocations for this invocation, and use it to update our
        // running tally.
        let diff = (mem_stats.current() - initial).total_allocated();
        self.allocated_bytes += diff;
        tracing::trace!(allocated_bytes = ?self.allocated_bytes, allocated_bytes_delta = ?diff, "mem usage");
        result
    }
}

pub fn drain_combiner(
    combiner: &mut Combiner,
    uuid_placeholder_ptr: &str,
    field_ptrs: &[Pointer],
    arena: &mut Vec<u8>,
    out: &mut Vec<cgo::Out>,
) {
    let key_ptrs = combiner.key().clone();
    tracing::debug!(
        arena_len = ?arena.len(),
        combiner_len = ?combiner.len(),
        ?key_ptrs,
        ?field_ptrs,
        uuid_placeholder_ptr,
        "drain_combiner",
    );
    for (doc, fully_reduced) in combiner.drain_entries(uuid_placeholder_ptr) {
        // Send serialized document.
        let begin = arena.len();
        let w: &mut Vec<u8> = &mut *arena;
        serde_json::to_writer(w, &doc).expect("encoding cannot fail");

        if fully_reduced {
            cgo::send_bytes(Code::DrainedReducedDocument as u32, begin, arena, out);
        } else {
            cgo::send_bytes(Code::DrainedCombinedDocument as u32, begin, arena, out);
        }

        // Send packed key.
        let begin = arena.len();
        let mut null_count = 0;
        for p in key_ptrs.iter() {
            let v = p.query(&doc).unwrap_or(&Value::Null);
            if v.is_null() {
                null_count += 1;
            }
            // Unwrap because pack() returns io::Result, but Vec<u8> is infallible.
            let _ = v.pack(arena, TupleDepth::new().increment()).unwrap();
        }
        if null_count == key_ptrs.len() {
            tracing::debug!(out_pos = ?out.len(), arena_pos = ?begin, doc = ?doc, "extracted null key")
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
}

#[cfg(test)]
pub mod test {
    use super::{super::test::build_min_max_sum_schema, Code, Error, API};
    use protocol::{cgo::Service, flow::combine_api};
    use serde_json::json;

    #[test]
    fn test_combine_api() {
        // Not covered: opening the database and building a schema index.
        // Rather, we install a fixture here.
        let (index, schema_url) = build_min_max_sum_schema();

        let mut svc = API::create();
        let mut arena = Vec::new();
        let mut out = Vec::new();

        // Configure the service.
        svc.invoke_message(
            Code::Configure as u32,
            combine_api::Config {
                schema_index_memptr: index as *const doc::SchemaIndex<'static> as u64,
                schema_uri: schema_url.as_str().to_owned(),
                key_ptr: vec!["/key".to_owned()],
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

        insta::assert_debug_snapshot!((String::from_utf8_lossy(&arena), out));
    }

    #[test]
    fn test_combine_empty_key() {
        let (index, schema_url) = build_min_max_sum_schema();

        let mut svc = API::create();
        let mut arena = Vec::new();
        let mut out = Vec::new();

        assert!(matches!(
            svc.invoke_message(
                Code::Configure as u32,
                combine_api::Config {
                    schema_index_memptr: index as *const doc::SchemaIndex<'static> as u64,
                    schema_uri: schema_url.as_str().to_owned(),
                    key_ptr: vec![],
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
