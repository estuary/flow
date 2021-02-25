use super::combiner::{self, Combiner};

use doc::{Pointer, SchemaIndex};
use prost::Message;
use protocol::{
    cgo,
    flow::combine_api::{self, Code},
};
use serde_json::Value;
use tuple::{TupleDepth, TuplePack};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("parsing URL: {0:?}")]
    Url(#[from] url::ParseError),
    #[error("schema index: {0}")]
    SchemaIndex(#[from] json::schema::index::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    CombineError(#[from] combiner::Error),
    #[error("Protobuf decoding error")]
    ProtoDecode(#[from] prost::DecodeError),
    #[error(transparent)]
    UTF8Error(#[from] std::str::Utf8Error),
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
    #[error("protocol error (invalid state or invocation)")]
    InvalidState,
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

/// API provides a combine capability as a cgo::Service.
pub struct API {
    state: Option<(combine_api::Config, Combiner)>,
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
        tracing::trace!(?code, "invoke");

        let code = match Code::from_i32(code as i32) {
            Some(c) => c,
            None => return Err(Error::InvalidState),
        };

        match (code, &mut self.state) {
            (Code::Configure, None) => {
                let cfg = combine_api::Config::decode(data)?;

                // Re-hydrate a &'static SchemaIndex from a provided memory address.
                let index_ptr = cfg.schema_index_memptr as usize;
                let index: &'static SchemaIndex = unsafe { std::mem::transmute(index_ptr) };

                let schema_url = url::Url::parse(&cfg.schema_uri)?;
                index.must_fetch(&schema_url)?;

                let key_ptrs: Vec<Pointer> = cfg.key_ptr.iter().map(Pointer::from).collect();
                let combiner = Combiner::new(index, &schema_url, key_ptrs.into());

                self.state = Some((cfg, combiner));
                Ok(())
            }
            (Code::ReduceLeft, Some((_, combiner))) => {
                let doc: Value = serde_json::from_slice(data)?;
                combiner.reduce_left(doc)?;
                Ok(())
            }
            (Code::CombineRight, Some((_, combiner))) => {
                let doc: Value = serde_json::from_slice(data)?;
                combiner.combine_right(doc)?;
                Ok(())
            }
            (Code::Drain, Some(_)) => {
                let (req, combiner) = self.state.take().unwrap();

                drain_combiner(
                    combiner,
                    &req.uuid_placeholder_ptr,
                    &req.field_ptrs.iter().map(Pointer::from).collect::<Vec<_>>(),
                    arena,
                    out,
                );
                Ok(())
            }
            _ => Err(Error::InvalidState),
        }
    }
}

pub fn drain_combiner(
    combiner: Combiner,
    uuid_placeholder_ptr: &str,
    field_ptrs: &[Pointer],
    arena: &mut Vec<u8>,
    out: &mut Vec<cgo::Out>,
) {
    let key_ptrs = combiner.key().clone();

    for (doc, fully_reduced) in combiner.into_entries(uuid_placeholder_ptr) {
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
        for p in key_ptrs.iter() {
            let v = p.query(&doc).unwrap_or(&Value::Null);
            // Unwrap because pack() returns io::Result, but Vec<u8> is infallible.
            let _ = v.pack(arena, TupleDepth::new().increment()).unwrap();
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
    use super::{super::test::build_min_max_sum_schema, Code, API};
    use protocol::{cgo::Service, flow::combine_api};
    use serde_json::json;

    #[test]
    fn test_combine_api() {
        let mut svc = API::create();

        // Not covered: opening the database and building a schema index.
        // Rather, we install a fixture here.
        let (index, schema_url) = build_min_max_sum_schema();

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
}
