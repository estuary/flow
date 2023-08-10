use crate::{new_validator, DebugJson, DocCounter, JsonError, StatsAccumulator};
use anyhow::Context;
use bytes::Buf;
use doc::shape::{limits::enforce_field_count_limits, schema::to_schema};
use prost::Message;
use proto_flow::flow::combine_api::{self, Code};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("parsing URL")]
    Url(#[from] url::ParseError),
    #[error("schema index")]
    SchemaIndex(#[from] json::schema::index::Error),
    #[error(transparent)]
    Json(JsonError),
    #[error(transparent)]
    CombineError(#[from] doc::combine::Error),
    #[error("Protobuf decoding error")]
    ProtoDecode(#[from] prost::DecodeError),
    #[error(transparent)]
    UTF8Error(#[from] std::str::Utf8Error),
    #[error("combined key cannot be empty")]
    EmptyKey,
    #[error(transparent)]
    Extractor(#[from] extractors::Error),
    #[error("protocol error (invalid state or invocation)")]
    InvalidState,
    #[error(transparent)]
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
    // Combiner which is doing the heavy lifting.
    combiner: doc::Combiner,
    // Fields which are extracted and returned from combined documents.
    fields_ex: Vec<doc::Extractor>,
    // Document key components over which we're grouping while combining.
    key_ex: Box<[doc::Extractor]>,
    // Inferred shape of drained documents. Some if schema inference
    // is confitured to be enabled, otherwise None
    shape: Option<doc::Shape>,
    // Keeps track of whether the running inferred shape was widened
    // since last being logged.
    shape_changed: bool,
    // Statistics of a current combine operation.
    stats: CombineStats,
    // JSON-Pointer into which a UUID placeholder should be set,
    // or None if a placeholder shouldn't be set.
    uuid_placeholder_ptr: Option<doc::Pointer>,
    collection_name: String,
}

impl cgo::Service for API {
    type Error = Error;

    fn create() -> Self {
        Self { state: None }
    }

    fn invoke(
        &mut self,
        code: u32,
        mut data: &[u8],
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
                    fields,
                    uuid_placeholder_ptr,
                    projections,
                    collection_name,
                    infer_schema_json,
                } = combine_api::Config::decode(data)?;
                tracing::debug!(
                    %schema_json,
                    ?key_ptrs,
                    ?fields,
                    ?uuid_placeholder_ptr,
                    ?collection_name,
                    schema_inference = infer_schema_json.len()>0,
                    "configure",
                );

                if key_ptrs.is_empty() {
                    return Err(Error::EmptyKey);
                }
                let key_ex: Box<[_]> = extractors::for_key(&key_ptrs, &projections)?.into();
                let fields_ex = extractors::for_fields(&fields, &projections)?;

                let uuid_placeholder_ptr = match uuid_placeholder_ptr.as_ref() {
                    "" => None,
                    s => Some(doc::Pointer::from(s)),
                };

                let validator = new_validator(&schema_json)?;

                let combiner = doc::Combiner::new(
                    key_ex.clone(),
                    None,
                    tempfile::tempfile().context("opening temporary spill file")?,
                    validator,
                )?;

                // If `infer_schema_json` is non-empty then enable schema inference
                // by setting Shape to Some, and use the schema defined by `infer_schema_json`
                // as the default schema to start from. Otherwise, disable schema inference.
                // TODO (jshearer): We're special-casing "false" here because at the moment,
                // the Go side of things doesn't actually know how to load a useful inferred schema
                // and instead always passes the maximally-restrictive "false" value. Once we teach
                // it how to fetch useful inferred schemas, we should remove this special case.
                let shape = if infer_schema_json.eq("false") {
                    self.state
                        .as_ref()
                        .and_then(|state| state.shape.clone())
                        .or(Some(doc::Shape::nothing()))
                } else if infer_schema_json.len() > 0 {
                    let validator = new_validator(infer_schema_json.as_str())?;
                    Some(doc::Shape::infer(
                        &validator.schemas()[0],
                        validator.schema_index(),
                    ))
                } else {
                    None
                };
                self.state = Some(State {
                    combiner,
                    shape,
                    shape_changed: false,
                    fields_ex,
                    key_ex,
                    uuid_placeholder_ptr,
                    stats: CombineStats::default(),
                    collection_name,
                });
                Ok(())
            }
            (Code::ReduceLeft, Some(mut state)) => {
                let accumulator = match &mut state.combiner {
                    doc::Combiner::Accumulator(accumulator) => accumulator,
                    doc::Combiner::Drainer(_) => {
                        return Err(anyhow::anyhow!("combiner is draining").into())
                    }
                };
                state.stats.left.increment(data.len());

                let memtable = accumulator.memtable()?;
                let doc = parse_node_with_placeholder(memtable, data, &state.uuid_placeholder_ptr)?;
                memtable.add(doc, true)?;

                self.state = Some(state);
                Ok(())
            }
            (Code::CombineRight, Some(mut state)) => {
                let accumulator = match &mut state.combiner {
                    doc::Combiner::Accumulator(accumulator) => accumulator,
                    doc::Combiner::Drainer(_) => {
                        return Err(anyhow::anyhow!("combiner is draining").into())
                    }
                };
                state.stats.right.increment(data.len());

                let memtable = accumulator.memtable()?;
                let doc = parse_node_with_placeholder(memtable, data, &state.uuid_placeholder_ptr)?;
                memtable.add(doc, false)?;

                self.state = Some(state);
                Ok(())
            }
            (Code::DrainChunk, Some(mut state)) if data.len() == 4 => {
                let mut drainer = match state.combiner {
                    doc::Combiner::Accumulator(accum) => accum.into_drainer()?,
                    doc::Combiner::Drainer(d) => d,
                };

                let more = drain_chunk(
                    &mut drainer,
                    data.get_u32() as usize,
                    &state.key_ex,
                    &state.fields_ex,
                    arena,
                    out,
                    &mut state.stats.out,
                    &mut state.shape,
                    &mut state.shape_changed,
                )?;

                if !more {
                    if let Some(ref shape) = state.shape {
                        if state.shape_changed {
                            state.shape_changed = false;

                            let serialized = serde_json::to_value(&to_schema(shape.clone()))
                                .expect("shape serialization should never fail");

                            tracing::info!(
                                schema = ?DebugJson(serialized),
                                collection_name = state.collection_name,
                                "inferred schema updated"
                            );
                        }
                    }

                    // Send a final message with accumulated stats.
                    cgo::send_message(Code::DrainedStats as u32, &state.stats.drain(), arena, out);
                    state.combiner = doc::Combiner::Accumulator(drainer.into_new_accumulator()?);
                } else {
                    state.combiner = doc::Combiner::Drainer(drainer);
                }

                self.state = Some(state);
                Ok(())
            }
            _ => Err(Error::InvalidState),
        }
    }
}

fn parse_node_with_placeholder<'m>(
    memtable: &'m doc::combine::MemTable,
    data: &[u8],
    uuid_placeholder_ptr: &Option<doc::Pointer>,
) -> Result<doc::HeapNode<'m>, Error> {
    let mut doc = doc::HeapNode::from_serde(
        &mut serde_json::Deserializer::from_slice(data),
        memtable.alloc(),
    )
    .map_err(|e| Error::Json(JsonError::new(data, e)))?;

    if let Some(ptr) = uuid_placeholder_ptr.as_ref() {
        if let Some(node) = ptr.create_heap_node(&mut doc, memtable.alloc()) {
            *node =
                doc::HeapNode::String(doc::BumpStr::from_str(UUID_PLACEHOLDER, memtable.alloc()));
        }
    }
    Ok(doc)
}

// Drain a chunk of the Drainer into the given buffers up to the target length.
// Stats are accumulated into the provided DocCounter.
pub fn drain_chunk(
    drainer: &mut doc::combine::Drainer,
    target_length: usize,
    key_ex: &[doc::Extractor],
    fields_ex: &[doc::Extractor],
    arena: &mut Vec<u8>,
    out: &mut Vec<cgo::Out>,
    stats: &mut DocCounter,
    shape: &mut Option<doc::Shape>,
    did_change: &mut bool,
) -> Result<bool, doc::combine::Error> {
    // Convert target from a delta to an absolute target length of the arena.
    let target_length = target_length + arena.len();

    drainer.drain_while(|doc, fully_reduced| {
        if let Some(ref mut shape) = shape {
            let changed = match &doc {
                doc::LazyNode::Node(n) => shape.widen(*n),
                doc::LazyNode::Heap(h) => shape.widen(h),
            };
            if changed {
                enforce_field_count_limits(shape, json::Location::Root);
                *did_change = true;
            }
        }
        // Send serialized document.
        let begin = arena.len();
        let w: &mut Vec<u8> = &mut *arena;
        serde_json::to_writer(w, &doc).expect("encoding cannot fail");
        // Only here do we know the actual length of the document in its serialized form.
        stats.increment(arena.len() - begin);
        if fully_reduced {
            cgo::send_bytes(Code::DrainedReducedDocument as u32, begin, arena, out);
        } else {
            cgo::send_bytes(Code::DrainedCombinedDocument as u32, begin, arena, out);
        }

        // Send packed key followed by packed additional fields.
        for (code, extractors) in [(Code::DrainedKey, key_ex), (Code::DrainedFields, fields_ex)] {
            let begin = arena.len();
            match &doc {
                doc::LazyNode::Heap(n) => {
                    for ex in extractors {
                        ex.extract(n, arena).unwrap();
                    }
                }
                doc::LazyNode::Node(n) => {
                    for ex in extractors {
                        ex.extract(*n, arena).unwrap();
                    }
                }
            }
            cgo::send_bytes(code as u32, begin, arena, out);
        }

        // Keep going if we have remaining arena length.
        Ok::<_, doc::combine::Error>(arena.len() < target_length)
    })
}

#[cfg(test)]
pub mod test {
    use super::super::test::build_min_max_sum_schema;
    use super::{Code, Error, API};
    use cgo::Service;
    use itertools::Itertools;
    use prost::Message;
    use proto_flow::flow::{
        self,
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
                fields: vec!["min".to_owned(), "max".to_owned()],
                uuid_placeholder_ptr: "/foo".to_owned(),
                projections: vec![
                    flow::Projection {
                        field: "key".to_string(),
                        ptr: "/key".to_string(),
                        inference: Some(flow::Inference::default()),
                        ..Default::default()
                    },
                    flow::Projection {
                        field: "max".to_string(),
                        ptr: "/max".to_string(),
                        inference: Some(flow::Inference::default()),
                        ..Default::default()
                    },
                    flow::Projection {
                        field: "min".to_string(),
                        ptr: "/min".to_string(),
                        inference: Some(flow::Inference::default()),
                        ..Default::default()
                    },
                ],
                collection_name: "test".to_string(),
                infer_schema_json: "false".to_string(),
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

        // Poll to drain one document from the combiner.
        svc.invoke(
            Code::DrainChunk as u32,
            &(1 as u32).to_be_bytes(),
            &mut arena,
            &mut out,
        )
        .unwrap();

        assert_eq!(out.len(), 1 * 3);

        // Poll again to drain the final two, plus stats.
        svc.invoke(
            Code::DrainChunk as u32,
            &(1024 as u32).to_be_bytes(),
            &mut arena,
            &mut out,
        )
        .unwrap();

        assert_eq!(out.len(), 3 * 3 + 1);

        // The last message in out should be stats
        let stats_out = out.pop().expect("missing stats");
        assert_eq!(Code::DrainedStats as u32, stats_out.code);

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
                    fields: vec![],
                    uuid_placeholder_ptr: String::new(),
                    projections: vec![],
                    collection_name: "test".to_string(),
                    infer_schema_json: "".to_string(),
                },
                &mut arena,
                &mut out,
            ),
            Err(Error::EmptyKey)
        ));
    }

    #[test]
    fn test_combine_with_defaults_simple() {
        let schema_json = serde_json::json!({
            "properties": {
                "aKey": { "type": "string" },
                "intProp": {
                    "type": "integer",
                    "default": 7,
                    "reduce": {"strategy": "sum"}
                },
                "strProp": {
                    "default": "defaultStringExtracted",
                    "type": "string",
                },
                "strPropNotExtracted": {
                    "default": "defaultStringNotExtracted",
                    "type": "string",
                },
            },
            "reduce": {"strategy": "merge"},
        })
        .to_string();

        let key_ptrs = vec!["/aKey".to_owned()];

        let field_ptrs = vec!["/intProp".to_owned(), "/strProp".to_owned()];

        let docs = vec![
            // If any of the combined documents have an actual value for a field, no default values
            // apply. Default values are not reduced into other documents. In this case, intProp for
            // the reduced document is 4 and the extracted field is also 4 due to the sum reduction
            // of the two actually present values, and the default for intProp has no effect.
            (true, json!({"aKey": "a", "intProp": 3})),
            (false, json!({"aKey": "a", "intProp": 1})),
            (false, json!({"aKey": "a", "strProp": "something"})),
            // The default value applies exclusively to extracted fields when none of the combined
            // documents have a value present for the field. Here the value for the extracted field
            // intProp is equal to the default value, but not summed from each document with the
            // value absent. The default value is not present in the reduced document.
            (true, json!({"aKey": "b"})),
            (false, json!({"aKey": "b"})),
            (false, json!({"aKey": "b", "strProp": "something"})),
            // A field with a default value that is not included in extracted fields will never be
            // present in the extracted fields, and as before never be included in document
            // reduction.
            (true, json!({"aKey": "c", "strPropNotExtracted": "first"})),
            (false, json!({"aKey": "c", "strPropNotExtracted": "second"})),
            (false, json!({"aKey": "c"})),
        ];

        insta::assert_debug_snapshot!(run_simple_svc(schema_json, key_ptrs, field_ptrs, &docs));
    }

    #[test]
    fn test_combine_with_defaults_different_types() {
        let schema_json = serde_json::json!({
            "properties": {
                "aKey": { "type": "string" },
                "intProp": {
                    "type": "integer",
                    "default": 7,
                },
                "numProp": {
                    "default": 12.4,
                    "type": "number",
                },
                "strProp": {
                    "default": "defaultString",
                    "type": "string",
                },
                "boolProp": {
                    "default": true,
                    "type": "boolean",
                },
                "objProp": {
                    "default": { "prop": "val" },
                    "type": "object",
                },
                "arrayProp": {
                    "default": [1, "hello", null],
                    "type": "array",
                },
            },
            "reduce": {"strategy": "merge"},
        })
        .to_string();

        let key_ptrs = vec!["/aKey".to_owned()];

        let field_ptrs = vec![
            "/intProp".to_owned(),
            "/numProp".to_owned(),
            "/strProp".to_owned(),
            "/boolProp".to_owned(),
            "/objProp".to_owned(),
            "/arrayProp".to_owned(),
        ];

        let docs = vec![(true, json!({"aKey": "a"}))];

        insta::assert_debug_snapshot!(run_simple_svc(schema_json, key_ptrs, field_ptrs, &docs));
    }

    #[test]
    fn test_combine_with_defaults_nested() {
        let schema_json = serde_json::json!({
            "properties": {
                "aKey": { "type": "string" },
                "objPropNoDefaultParent": {
                    "type": "object",
                    "properties": {
                        "nested": {
                            "type": "object",
                            "properties": {
                                "val": {
                                    "type": "string",
                                    "default": "nestedValNoDefaultParent",
                                },
                            },
                        },
                    },
                },
                "objPropWithDefaultParent": {
                    "type": "object",
                    "default": { "other": "thing" },
                    "properties": {
                        "nested": {
                            "type": "object",
                            "properties": {
                                "val": {
                                    "type": "string",
                                    "default": "nestedValWithDefaultParent",
                                },
                            },
                        },
                    },
                },
            },
        })
        .to_string();

        let key_ptrs = vec!["/aKey".to_owned()];

        let field_ptrs = vec![
            // Uses the default value of a located nested property, even though the parent object
            // doesn't have a default value.
            "/objPropNoDefaultParent/nested/val".to_owned(),
            // But the default value of nested properties is not used if the parent property
            // is omitted & does not have a default value.
            "/objPropNoDefaultParent".to_owned(),
            // A nested value can be extracted from a parent object even if the parent
            // object is not present and has a default value that does not include the
            // extracted field.
            "/objPropWithDefaultParent/nested/val".to_owned(),
            // The default value for an object that is not present overrides any default
            // values from the object's properties.
            "/objPropWithDefaultParent".to_owned(),
        ];

        let docs = vec![(true, json!({"aKey": "a", "intProp": 3}))];

        insta::assert_debug_snapshot!(run_simple_svc(schema_json, key_ptrs, field_ptrs, &docs));
    }

    /*
    // TODO(johnny): This is a good end-to-end test, but doesn't function with the test
    // harness because /arrayItems/2 needs to be an explicit projection and isn't inferred.
    #[test]
    fn test_combine_with_defaults_array() {
        let schema_json = serde_json::json!({
            "properties": {
                "arrayItems": {
                    "type": "array",
                    "items": [
                        {
                            "type": "string",
                            "default": "firstDefault",
                        },
                        {
                            "type": "string",
                            "default": "secondDefault",
                        },
                    ],
                    "minItems": 1,
                    "maxItems": 3,
                },
                "arrayContains": {
                    "type": "array",
                    "contains": {
                        "type": "string",
                        "default": "containsDefault",
                    },
                    "minContains": 1,
                    "maxContains": 3,
                },
            },
        })
        .to_string();

        let key_ptrs = vec!["/aKey".to_owned()];

        let field_ptrs = vec![
            // Defaults work positionally with items.
            "/arrayItems/0".to_owned(), // Matches default at idx 0
            "/arrayItems/1".to_owned(), // Matches default at idx 1
            "/arrayItems/2".to_owned(), // Cannot be located -> null
            // Defaults are not applied for contains.
            "/arrayContains/0".to_owned(),
        ];

        let docs = vec![(true, json!({"aKey": "a"}))];

        insta::assert_debug_snapshot!(run_simple_svc(schema_json, key_ptrs, field_ptrs, &docs));
    }
    */

    // Runs a combine svc to completion for a set of inputs and return the results sans stats with
    // some simple formatting applied.
    fn run_simple_svc(
        schema_json: String,
        key_ptrs: Vec<String>,
        field_ptrs: Vec<String>,
        docs: &[(bool, serde_json::Value)],
    ) -> Vec<Vec<String>> {
        let validator = crate::new_validator(&schema_json).unwrap();
        let shape = doc::Shape::infer(&validator.schemas()[0], validator.schema_index());

        let projections: Vec<_> = shape
            .locations()
            .into_iter()
            .map(|(ptr, is_pattern, shape, _exists)| {
                assert!(!is_pattern);

                // Test projection fields == their pointer.
                flow::Projection {
                    field: ptr.clone(),
                    ptr: ptr,
                    inference: Some(flow::Inference {
                        default_json: shape
                            .default
                            .as_ref()
                            .map(|val| val.0.to_string())
                            .unwrap_or_default(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            })
            .collect();

        let mut svc = API::create();
        let mut arena = Vec::new();
        let mut out = Vec::new();

        svc.invoke_message(
            Code::Configure as u32,
            combine_api::Config {
                schema_json,
                key_ptrs,
                fields: field_ptrs,
                uuid_placeholder_ptr: String::new(),
                projections,
                collection_name: "test".to_string(),
                infer_schema_json: "".to_string(),
            },
            &mut arena,
            &mut out,
        )
        .unwrap();

        for (left, doc) in docs {
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

        svc.invoke(
            Code::DrainChunk as u32,
            &(1024 as u32).to_be_bytes(),
            &mut arena,
            &mut out,
        )
        .unwrap();

        let stats_out = out.pop().expect("missing stats");
        assert_eq!(Code::DrainedStats as u32, stats_out.code);

        let code_seq = vec![
            Code::DrainedReducedDocument,
            Code::DrainedKey,
            Code::DrainedFields,
        ];

        out.iter()
            .chunks(3)
            .into_iter()
            .map(|i| {
                i.enumerate()
                    .map(|(idx, pos)| {
                        assert_eq!(code_seq[idx] as u32, pos.code);
                        String::from_utf8_lossy(&arena[pos.begin as usize..pos.end as usize])
                            .to_string()
                    })
                    .collect()
            })
            .collect()
    }
}

// This constant is shared between Rust and Go code.
// See go/protocols/flow/document_extensions.go.
pub const UUID_PLACEHOLDER: &str = "DocUUIDPlaceholder-329Bb50aa48EAa9ef";
