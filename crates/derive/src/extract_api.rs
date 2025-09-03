use crate::new_validator;
use doc::AsNode;
use prost::Message;
use proto_flow::flow::{
    self,
    extract_api::{self, Code},
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to parse URL")]
    Url(#[from] url::ParseError),
    #[error("schema index")]
    SchemaIndex(#[from] json::schema::index::Error),
    #[error("failed to parse JSON document: {0:?}")]
    Json(std::io::Error),
    #[error("invalid document UUID: {value:?}")]
    InvalidUuid { value: Option<serde_json::Value> },
    #[error("Protobuf decoding error")]
    ProtoDecode(#[from] prost::DecodeError),
    #[error("source document failed validation against its collection JSON")]
    FailedValidation(#[source] doc::FailedValidation),
    #[error(transparent)]
    Extractor(#[from] extractors::Error),
    #[error("protocol error (invalid state or invocation)")]
    InvalidState,
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

/// Extract a UUID at the given location within the document, returning its UuidParts,
/// or None if the Pointer does not resolve to a valid v1 UUID.
pub fn extract_uuid_parts<N: AsNode>(v: &N, ptr: &doc::Pointer) -> Option<flow::UuidParts> {
    let Some(v_uuid) = ptr.query(v) else {
        return None;
    };
    let doc::Node::String(v_uuid) = v_uuid.as_node() else {
        return None;
    };

    uuid::Uuid::parse_str(v_uuid).ok().and_then(|u| {
        if u.get_version_num() != 1 {
            return None;
        }
        let (c_low, c_mid, c_high, seq_node_id) = u.as_fields();

        Some(flow::UuidParts {
            clock: (c_low as u64) << 4          // Clock low bits.
            | (c_mid as u64) << 36                  // Clock middle bits.
            | (c_high as u64) << 52                 // Clock high bits.
            | ((seq_node_id[0] as u64) >> 2) & 0xf, // High 4 bits of sequence number.

            node: (seq_node_id[2] as u64) << 56 // 6 bytes of big-endian node ID.
            | (seq_node_id[3] as u64) << 48
            | (seq_node_id[4] as u64) << 40
            | (seq_node_id[5] as u64) << 32
            | (seq_node_id[6] as u64) << 24
            | (seq_node_id[7] as u64) << 16
            | ((seq_node_id[0] as u64) & 0x3) << 8 // High 2 bits of flags.
            | (seq_node_id[1] as u64), // Low 8 bits of flags.
        })
    })
}

/// API provides extraction as a cgo::Service.
pub struct API {
    state: Option<State>,
}

struct State {
    alloc: doc::Allocator,
    parser: simd_doc::Parser,
    uuid_ptr: doc::Pointer,
    extractors: Vec<doc::Extractor>,
    validator: Option<doc::Validator>,
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
        let code = match Code::try_from(code as i32) {
            Ok(c) => c,
            Err(_) => return Err(Error::InvalidState),
        };
        tracing::trace!(?code, "invoke");

        match (code, std::mem::take(&mut self.state)) {
            (Code::Configure, _) => {
                let extract_api::Config {
                    uuid_ptr,
                    schema_json,
                    field_ptrs,
                    projections,
                } = extract_api::Config::decode(data)?;

                let validator = if schema_json.is_empty() {
                    None
                } else {
                    Some(new_validator(&schema_json)?)
                };

                self.state = Some(State {
                    alloc: doc::Allocator::new(),
                    parser: simd_doc::Parser::new(),
                    uuid_ptr: doc::Pointer::from(&uuid_ptr),
                    extractors: extractors::for_key(
                        &field_ptrs,
                        &projections,
                        &doc::SerPolicy::noop(),
                    )?,
                    validator,
                });
                Ok(())
            }
            // Extract from JSON document.
            (Code::Extract, Some(mut state)) => {
                let doc = state
                    .parser
                    .parse_one(data, &state.alloc)
                    .map_err(|err| Error::Json(err))?;

                let uuid = extract_uuid_parts(&doc, &state.uuid_ptr).ok_or_else(|| {
                    Error::InvalidUuid {
                        value: state.uuid_ptr.query(&doc).map(AsNode::to_debug_json_value),
                    }
                })?;

                if proto_gazette::message_flags::ACK_TXN & uuid.node != 0 {
                    // Transaction acknowledgements aren't expected to validate.
                } else if let Some(validator) = &mut state.validator {
                    validator
                        .validate(None, &doc)?
                        .ok()
                        .map_err(|invalid| Error::FailedValidation(invalid.revalidate_with_context(&doc)))?;
                }

                // Send extracted UUID.
                cgo::send_message(Code::ExtractedUuid as u32, &uuid, arena, out);

                // Send extracted, packed field pointers.
                let begin = arena.len();

                for ex in &state.extractors {
                    ex.extract(&doc, arena).unwrap(); // Vec<u8> is infallible for io::Write.
                }
                cgo::send_bytes(Code::ExtractedFields as u32, begin, arena, out);

                state.alloc.reset();
                self.state = Some(state);
                Ok(())
            }
            _ => Err(Error::InvalidState),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{extract_uuid_parts, Code, API};
    use cgo::Service;
    use proto_flow::flow;
    use serde_json::{json, Value};

    #[test]
    fn test_extraction_uuid_to_parts() {
        let v = json!({
            "_meta": {
                "uuid": "9f2952f3-c6a3-11ea-8802-080607050309",
            },
            "foo": "bar",
            "tru": true,
        });

        // "/_meta/uuid" maps to an encoded UUID. This fixture and the values
        // below are also used in Go-side tests.
        assert_eq!(
            extract_uuid_parts(&v, &doc::Pointer::from("/_meta/uuid")).unwrap(),
            flow::UuidParts {
                node: 0x0806070503090000 + 0x02,
                clock: 0x1eac6a39f2952f32,
            },
        );
        // "/missing" maps to Null, which is the wrong type.
        match extract_uuid_parts(&v, &doc::Pointer::from("/missing")) {
            None => {}
            p @ _ => panic!("{:?}", p),
        }
        // "/foo" maps to "bar", also not a UUID.
        match extract_uuid_parts(&v, &doc::Pointer::from("/foo")) {
            None => {}
            p @ _ => panic!("{:?}", p),
        }
        // "/tru" maps to true, of the wrong type.
        match extract_uuid_parts(&v, &doc::Pointer::from("/tru")) {
            None => {}
            p @ _ => panic!("{:?}", p),
        }
    }

    #[test]
    fn test_extraction_hashes_and_fields() {
        let v1 = serde_json::json!({
            "a": "value",
            "obj": {"tru": true, "other": "value"},
            "fals": false,
            "arr": ["foo"],
            "doub": 1.3,
            "unsi": 2,
            "sign": -30,
        });

        let cases = vec![
            ("/missing", json!(null)),
            ("/obj/tru", json!(true)),
            ("/fals", json!(false)),
            ("/arr/0", json!("foo")),
            ("/unsi", json!(2)),
            ("/doub", json!(1.3)),
            ("/sign", json!(-30)),
            ("/obj", json!({"other":"value","tru":true})),
            ("/arr", json!(["foo"])),
        ];
        for (ptr, expect_value) in cases {
            let ptr = doc::Pointer::from(ptr);
            let field = ptr.query(&v1).unwrap_or(&Value::Null);
            assert_eq!(field, &expect_value);
        }
    }

    #[test]
    fn test_extractor_service() {
        let mut svc = API::create();

        // Initialize arena & out with content which must not be touched.
        let mut arena = b"prefix".to_vec();
        let mut out = vec![cgo::Out {
            code: 999,
            begin: 0,
            end: 0,
        }];

        // Configure the service.
        svc.invoke_message(
            Code::Configure as u32,
            flow::extract_api::Config {
                uuid_ptr: "/0".to_string(),
                field_ptrs: vec!["/1".to_string(), "/2".to_string()],
                projections: ["/1", "/2"]
                    .iter()
                    .map(|ptr| flow::Projection {
                        ptr: ptr.to_string(),
                        inference: Some(flow::Inference::default()),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            },
            &mut arena,
            &mut out,
        )
        .unwrap();

        // Extract from a document.
        svc.invoke(
            Code::Extract as u32,
            br#"["9f2952f3-c6a3-11ea-8802-080607050309", 42, "a-string"]"#,
            &mut arena,
            &mut out,
        )
        .unwrap();

        insta::assert_debug_snapshot!((String::from_utf8_lossy(&arena), out));
    }
}
