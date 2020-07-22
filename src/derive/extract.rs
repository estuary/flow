use super::Error;
use crate::doc::Pointer;
use estuary_protocol::flow;

#[derive(Debug)]
pub struct ExtractService {}

#[tonic::async_trait]
impl flow::extract_server::Extract for ExtractService {
    async fn extract(
        &self,
        request: tonic::Request<flow::ExtractRequest>,
    ) -> Result<tonic::Response<flow::ExtractResponse>, tonic::Status> {
        extract(request.get_ref())
    }
}

fn extract(
    request: &flow::ExtractRequest,
) -> Result<tonic::Response<flow::ExtractResponse>, tonic::Status> {
    // Allocate an ExtractResponse of the right shape.
    let mut response = flow::ExtractResponse {
        uuid_parts: Vec::with_capacity(request.documents.len()),
        hashes: request
            .hashes
            .iter()
            .map(|_| flow::Hash {
                values: Vec::with_capacity(request.documents.len()),
            })
            .collect(),
        fields: request
            .fields
            .iter()
            .map(|field| flow::Field {
                name: field.name.clone(),
                values: Vec::with_capacity(request.documents.len()),
            })
            .collect(),
    };

    // Project UUID pointer, hashes and fields into parsed JSON pointers.
    let uuid_ptr = Pointer::from(&request.uuid_ptr);
    let hash_ptrs: Vec<Vec<Pointer>> = request
        .hashes
        .iter()
        .map(|h| h.ptrs.iter().map(|p| p.into()).collect())
        .collect();
    let field_ptrs: Vec<Pointer> = request
        .fields
        .iter()
        .map(|field| Pointer::from(&field.ptr))
        .collect();

    for (index, doc) in request.documents.iter().enumerate() {
        decode_to_value(&doc.content, doc.content_type)
            .and_then(|v| {
                // Extract UUIDParts, fields, and hashes.
                response.uuid_parts.push(extract_uuid_parts(&v, &uuid_ptr)?);

                for (field, ptr) in response.fields.iter_mut().zip(field_ptrs.iter()) {
                    field.values.push(extract_field(&v, ptr));
                }
                for (hash, ptrs) in response.hashes.iter_mut().zip(hash_ptrs.iter()) {
                    hash.values.push(extract_hash(&v, &ptrs) as u32);
                }
                Ok(())
            })
            .map_err(|err| {
                tonic::Status::invalid_argument(format!(
                    "extraction of document {}: {}",
                    index, err
                ))
            })?;
    }
    Ok(tonic::Response::new(response))
}

fn decode_to_value(content: &[u8], content_type_code: i32) -> Result<serde_json::Value, Error> {
    match content_type_code {
        ct if ct == (flow::document::ContentType::Json as i32) => {
            Ok(serde_json::from_slice(content)?)
        }
        _ => Err(Error::InvalidContentType {
            code: content_type_code,
            content_type: flow::document::ContentType::from_i32(content_type_code),
        }),
    }
}

fn extract_uuid_parts(v: &serde_json::Value, ptr: &Pointer) -> Result<flow::UuidParts, Error> {
    let v_uuid = ptr.query(&v).unwrap_or(&serde_json::Value::Null);
    v_uuid
        .as_str()
        .and_then(|s| uuid::Uuid::parse_str(s).ok())
        .and_then(|u| {
            if u.get_version_num() != 1 {
                return None;
            }
            let (c_low, c_mid, c_high, seq_node_id) = u.as_fields();

            Some(flow::UuidParts {
                clock: (c_low as u64) << 4          // Clock low bits.
            | (c_mid as u64) << 36                  // Clock middle bits.
            | (c_high as u64) << 52                 // Clock high bits.
            | ((seq_node_id[0] as u64) >> 2) & 0xf, // High 4 bits of sequence number.

                producer_and_flags: (seq_node_id[2] as u64) << 56 // 6 bytes of big-endian node ID.
            | (seq_node_id[3] as u64) << 48
            | (seq_node_id[4] as u64) << 40
            | (seq_node_id[5] as u64) << 32
            | (seq_node_id[6] as u64) << 24
            | (seq_node_id[7] as u64) << 16
            | ((seq_node_id[0] as u64) & 0x3) << 8 // High 2 bits of flags.
            | (seq_node_id[1] as u64), // Low 8 bits of flags.
            })
        })
        .ok_or_else(|| Error::InvalidUuid {
            value: v_uuid.clone(),
        })
}

fn extract_field(v: &serde_json::Value, ptr: &Pointer) -> flow::field::Value {
    let vv = ptr.query(v).unwrap_or(&serde_json::Value::Null);

    let mut out = flow::field::Value {
        kind: 0,
        unsigned: 0,
        signed: 0,
        double: 0.0,
        bytes: Vec::new(),
    };

    match vv {
        serde_json::Value::Null => out.kind = flow::field::value::Kind::Null as i32,
        serde_json::Value::Bool(true) => out.kind = flow::field::value::Kind::True as i32,
        serde_json::Value::Bool(false) => out.kind = flow::field::value::Kind::False as i32,
        serde_json::Value::Number(n) => match estuary_json::Number::from(n) {
            estuary_json::Number::Float(d) => {
                out.kind = flow::field::value::Kind::Double as i32;
                out.double = d;
            }
            estuary_json::Number::Signed(s) => {
                out.kind = flow::field::value::Kind::Signed as i32;
                out.signed = s;
            }
            estuary_json::Number::Unsigned(u) => {
                out.kind = flow::field::value::Kind::Unsigned as i32;
                out.unsigned = u;
            }
        },
        serde_json::Value::String(s) => {
            out.kind = flow::field::value::Kind::String as i32;
            out.bytes.extend(s.as_bytes().iter()); // Send raw UTF-8 string.
        }
        serde_json::Value::Array(_) => {
            out.kind = flow::field::value::Kind::Array as i32;
            out.bytes = serde_json::to_vec(vv).unwrap();
        }
        serde_json::Value::Object(_) => {
            out.kind = flow::field::value::Kind::Object as i32;
            out.bytes = serde_json::to_vec(vv).unwrap();
        }
    }
    out
}

fn extract_hash(doc: &serde_json::Value, ptrs: &[Pointer]) -> u64 {
    use std::num::Wrapping;
    let mut hash = Wrapping(0u64);

    for ptr in ptrs.iter() {
        let value = ptr.query(doc).unwrap_or(&serde_json::Value::Null);
        let span = estuary_json::de::walk(value, &mut estuary_json::NoopWalker).unwrap();

        // Drawn from boost::hash_combine(). The constant is the 64-bit inverse of the golden ratio.
        // See https://stackoverflow.com/questions/5889238/why-is-xor-the-default-way-to-combine-hashes
        hash ^= Wrapping(span.hashed) + Wrapping(0x9e3779b97f4a7c15) + (hash << 6) + (hash >> 2);
    }
    hash.0
}

#[cfg(test)]
mod test {
    use super::{
        decode_to_value, extract_field, extract_hash, extract_uuid_parts, flow, Error, Pointer,
    };

    #[test]
    fn test_decode_to_value() {
        assert_eq!(
            decode_to_value(
                r#"{"key":42}"#.as_bytes(),
                flow::document::ContentType::Json as i32
            )
            .unwrap(),
            serde_json::json!({"key": 42}),
        );
        // Reports malformed JSON.
        match decode_to_value(
            r#"{"key":42"#.as_bytes(),
            flow::document::ContentType::Json as i32,
        ) {
            Err(Error::JSONErr(_)) => {}
            p @ _ => panic!(p),
        };
        // Reports unexpected / unknown Content-Type.
        match decode_to_value(r#"foobar"#.as_bytes(), 1234567) {
            Err(Error::InvalidContentType {
                code: 1234567,
                content_type: None,
            }) => {}
            p @ _ => panic!(p),
        };
    }

    #[test]
    fn test_extraction_uuid_to_parts() {
        let v = serde_json::json!({
            "_meta": {
                "uuid": "9f2952f3-c6a3-11ea-8802-080607050309",
            },
            "foo": "bar",
            "tru": true,
        });

        // "/_meta/uuid" maps to an encoded UUID. This fixture and the values
        // below are also used in Go-side tests.
        assert_eq!(
            extract_uuid_parts(&v, &Pointer::from("/_meta/uuid")).unwrap(),
            flow::UuidParts {
                producer_and_flags: 0x0806070503090000 + 0x02,
                clock: 0x1eac6a39f2952f32,
            },
        );
        // "/missing" maps to Null, which is the wrong type.
        match extract_uuid_parts(&v, &Pointer::from("/missing")) {
            Err(Error::InvalidUuid {
                value: serde_json::Value::Null,
            }) => {}
            p @ _ => panic!(p),
        }
        // "/foo" maps to "bar", also not a UUID.
        match extract_uuid_parts(&v, &Pointer::from("/foo")) {
            Err(Error::InvalidUuid {
                value: serde_json::Value::String(s),
            }) if s == "bar" => {}
            p @ _ => panic!(p),
        }
        // "/tru" maps to true, of the wrong type.
        match extract_uuid_parts(&v, &Pointer::from("/tru")) {
            Err(Error::InvalidUuid {
                value: serde_json::Value::Bool(b),
            }) if b => {}
            p @ _ => panic!(p),
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

        // Hash pointer ordering matters.
        assert_eq!(
            11540352395275474257,
            extract_hash(&v1, &[Pointer::from("/a"), Pointer::from("/obj")])
        );
        assert_eq!(
            10557852061454008160,
            extract_hash(&v1, &[Pointer::from("/obj"), Pointer::from("/a")])
        );
        // Different locations with the same value have the same hash.
        assert_eq!(
            16180379926493368624,
            extract_hash(&v1, &[Pointer::from("/a")])
        );
        assert_eq!(
            16180379926493368624,
            extract_hash(&v1, &[Pointer::from("/obj/other")])
        );

        let zero_value = flow::field::Value {
            kind: 0,
            unsigned: 0,
            signed: 0,
            double: 0.0,
            bytes: Vec::new(),
        };

        let cases = vec![
            ("/missing", {
                let mut o = zero_value.clone();
                o.kind = flow::field::value::Kind::Null as i32;
                o
            }),
            ("/obj/tru", {
                let mut o = zero_value.clone();
                o.kind = flow::field::value::Kind::True as i32;
                o
            }),
            ("/fals", {
                let mut o = zero_value.clone();
                o.kind = flow::field::value::Kind::False as i32;
                o
            }),
            ("/arr/0", {
                let mut o = zero_value.clone();
                o.kind = flow::field::value::Kind::String as i32;
                o.bytes = "foo".as_bytes().iter().copied().collect();
                o
            }),
            ("/unsi", {
                let mut o = zero_value.clone();
                o.kind = flow::field::value::Kind::Unsigned as i32;
                o.unsigned = 2;
                o
            }),
            ("/doub", {
                let mut o = zero_value.clone();
                o.kind = flow::field::value::Kind::Double as i32;
                o.double = 1.3;
                o
            }),
            ("/sign", {
                let mut o = zero_value.clone();
                o.kind = flow::field::value::Kind::Signed as i32;
                o.signed = -30;
                o
            }),
            ("/obj", {
                let mut o = zero_value.clone();
                o.kind = flow::field::value::Kind::Object as i32;
                o.bytes = r#"{"other":"value","tru":true}"#.as_bytes().iter().copied().collect();
                o
            }),
            ("/arr", {
                let mut o = zero_value.clone();
                o.kind = flow::field::value::Kind::Array as i32;
                o.bytes = r#"["foo"]"#.as_bytes().iter().copied().collect();
                o
            }),
        ];
        for (ptr, expect) in cases {
            assert_eq!(expect, extract_field(&v1, &Pointer::from(ptr)));
        }
    }
}
