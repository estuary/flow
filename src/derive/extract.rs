use crate::doc::Pointer;
use estuary_protocol::consumer;
use estuary_protocol::flow::{self, derive_server::Derive};
use estuary_protocol::recoverylog;
use std::hash::Hasher;

//use futures_core::Stream;
use futures::stream::Stream;
use std::pin::Pin;

#[tonic::async_trait]
impl Derive for DeriveService {
    async fn restore_checkpoint(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<consumer::Checkpoint>, tonic::Status> {
        unimplemented!();
    }

    type DeriveStream = Pin<
        Box<dyn Stream<Item = Result<flow::DeriveResponse, tonic::Status>> + Send + Sync + 'static>,
    >;

    async fn derive(
        &self,
        _request: tonic::Request<tonic::Streaming<flow::DeriveRequest>>,
    ) -> Result<tonic::Response<Self::DeriveStream>, tonic::Status> {
        unimplemented!();
    }

    async fn build_hints(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<recoverylog::FsmHints>, tonic::Status> {
        unimplemented!();
    }

    async fn extract(
        &self,
        request: tonic::Request<flow::ExtractRequest>,
    ) -> Result<tonic::Response<flow::ExtractResponse>, tonic::Status> {
        unimplemented!();
    }
}

#[derive(Debug)]
pub struct DeriveService {}

fn extract(request: flow::ExtractRequest) -> Result<flow::ExtractResponse, super::Error> {
    // Project UUID pointer, hashes and fields into a representation with parsed JSON pointers.
    let uuid_ptr = Pointer::from(&request.uuid_ptr);
    let hashes: Vec<Vec<Pointer>> = request
        .hashes
        .iter()
        .map(|h| h.ptrs.iter().map(|p| p.into()).collect())
        .collect();

    let fields: Vec<(&str, Pointer)> = request
        .fields
        .iter()
        .map(|f| (f.name.as_ref(), Pointer::from(&f.ptr)))
        .collect();

    let mut response = flow::ExtractResponse {
        uuid_parts: Vec::with_capacity(request.documents.len()),
        hashes: hashes
            .iter()
            .map(|_| flow::Hash {
                values: Vec::with_capacity(request.documents.len()),
            })
            .collect(),
        fields: fields
            .iter()
            .map(|f| flow::Field {
                name: f.0.to_owned(),
                values: Vec::with_capacity(request.documents.len()),
            })
            .collect(),
    };

    for (index, doc) in request.documents.iter().enumerate() {
        let v: serde_json::Value = serde_json::from_slice(&doc.content)?;

        // Extract UUID parts.
        let v_uuid = uuid_ptr.query(&v).unwrap_or(&serde_json::Value::Null);
        let v_uuid = v_uuid
            .as_str()
            .and_then(|s| uuid::Uuid::parse_str(s).ok())
            .and_then(|u| uuid_to_parts(u))
            .ok_or_else(|| super::Error::InvalidValue {
                index,
                value: v_uuid.clone(),
            })?;
        response.uuid_parts.push(v_uuid);

        // Extract hashes.
        for (h, ptrs) in hashes.iter().enumerate() {
            response.hashes[h]
                .values
                .push(extract_hash(&v, &ptrs) as u32);
        }
        // Extract fields.
        for (f, (_, ptr)) in fields.iter().enumerate() {
            response.fields[f].values.push(extract_field(&v, ptr));
        }
    }
    Ok(response)
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

fn uuid_to_parts(u: uuid::Uuid) -> Option<flow::UuidParts> {
    if u.get_version_num() != 1 {
        return None;
    }
    let (c_low, c_mid, c_high, seq_node_id) = u.as_fields();

    Some(flow::UuidParts {
        clock: (c_low as u64) << 4                  // Clock low bits.
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
}

#[cfg(test)]
mod test {
    use super::{extract_field, extract_hash, flow, uuid_to_parts, Pointer};

    #[test]
    fn test_uuid_to_parts() {
        let uuid = uuid::Uuid::parse_str("9f2952f3-c6a3-11ea-8802-080607050309").unwrap();

        assert_eq!(
            uuid_to_parts(uuid),
            Some(flow::UuidParts {
                producer_and_flags: 0x0806070503090000 + 0x02,
                clock: 0x1eac6a39f2952f32,
            })
        );
    }

    #[test]
    fn test_extraction() {
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
