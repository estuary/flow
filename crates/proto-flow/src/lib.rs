use serde_json::value::RawValue;
use std::collections::BTreeMap;

pub mod capture;
pub mod derive;
pub mod flow;
pub mod materialize;
pub mod ops;
pub mod runtime;

// Adapt a &str of JSON to a &RawValue for serialization.
fn as_raw_json<E: serde::ser::Error>(v: &str) -> Result<&RawValue, E> {
    match serde_json::from_str::<&RawValue>(v) {
        Ok(v) => Ok(v),
        Err(err) => Err(E::custom(format!(
            "field is required to be JSON but is not: {err:?}"
        ))),
    }
}

// Adapt a map of JSON string values to a BTreeMap of &RawValue for serialization.
fn as_raw_json_map<'a, E: serde::ser::Error>(
    v: &'a BTreeMap<String, String>,
) -> Result<BTreeMap<&'a str, &'a RawValue>, E> {
    v.iter()
        .map(
            |(field, value)| match serde_json::from_str::<&RawValue>(value) {
                Ok(v) => Ok((field.as_str(), v)),
                Err(err) => Err(E::custom(format!(
                    "field {field} is required to be JSON but is not: {err:?}"
                ))),
            },
        )
        .collect::<Result<_, _>>()
}

// Adapt a vector of JSON string values to a Vec of &RawValue for serialization.
fn as_raw_json_vec<'a, E: serde::ser::Error>(v: &'a Vec<String>) -> Result<Vec<&'a RawValue>, E> {
    v.iter()
        .enumerate()
        .map(
            |(index, value)| match serde_json::from_str::<&RawValue>(value) {
                Ok(v) => Ok(v),
                Err(err) => Err(E::custom(format!(
                    "index {index} is required to be JSON but is not: {err:?}"
                ))),
            },
        )
        .collect::<Result<_, _>>()
}

pub fn as_timestamp(ts: std::time::SystemTime) -> ::pbjson_types::Timestamp {
    let dur = ts.duration_since(std::time::UNIX_EPOCH).unwrap();
    ::pbjson_types::Timestamp {
        seconds: dur.as_secs() as i64,
        nanos: (dur.as_nanos() % 1_000_000_000) as i32,
    }
}

// Re-export some commonly used types.
pub use pbjson_types::Any;
pub use proto_gazette::consumer::checkpoint as runtime_checkpoint;
pub use proto_gazette::consumer::Checkpoint as RuntimeCheckpoint;

mod serde_capture {
    use crate::capture::*;
    include!("capture.serde.rs");
}
mod serde_derive {
    use crate::derive::*;
    include!("derive.serde.rs");
}
mod serde_flow {
    use crate::flow::*;
    include!("flow.serde.rs");
}
mod serde_materialize {
    use crate::materialize::*;
    include!("materialize.serde.rs");
}
mod serde_ops {
    use crate::ops::*;
    include!("ops.serde.rs");
}
// We don't generate serde support for the `runtime` protobuf package,
// as it's not intended for JSON serialization.
