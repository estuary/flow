use serde_json::value::RawValue;
use std::collections::BTreeMap;

pub mod capture;
pub mod derive;
pub mod flow;
mod internal;
pub mod materialize;
pub mod ops;
pub mod runtime;

/// An enum representing any one of the types of built specs.
#[derive(Clone, Debug, serde::Serialize)]
pub enum AnyBuiltSpec {
    Capture(flow::CaptureSpec),
    Collection(flow::CollectionSpec),
    Materialization(flow::MaterializationSpec),
    Test(flow::TestSpec),
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Hash, Ord, Eq)]
pub struct RawJSONDeserialize(pub bytes::Bytes);

impl<'de> serde::Deserialize<'de> for RawJSONDeserialize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
        let value: Box<str> = value.into();
        let value: String = value.into();
        let value: bytes::Bytes = value.into();
        Ok(Self(value))
    }
}

// Adapt a &[u8] of JSON to a &RawValue for serialization.
fn as_raw_json<E: serde::ser::Error>(v: &[u8]) -> Result<&RawValue, E> {
    match serde_json::from_slice::<&RawValue>(v) {
        Ok(v) => Ok(v),
        Err(err) => Err(E::custom(format!(
            "field is required to be JSON but is not: {err:?}"
        ))),
    }
}

// Adapt a map of JSON string values to a BTreeMap of &RawValue for serialization.
fn as_raw_json_map<'a, E: serde::ser::Error>(
    v: &'a BTreeMap<String, bytes::Bytes>,
) -> Result<BTreeMap<&'a str, &'a RawValue>, E> {
    v.iter()
        .map(
            |(field, value)| match serde_json::from_slice::<&RawValue>(value) {
                Ok(v) => Ok((field.as_str(), v)),
                Err(err) => Err(E::custom(format!(
                    "field {field} is required to be JSON but is not: {err:?}"
                ))),
            },
        )
        .collect::<Result<_, _>>()
}

// Adapt a vector of JSON string values to a Vec of &RawValue for serialization.
fn as_raw_json_vec<'a, E: serde::ser::Error>(
    v: &'a Vec<bytes::Bytes>,
) -> Result<Vec<&'a RawValue>, E> {
    v.iter()
        .enumerate()
        .map(
            |(index, value)| match serde_json::from_slice::<&RawValue>(value) {
                Ok(v) => Ok(v),
                Err(err) => Err(E::custom(format!(
                    "index {index} is required to be JSON but is not: {err:?}"
                ))),
            },
        )
        .collect::<Result<_, _>>()
}

pub fn as_timestamp(ts: std::time::SystemTime) -> Timestamp {
    let dur = ts.duration_since(std::time::UNIX_EPOCH).unwrap();
    ::pbjson_types::Timestamp {
        seconds: dur.as_secs() as i64,
        nanos: (dur.as_nanos() % 1_000_000_000) as i32,
    }
}

impl ops::log::Level {
    /// Return this Level if it's not UndefinedLevel, or else return `or`.
    pub fn or(self, or: Self) -> Self {
        if self != ops::log::Level::UndefinedLevel {
            self
        } else {
            or
        }
    }
}

// Re-export some commonly used types.
pub use pbjson_types::Timestamp;
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

pub mod capability {
    pub const AUTHORIZE: u32 = 1 << 16;
    pub const SHUFFLE: u32 = 1 << 17;
    pub const NETWORK_PROXY: u32 = 1 << 18;
    pub const PROXY_CONNECTOR: u32 = 1 << 19;
}
