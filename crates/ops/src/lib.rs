use serde::{de::Error, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Write;

pub mod decode;
pub mod tracing;

// Re-export LogLevel for usage as ops::LogLevel elsewhere.
pub use proto_flow::flow::LogLevel;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Log {
    /// Timestamp at which the Log was created.
    #[serde(
        serialize_with = "time::serde::rfc3339::serialize",
        deserialize_with = "time::serde::rfc3339::deserialize"
    )]
    ts: time::OffsetDateTime,
    /// Level of the log.
    level: LogLevel,
    /// Message of the log.
    message: String,
    /// Supplemental fields of the log.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    fields: BTreeMap<String, Box<serde_json::value::RawValue>>,
    /// Metadata of the shard which created the Log.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    shard: Option<Shard>,
    /// Spans of the shard.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    spans: Vec<Log>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum ShardKind {
    Capture,
    Derivation,
    Materialization,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Shard {
    /// The type of the shard's catalog task.
    kind: ShardKind,
    /// The name of the shard's catalog task.
    name: String,
    /// The inclusive beginning of the shard's assigned key range.
    key_begin: HexU32,
    /// The inclusive beginning of the shard's assigned rClock range.
    r_clock_begin: HexU32,
}

#[derive(Debug, Clone)]
pub struct HexU32(pub u32);

impl Serialize for HexU32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        format!("{:08x}", self.0).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for HexU32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;

        match u32::from_str_radix(&s, 16) {
            Ok(s) => Ok(Self(s)),
            Err(err) => Err(D::Error::custom(err)),
        }
    }
}

/// stderr_log_handler is a log handler that writes canonical
/// JSON log serializations to stderr.
pub fn stderr_log_handler(log: Log) {
    let mut buf = serde_json::to_vec(&log).expect("Log always serializes");
    buf.push(b'\n');
    _ = std::io::stderr().write_all(&buf); // Best-effort.
}

/// new_encoded_json_write_handler returns a log handler that
/// writes canonical JSON log serializations to the given writer.
pub fn new_encoded_json_write_handler<W>(
    writer: std::sync::Arc<std::sync::Mutex<W>>,
) -> impl Fn(Log) + Send + Sync + 'static
where
    W: std::io::Write + Send + 'static,
{
    move |log: Log| {
        let mut buf = serde_json::to_vec(&log).expect("Log always serializes");
        buf.push(b'\n');
        _ = writer
            .lock()
            .expect("writer is never poisoned")
            .write_all(&buf); // Best-effort.
    }
}

#[cfg(test)]
mod test {
    use super::{Log, LogLevel};
    use crate::new_encoded_json_write_handler;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_encoded_json_write_handler() {
        let writer = Arc::new(Mutex::new(Vec::new()));
        let handler = new_encoded_json_write_handler(writer.clone());

        let mut log = Log {
            ts: time::OffsetDateTime::UNIX_EPOCH,
            level: LogLevel::Warn,
            message: "hello world".to_string(),
            fields: [(
                "name".to_string(),
                serde_json::value::to_raw_value("value").unwrap(),
            )]
            .into_iter()
            .collect(),
            shard: None,
            spans: Vec::new(),
        };

        handler(log.clone());
        log.message = "I'm different!".to_string();
        handler(log);

        std::mem::drop(handler);
        let writer = Arc::try_unwrap(writer).unwrap().into_inner().unwrap();

        insta::assert_snapshot!(String::from_utf8_lossy(&writer), @r###"
        {"ts":"1970-01-01T00:00:00Z","level":"warn","message":"hello world","fields":{"name":"value"}}
        {"ts":"1970-01-01T00:00:00Z","level":"warn","message":"I'm different!","fields":{"name":"value"}}
        "###);
    }
}
