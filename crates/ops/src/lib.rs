use serde::{de::Error, Deserialize, Serialize};
use std::io::Write;

pub mod decode;
pub mod tracing;

pub use proto_flow::ops::log::Level as LogLevel;
pub use proto_flow::ops::Log;
pub use proto_flow::ops::TaskType;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Shard {
    /// The type of the shard's catalog task.
    kind: TaskType,
    /// The name of the shard's catalog task.
    name: String,
    /// The inclusive beginning of the shard's assigned key range.
    key_begin: HexU32,
    /// The inclusive beginning of the shard's assigned rClock range.
    r_clock_begin: HexU32,
}

impl From<Shard> for proto_flow::ops::ShardRef {
    fn from(
        Shard {
            kind,
            name,
            key_begin,
            r_clock_begin,
        }: Shard,
    ) -> Self {
        Self {
            kind: kind as i32,
            name,
            key_begin: format!("{:08x}", key_begin.0),
            r_clock_begin: format!("{:08x}", r_clock_begin.0),
        }
    }
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
) -> impl Fn(Log) + Send + Sync + Clone + 'static
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

// new_tracing_dispatch_handler returns a log handler that
// writes tracing events using the given dispatcher.
pub fn new_tracing_dispatch_handler(
    dispatcher: ::tracing::Dispatch,
) -> impl Fn(Log) + Send + Sync + Clone + 'static {
    move |log| ::tracing::dispatcher::with_default(&dispatcher, || tracing_log_handler(log))
}

/// tracing_log_handler is a log handler that writes logs
/// as tracing events.
pub fn tracing_log_handler(
    Log {
        level,
        fields_json_map: fields,
        message,
        ..
    }: Log,
) {
    match LogLevel::from_i32(level).unwrap_or_default() {
        LogLevel::Trace => ::tracing::trace!(?fields, message),
        LogLevel::Debug => ::tracing::debug!(?fields, message),
        LogLevel::Info => ::tracing::info!(?fields, message),
        LogLevel::Warn => ::tracing::warn!(?fields, message),
        LogLevel::Error => ::tracing::error!(?fields, message),
        LogLevel::UndefinedLevel => (),
    }
}

#[cfg(test)]
mod test {
    use super::{Log, LogLevel};
    use crate::new_encoded_json_write_handler;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_encoded_json_write_handler() {
        let writer = Arc::new(Mutex::new(Vec::new()));
        let handler = new_encoded_json_write_handler(writer.clone());

        let mut log = Log {
            meta: None,
            timestamp: Some(proto_flow::as_timestamp(std::time::UNIX_EPOCH)),
            level: LogLevel::Warn as i32,
            message: "hello world".to_string(),
            fields_json_map: [("name".to_string(), json!("value").to_string())].into(),
            shard: None,
            spans: Vec::new(),
        };

        handler(log.clone());
        log.message = "I'm different!".to_string();
        handler(log);

        std::mem::drop(handler);
        let writer = Arc::try_unwrap(writer).unwrap().into_inner().unwrap();

        insta::assert_snapshot!(String::from_utf8_lossy(&writer), @r###"
        {"ts":"1970-01-01T00:00:00+00:00","level":"warn","message":"hello world","fields":{"name":"value"}}
        {"ts":"1970-01-01T00:00:00+00:00","level":"warn","message":"I'm different!","fields":{"name":"value"}}
        "###);
    }
}
