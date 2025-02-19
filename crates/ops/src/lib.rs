use serde::{de::Error, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Write;

pub mod decode;
pub mod tracing;

// Re-export many types from proto_flow::ops, so that users of this crate
// don't also have to use that module.
pub use proto_flow::ops::{
    log::Level as LogLevel, stats, Log, Meta, ShardLabeling, ShardRef, Stats, TaskType,
};

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
    /// The id of the build that the shard is currently running
    #[serde(default)]
    build: String,
}

impl From<Shard> for ShardRef {
    fn from(
        Shard {
            kind,
            name,
            key_begin,
            r_clock_begin,
            build,
        }: Shard,
    ) -> Self {
        Self {
            kind: kind as i32,
            name,
            key_begin: format!("{:08x}", key_begin.0),
            r_clock_begin: format!("{:08x}", r_clock_begin.0),
            build,
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
pub fn stderr_log_handler(log: &Log) {
    let mut buf = serde_json::to_vec(&log).expect("Log always serializes");
    buf.push(b'\n');
    _ = std::io::stderr().write_all(&buf); // Best-effort.
}

/// new_encoded_json_write_handler returns a log handler that
/// writes canonical JSON log serializations to the given writer.
pub fn new_encoded_json_write_handler<W>(
    writer: std::sync::Arc<std::sync::Mutex<W>>,
) -> impl Fn(&Log) + Send + Sync + Clone + 'static
where
    W: std::io::Write + Send + 'static,
{
    move |log: &Log| {
        let mut buf = serde_json::to_vec(log).expect("Log always serializes");
        buf.push(b'\n');
        _ = writer
            .lock()
            .expect("writer is never poisoned")
            .write_all(&buf); // Best-effort.
    }
}

/// tracing_log_handler is a log handler that writes logs
/// as tracing events.
pub fn tracing_log_handler(
    Log {
        level,
        fields_json_map,
        message,
        ..
    }: &Log,
) {
    let fields = DebugJson(
        fields_json_map
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    serde_json::value::RawValue::from_string(v.clone()).unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>(),
    );

    match LogLevel::try_from(*level).unwrap_or_default() {
        LogLevel::Trace => ::tracing::trace!(message, ?fields),
        LogLevel::Debug => ::tracing::debug!(message, ?fields),
        LogLevel::Info => ::tracing::info!(message, ?fields),
        LogLevel::Warn => ::tracing::warn!(message, ?fields),
        LogLevel::Error => ::tracing::error!(message, ?fields),
        LogLevel::UndefinedLevel => (),
    }
}

/// DebugJson is a new-type wrapper around any Serialize implementation
/// that wishes to support the Debug trait via JSON encoding itself.
/// If stderr is a terminal, it colorizes and styles its output for legibility.
pub struct DebugJson<S: Serialize>(pub S);

impl<S: Serialize> std::fmt::Debug for DebugJson<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use colored_json::{ColorMode, ColoredFormatter, CompactFormatter, Output, Styler};

        let value = ColoredFormatter::with_styler(
            CompactFormatter {},
            // This can be customized, but it's default already matches `jq` 👍.
            Styler::default(),
        )
        .to_colored_json(
            &serde_json::to_value(&self.0).unwrap(),
            ColorMode::Auto(Output::StdErr),
        )
        .unwrap();

        f.write_str(&value)
    }
}

// Merge non-zero counts from `from` into `to`.
#[inline]
pub fn merge_docs_and_bytes(from: &stats::DocsAndBytes, to: &mut Option<stats::DocsAndBytes>) {
    if from.docs_total != 0 {
        let entry = to.get_or_insert_with(Default::default);
        entry.docs_total += from.docs_total;
        entry.bytes_total += from.bytes_total;
    }
}

#[cfg(test)]
mod test {
    use super::{merge_docs_and_bytes, Log, LogLevel};
    use crate::new_encoded_json_write_handler;
    use proto_flow::ops::stats;
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

        handler(&log);
        log.message = "I'm different!".to_string();
        handler(&log);

        std::mem::drop(handler);
        let writer = Arc::try_unwrap(writer).unwrap().into_inner().unwrap();

        insta::assert_snapshot!(String::from_utf8_lossy(&writer), @r###"
        {"ts":"1970-01-01T00:00:00+00:00","level":"warn","message":"hello world","fields":{"name":"value"}}
        {"ts":"1970-01-01T00:00:00+00:00","level":"warn","message":"I'm different!","fields":{"name":"value"}}
        "###);
    }

    #[test]
    fn test_merge_stats() {
        let mut to = None;

        merge_docs_and_bytes(
            &stats::DocsAndBytes {
                docs_total: 0,
                bytes_total: 0,
            },
            &mut to,
        );
        assert_eq!(to, None);

        merge_docs_and_bytes(
            &stats::DocsAndBytes {
                docs_total: 1,
                bytes_total: 10,
            },
            &mut to,
        );
        assert_eq!(
            to,
            Some(stats::DocsAndBytes {
                docs_total: 1,
                bytes_total: 10
            })
        );

        merge_docs_and_bytes(
            &stats::DocsAndBytes {
                docs_total: 2,
                bytes_total: 20,
            },
            &mut to,
        );
        assert_eq!(
            to,
            Some(stats::DocsAndBytes {
                docs_total: 3,
                bytes_total: 30
            })
        );
    }
}
