//! `connector` owns the mechanics of running a Flow connector: starting and
//! tearing down its docker container, dialing `flow-connector-init` inside it,
//! running a `local:` connector as a subprocess, and pumping the connector's
//! logs.
//!
//! The crate has no notion of a session logger. Everything it reports about a
//! connector -- the connector's own log lines, and the three container
//! lifecycle records this crate renders itself -- is an `ops::Log` pushed into
//! a [`LogSink`] the caller supplies.

pub use proto_flow::runtime::{Container, Plane};

mod container;
pub mod image;
pub mod local;

pub use container::{Guard, flow_runtime_protocol};

/// Sink for the connector's log stream: its own decoded `ops::Log` lines, plus
/// the three container lifecycle records this crate renders itself.
///
/// Cloning fans a sink out to a stream's log pumps.
#[derive(Clone)]
pub struct LogSink(std::sync::Arc<LogDest>);

enum LogDest {
    /// Forwards each log to a caller-supplied handler.
    Handler(Box<dyn Fn(&ops::Log) + Send + Sync>),
    /// Traces, for contexts having no log stream to sink into: image
    /// inspection, and this crate's own tests.
    Tracing,
}

impl LogSink {
    pub fn handler(handler: impl Fn(&ops::Log) + Send + Sync + 'static) -> Self {
        Self(std::sync::Arc::new(LogDest::Handler(Box::new(handler))))
    }

    pub fn tracing() -> Self {
        Self(std::sync::Arc::new(LogDest::Tracing))
    }

    /// Send one log, awaiting its destination.
    pub async fn send(&self, log: ops::Log) {
        match &*self.0 {
            LogDest::Handler(handler) => (handler)(&log),
            LogDest::Tracing => ops::tracing_log_handler(&log),
        }
    }
}

/// Describes the basic type of runtime protocol advertised by a connector
/// image's `FLOW_RUNTIME_PROTOCOL` label.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeProtocol {
    Capture,
    Materialize,
    Derive,
}

impl RuntimeProtocol {
    fn from_image_label(value: &str) -> Result<Self, &str> {
        match value {
            "capture" => Ok(RuntimeProtocol::Capture),
            "materialize" => Ok(RuntimeProtocol::Materialize),
            "derive" => Ok(RuntimeProtocol::Derive),
            other => Err(other),
        }
    }
}

/// Render one `ops::Log` of this crate's own reporting.
pub(crate) fn build_log<'a>(
    level: ops::LogLevel,
    message: &str,
    fields: impl IntoIterator<Item = (&'a str, bytes::Bytes)>,
) -> ops::Log {
    ops::Log {
        meta: None,
        shard: None,
        timestamp: Some(proto_flow::as_timestamp(std::time::SystemTime::now())),
        level: level as i32,
        message: message.to_string(),
        fields_json_map: fields
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
        spans: Vec::new(),
    }
}

pub(crate) fn json_field(value: &impl serde::Serialize) -> bytes::Bytes {
    serde_json::to_vec(value)
        .expect("log field always serializes")
        .into()
}
