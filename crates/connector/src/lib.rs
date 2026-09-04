//! `connector` owns every part of running a Flow connector: extracting and
//! unsealing its endpoint configuration, injecting IAM credentials, dispatching
//! to a docker image / local subprocess / in-process connector, pumping its
//! logs, and tearing it down in the right order.
//!
//! All of that is reachable through exactly one protocol,
//! [`connector.Connector`](proto), served three ways from a single [`Service`]:
//!
//! - in-process, via [`Service::spawn_connector`], with no wire hop;
//! - over gRPC, via [`Service::into_tonic_service`], on every `TaskService`'s
//!   Unix domain socket;
//! - over gRPC on the reactor's public address, through a Go pass-through
//!   proxy, for callers outside the reactor bearing a `PROXY_CONNECTOR` token.
//!
//! Callers use [`proto_grpc::connector::Router`]. This crate provides
//! [`ServiceRouter`] for processes which host their connector service.
//!
//! See `README.md` for the protocol contract and non-obvious details.

pub use proto_flow::connector as proto;
pub use proto_flow::runtime::{Container, Plane};

mod capture;
mod container;
mod derive;
mod image;
mod local;
mod materialize;
mod router;
mod serve;
mod service;

#[cfg(test)]
mod tests;

pub use container::flow_runtime_protocol;
pub(crate) use proto_grpc::connector::SPEC_TASK_NAME;
pub(crate) use proto_grpc::{status_to_anyhow, verify};
pub use router::{LOCAL_ISSUER, ServiceRouter};
pub use service::Service;

/// Sink for the connector's log stream: its own decoded `ops::Log` lines, plus
/// the three container lifecycle records this crate renders itself.
///
/// Cloning fans a sink out to a stream's log pumps, and is also
/// how [`LogDest::Response`] knows when the last of them is done.
#[derive(Clone)]
pub(crate) struct LogSink(std::sync::Arc<LogDest>);

enum LogDest {
    /// Forwards each log onto a served stream's response channel. Its
    /// `oneshot::Sender` is never sent on: it drops with the last clone of the
    /// sink — every clone lives inside a log pump — which is exactly how
    /// `serve` learns the connector's stderr has been read through.
    Response(
        tokio::sync::mpsc::Sender<tonic::Result<proto::Response>>,
        tokio::sync::oneshot::Sender<()>,
    ),
    /// Traces, for contexts having no response stream to sink into: image
    /// inspection, and this crate's own tests.
    Tracing,
}

impl LogSink {
    pub(crate) fn response(
        response_tx: tokio::sync::mpsc::Sender<tonic::Result<proto::Response>>,
        read_through: tokio::sync::oneshot::Sender<()>,
    ) -> Self {
        Self(std::sync::Arc::new(LogDest::Response(
            response_tx,
            read_through,
        )))
    }

    pub(crate) fn tracing() -> Self {
        Self(std::sync::Arc::new(LogDest::Tracing))
    }

    /// Send one log, awaiting its destination. A response channel is bounded,
    /// so a chatty connector back-pressures on its own stderr rather than
    /// being buffered without bound.
    pub(crate) async fn send(&self, log: ops::Log) {
        match &*self.0 {
            LogDest::Response(response_tx, _read_through) => {
                let response = proto::Response {
                    kind: Some(proto::response::Kind::Log(log)),
                };
                _ = response_tx.send(Ok(response)).await; // Ignore a hung-up client.
            }
            LogDest::Tracing => ops::tracing_log_handler(&log),
        }
    }
}

/// Deadline for beginning a graceful session restart ahead of IAM token
/// expiry, so a transaction started near the deadline still has runway.
pub fn token_restart_deadline(
    now: std::time::SystemTime,
    expires_at: std::time::SystemTime,
) -> std::time::SystemTime {
    use std::time::Duration;

    const LONG_LIFETIME: Duration = Duration::from_secs(4 * 3600);
    const LONG_MARGIN: Duration = Duration::from_secs(30 * 60);
    const SHORT_MARGIN: Duration = Duration::from_secs(5 * 60);

    let lifetime = expires_at.duration_since(now).unwrap_or_default();
    let margin = if lifetime >= LONG_LIFETIME {
        LONG_MARGIN
    } else {
        SHORT_MARGIN
    };
    // A pathologically short lifetime restarts immediately rather than never.
    expires_at - margin.min(lifetime)
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

/// A started connector: its request sink, its response stream, and the facts
/// which `Response.Started` reports back to the client.
pub(crate) struct Started<Request, Response> {
    pub connector_tx: tokio::sync::mpsc::Sender<Request>,
    pub connector_rx: futures::stream::BoxStream<'static, tonic::Result<Response>>,
    pub container: Option<Container>,
    pub codec: connector_init::Codec,
    pub token_restart_at: Option<std::time::SystemTime>,
    pub spec: proto::response::started::Spec,
    /// Owns a running container; `serve` drops it to begin teardown. `None`
    /// for local and in-process connectors, which have no container.
    pub guard: Option<container::Guard>,
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

/// Build an error which the `spawn_*` adapter renders as `InvalidArgument`:
/// the client sent a message this stream can't accept.
pub(crate) fn invalid_argument(message: String) -> anyhow::Error {
    anyhow::Error::new(tonic::Status::invalid_argument(message))
}

#[cfg(test)]
mod deadline_tests {
    use super::token_restart_deadline;
    use std::time::Duration;

    #[test]
    fn test_token_restart_deadline_margins() {
        let now = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);

        // One-hour token restarts five minutes early.
        let expires = now + Duration::from_secs(3600);
        assert_eq!(
            token_restart_deadline(now, expires),
            expires - Duration::from_secs(5 * 60)
        );

        // Twelve-hour token restarts thirty minutes early.
        let expires = now + Duration::from_secs(12 * 3600);
        assert_eq!(
            token_restart_deadline(now, expires),
            expires - Duration::from_secs(30 * 60)
        );

        // A lifetime shorter than its margin restarts immediately, not never.
        let expires = now + Duration::from_secs(60);
        assert_eq!(token_restart_deadline(now, expires), now);
        assert_eq!(token_restart_deadline(now, now), now);
    }
}
