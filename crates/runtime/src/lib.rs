use futures::TryStreamExt;
use std::fmt::{self, Display};
use std::sync::Arc;

mod capture;
mod container;
mod derive;
pub mod harness;
mod image_connector;
mod local_connector;
mod materialize;
mod rocksdb;
mod task_service;
mod tokio_context;
mod unary;
mod unseal;
pub mod uuid;

pub use container::flow_runtime_protocol;
pub use task_service::TaskService;
pub use tokio_context::TokioContext;

// This constant is shared between Rust and Go code.
// See go/protocols/flow/document_extensions.go.
pub const UUID_PLACEHOLDER: &str = "DocUUIDPlaceholder-329Bb50aa48EAa9ef";

/// CHANNEL_BUFFER is the standard buffer size used for holding documents in an
/// asynchronous processing pipeline. User documents can be large -- up to 64MB --
/// so this value should be small. At the same time, processing steps such as
/// schema validation are greatly accelerated when they can loop over multiple
/// documents without yielding, so it should not be *too* small.
pub const CHANNEL_BUFFER: usize = 16;

/// Describes the basic type of runtime protocol. This corresponds to the
/// `FLOW_RUNTIME_PROTOCOL` label that's used on docker images.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RuntimeProtocol {
    Capture,
    Materialization,
    // Derivation, // eventually, maybe
}

impl<'a> TryFrom<&'a str> for RuntimeProtocol {
    type Error = &'a str;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        match value {
            "capture" => Ok(RuntimeProtocol::Capture),
            "materialization" => Ok(RuntimeProtocol::Materialization),
            other => Err(other),
        }
    }
}

impl Display for RuntimeProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            RuntimeProtocol::Capture => "capture",
            RuntimeProtocol::Materialization => "materialization",
        };
        f.write_str(s)
    }
}

fn anyhow_to_status(err: anyhow::Error) -> tonic::Status {
    tonic::Status::internal(format!("{err:?}"))
}

fn stream_error_to_status<T, S: futures::Stream<Item = anyhow::Result<T>>>(
    s: S,
) -> impl futures::Stream<Item = tonic::Result<T>> {
    s.map_err(|err: anyhow::Error| match err.downcast::<tonic::Status>() {
        Ok(status) => status,
        Err(err) => anyhow_to_status(err),
    })
}

fn stream_status_to_error<T, S: futures::Stream<Item = tonic::Result<T>>>(
    s: S,
) -> impl futures::Stream<Item = anyhow::Result<T>> {
    s.map_err(|status| match status.code() {
        // Unwrap Internal (only), as this code is consistently used for user-facing errors.
        // Note that non-Status errors are wrapped with Internal when mapping back into Status.
        tonic::Code::Internal => anyhow::anyhow!(status.message().to_owned()),
        // For all other Status types, pass through the Status in order to preserve a
        // capability to lossless-ly downcast back to the Status later.
        _ => anyhow::Error::new(status),
    })
}

pub trait LogHandler: Fn(&ops::Log) + Send + Sync + Clone + 'static {}
impl<T: Fn(&ops::Log) + Send + Sync + Clone + 'static> LogHandler for T {}

/// Runtime implements the various services that constitute the Flow Runtime.
#[derive(Clone)]
pub struct Runtime<L: LogHandler> {
    allow_local: bool,
    container_network: String,
    log_handler: L,
    set_log_level: Option<Arc<dyn Fn(ops::LogLevel) + Send + Sync>>,
    task_name: String,
}

impl<L: LogHandler> Runtime<L> {
    /// Build a new Runtime.
    /// * `allow_local`: Whether local connectors are permitted by this Runtime.
    /// * `container_network`: the Docker container network used for connector containers.
    /// * `log_handler`: handler to which connector logs are dispatched.
    /// * `set_log_level`: callback for adjusting the log level implied by runtime requests.
    /// * `task_name`: name which is used to label any started connector containers.
    pub fn new(
        allow_local: bool,
        container_network: String,
        log_handler: L,
        set_log_level: Option<Arc<dyn Fn(ops::LogLevel) + Send + Sync>>,
        task_name: String,
    ) -> Self {
        Self {
            allow_local,
            container_network,
            log_handler,
            set_log_level,
            task_name,
        }
    }

    /// Attempt to set the dynamic log level to the given `level`.
    pub fn set_log_level(&self, level: Option<ops::LogLevel>) {
        if let (Some(level), Some(set_log_level)) = (level, &self.set_log_level) {
            (set_log_level)(level);
        }
    }

    /// Build a tonic Server which includes all of the Runtime's services.
    pub fn build_tonic_server(self) -> tonic::transport::server::Router {
        tonic::transport::Server::builder()
            .add_service(
                proto_grpc::capture::connector_server::ConnectorServer::new(self.clone())
                    .max_decoding_message_size(usize::MAX) // Up from 4MB. Accept whatever the Go runtime sends.
                    .max_encoding_message_size(usize::MAX), // The default, made explicit.
            )
            .add_service(
                proto_grpc::derive::connector_server::ConnectorServer::new(self.clone())
                    .max_decoding_message_size(usize::MAX) // Up from 4MB. Accept whatever the Go runtime sends.
                    .max_encoding_message_size(usize::MAX), // The default, made explicit.
            )
            .add_service(
                proto_grpc::materialize::connector_server::ConnectorServer::new(self)
                    .max_decoding_message_size(usize::MAX) // Up from 4MB. Accept whatever the Go runtime sends.
                    .max_encoding_message_size(usize::MAX), // The default, made explicit.
            )
    }
}

// Extract a LogLevel from a ShardSpec.
fn shard_log_level(shard: Option<&proto_gazette::consumer::ShardSpec>) -> Option<ops::LogLevel> {
    let labels = shard
        .and_then(|shard| shard.labels.as_ref())
        .map(|l| l.labels.as_slice());

    let Some(labels) = labels else {
        return None;
    };
    match labels.binary_search_by(|label| label.name.as_str().cmp(::labels::LOG_LEVEL)) {
        Ok(index) => ops::LogLevel::from_str_name(&labels[index].value),
        Err(_index) => None,
    }
}

// verify is a convenience for building protocol error messages in a standard, structured way.
// You call verify to establish a Verify instance, which is then used to assert expectations
// over protocol requests or responses.
// If an expectation fails, it produces a suitable error message.
fn verify(source: &'static str, expect: &'static str) -> Verify {
    Verify { source, expect }
}

struct Verify {
    source: &'static str,
    expect: &'static str,
}

impl Verify {
    #[must_use]
    #[inline]
    fn not_eof<T>(&self, t: Option<T>) -> anyhow::Result<T> {
        if let Some(t) = t {
            Ok(t)
        } else {
            self.fail(Option::<()>::None)
        }
    }

    #[must_use]
    #[inline]
    fn is_eof<T: serde::Serialize>(&self, t: Option<T>) -> anyhow::Result<()> {
        if let Some(t) = t {
            self.fail(t)
        } else {
            Ok(())
        }
    }

    #[must_use]
    #[cold]
    fn fail<Ok, T: serde::Serialize>(&self, t: T) -> anyhow::Result<Ok> {
        let (source, expect) = (self.source, self.expect);

        let mut t = serde_json::to_string(&t).unwrap();
        t.truncate(4096);

        if t == "null" {
            Err(anyhow::format_err!(
                "unexpected {source} EOF (expected {expect})"
            ))
        } else {
            Err(anyhow::format_err!(
                "{source} protocol error (expected {expect}): {t}"
            ))
        }
    }
}

/// exchange is a combinator for avoiding deadlocks. It sends into a request
/// Stream while concurrently polling and yielding responses of a corresponding
/// response Stream. It returns a stream which completes once the send has
/// completed.
///
/// `exchange` mitigates an extremely common deadlock mistake, of sending into
/// a receiver without consideration for whether the receiver may be unable to
/// receive because it's output channel is stuffed and is not being serviced.
/// This is a generalized problem -- in no way unique to Rust -- but the polled
/// nature of Futures and Streams accentuate it because receiving from a Stream
/// is *also* polling it, allowing it to perform other important activity even
/// if it cannot immediately yield an item.
fn exchange<'s, Request, Tx, Response, Rx>(
    request: Request,
    tx: &'s mut Tx,
    rx: &'s mut Rx,
) -> impl futures::Stream<Item = Response> + 's
where
    Request: 'static,
    Tx: futures::Sink<Request> + Unpin + 's,
    Rx: futures::Stream<Item = Response> + Unpin + 's,
{
    use futures::{SinkExt, StreamExt};

    futures::stream::unfold((tx.feed(request), rx), move |(mut feed, rx)| async move {
        tokio::select! {
            biased;

            // We suppress a `feed` error, which represents a disconnection / reset,
            // because a better and causal error will invariably be surfaced by `rx`.
            _result = &mut feed => None,

            response = rx.next() => if let Some(response) = response {
                Some((response, (feed, rx)))
            } else {
                None
            },
        }
    })
}

// Maximum accepted message size.
const MAX_MESSAGE_SIZE: usize = 1 << 26; // 64MB.
