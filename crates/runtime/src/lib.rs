use anyhow::Context;
use futures::TryStreamExt;
use std::sync::Arc;

mod capture;
mod combine;
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
pub mod uuid;

pub use container::{flow_runtime_protocol, DEKAF_IMAGE_NAME_PREFIX};
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
///
/// Note that there's an unfortunate mismatch in how `RuntimeProtocol::Materialize`
/// is represented in the control-plane database versus the image labels. We might
/// in the future want to have more general and flexible `TryFrom<&str>` and `Display`
/// impls, but for now there are only specific named functions since there are few places
/// where we actually use this. The `Serialize` impl uses the image label representation,
/// though that decision was made arbitrarily.
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

impl RuntimeProtocol {
    /// Returns the appropriate representation for storing in the control plane database.
    pub fn database_string_value(&self) -> &'static str {
        match self {
            RuntimeProtocol::Capture => "capture",
            RuntimeProtocol::Materialize => "materialization",
            RuntimeProtocol::Derive => "derive",
        }
    }

    pub fn from_database_string_value(proto: &str) -> Option<Self> {
        match proto {
            "capture" => Some(RuntimeProtocol::Capture),
            "materialization" => Some(RuntimeProtocol::Materialize),
            "derive" => Some(RuntimeProtocol::Derive),
            _ => None,
        }
    }
}

// Map an anyhow::Error into a tonic::Status.
// If the error is already a Status, it's downcast.
// Otherwise, an internal error is used to wrap a formatted anyhow::Error chain.
pub fn anyhow_to_status(err: anyhow::Error) -> tonic::Status {
    match err.downcast::<tonic::Status>() {
        Ok(status) => status,
        Err(err) => tonic::Status::internal(format!("{err:?}")),
    }
}

// Map a tonic::Status into an anyhow::Error.
// If the status is an internal error, its message is extracted into a dynamic anyhow::Error.
// Otherwise the Status is wrapped by a dynamic anyhow::Error, and may be downcast again.
pub fn status_to_anyhow(status: tonic::Status) -> anyhow::Error {
    match status.code() {
        // Unwrap Internal (only), as this code is consistently used for user-facing errors.
        // Note that non-Status errors are wrapped with Internal when mapping back into Status.
        tonic::Code::Internal => anyhow::anyhow!(status.message().to_owned()),
        // For all other Status types, pass through the Status in order to preserve a
        // capability to lossless-ly downcast back to the Status later.
        _ => anyhow::Error::new(status),
    }
}

fn stream_error_to_status<T, S: futures::Stream<Item = anyhow::Result<T>>>(
    s: S,
) -> impl futures::Stream<Item = tonic::Result<T>> {
    s.map_err(anyhow_to_status)
}

fn stream_status_to_error<T, S: futures::Stream<Item = tonic::Result<T>>>(
    s: S,
) -> impl futures::Stream<Item = anyhow::Result<T>> {
    s.map_err(status_to_anyhow)
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
    pub fn set_log_level(&self, level: ops::LogLevel) {
        if level == ops::LogLevel::UndefinedLevel {
            // No-op
        } else if let Some(set_log_level) = &self.set_log_level {
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
                proto_grpc::materialize::connector_server::ConnectorServer::new(self.clone())
                    .max_decoding_message_size(usize::MAX) // Up from 4MB. Accept whatever the Go runtime sends.
                    .max_encoding_message_size(usize::MAX), // The default, made explicit.
            )
            .add_service(
                proto_grpc::runtime::combiner_server::CombinerServer::new(self)
                    .max_decoding_message_size(usize::MAX) // Up from 4MB. Accept whatever the Go runtime sends.
                    .max_encoding_message_size(usize::MAX), // The default, made explicit.
            )
    }
}

fn parse_shard_labeling(
    shard: Option<&proto_gazette::consumer::ShardSpec>,
) -> anyhow::Result<ops::ShardLabeling> {
    let Some(shard) = shard else {
        anyhow::bail!("missing shard")
    };
    let Some(set) = &shard.labels else {
        anyhow::bail!("missing shard labels")
    };
    labels::shard::decode_labeling(set).context("parsing shard labeling")
}

struct Accumulator(doc::combine::Accumulator, simd_doc::Parser);

impl Accumulator {
    fn new(spec: doc::combine::Spec) -> anyhow::Result<Self> {
        Ok(Self(
            doc::combine::Accumulator::new(spec, tempfile::tempfile()?)?,
            simd_doc::Parser::new(),
        ))
    }

    // Parse document bytes into a HeapNode backed by the Accumulator's current
    // MemTable and Allocator. Return the MemTable, Allocator, and HeapNode.
    fn doc_bytes_to_heap_node<'a>(
        &'a mut self,
        doc_bytes: &[u8],
    ) -> anyhow::Result<(
        &'a doc::combine::MemTable,
        &'a doc::Allocator,
        doc::HeapNode<'a>,
    )> {
        // Currently, we assume that `doc_bytes` is a JSON document.
        // In the future, it could be an ArchivedNode serialization.
        let memtable = self.0.memtable()?;
        let alloc = memtable.alloc();
        Ok((memtable, alloc, self.1.parse_one(doc_bytes, alloc)?))
    }

    fn into_drainer(
        self,
    ) -> Result<(doc::combine::Drainer, simd_doc::Parser), doc::combine::Error> {
        Ok((self.0.into_drainer()?, self.1))
    }

    fn from_drainer(
        drainer: doc::combine::Drainer,
        parser: simd_doc::Parser,
    ) -> Result<Self, doc::combine::Error> {
        Ok(Self(drainer.into_new_accumulator()?, parser))
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

fn truncate_chars(s: &str, max_chars: usize) -> &str {
    match s.char_indices().nth(max_chars) {
        None => s,
        Some((idx, _)) => &s[..idx],
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_truncate_chars() {
        use super::truncate_chars;
        let s = "ボAルBテックス";
        assert_eq!(truncate_chars(s, 0), "");
        assert_eq!(truncate_chars(s, 6), "ボAルBテッ");
        assert_eq!(truncate_chars(s, 100), s);
    }
}

// Maximum accepted message size.
pub const MAX_MESSAGE_SIZE: usize = 1 << 26; // 64MB.
