//! `runtime-next` hosts both the controller-facing `Shard` gRPC service
//! (per-shard, in `crate::shard`) and the `Leader` gRPC service (sidecar,
//! in `crate::leader`). Each shard's `Shard` stream terminates both the
//! controller-bound and leader-bound `runtime.proto` streams, and
//! translates between them and the connector RPC. The only messages that
//! flow end-to-end unmodified are `Stop` and `CloseNow`
//! (controller → runtime-next → leader).
//!
//! "Controller" here is the peer that drives the shard's lifecycle: the
//! Go runtime in production, an in-process driver such as `flowctl
//! preview`, or a unit-test harness. This crate is agnostic to which.
//!
//! `runtime-next` will eventually replace `runtime`; during the parity
//! period both crates coexist and the controller selects between them
//! per-task at startup. `runtime-next` MUST NOT depend on `runtime` —
//! files shared between the two crates live physically in `runtime/` and
//! are pulled into `runtime-next` via `#[path]`.

// `runtime` shares files with this crate via `#[path]`. Those shared files
// reference symbols as `runtime_next::*` so the path resolves identically
// from `runtime` (which has runtime-next as a dependency) and from this
// crate compiling them in-tree.
extern crate self as runtime_next;

pub use ::proto_flow::runtime::Plane; // Re-export.
/// Re-export of `proto_flow::runtime` so that this crate (and its dependents)
/// can refer to protocol message types as `crate::proto::*` /
/// `runtime_next::proto::*`, avoiding the naming conflict between this crate
/// and the protobuf module.
pub use proto_flow::runtime as proto;

mod container;
mod image_connector;
mod local_connector;
mod tokio_context;

pub mod leader;
pub mod logger;
pub mod patches;
pub mod publish;
pub mod shard;
mod task_service;

pub use container::flow_runtime_protocol;

pub use leader::{Service, ShuffleServiceFactory, ShuffleSession, ShuffleSessionFactory};
pub use logger::{
    FnLogger, FnLoggerFactory, LogEvent, Logger, LoggerFactory, TracingLogger, TracingLoggerFactory,
};
pub use publish::{
    JournalPublisher, JournalPublisherFactory, Publisher, PublisherFactory, new_producer,
};
pub use task_service::TaskService;
pub use tokio_context::TokioContext;

/// Maximum accepted protobuf message size on Shard / Leader streams.
pub const MAX_MESSAGE_SIZE: usize = 1 << 26; // 64MB.

/// CHANNEL_BUFFER for connector RPC pipelines, shared with `runtime`.
pub const CHANNEL_BUFFER: usize = 16;

// This constant is shared between Rust and Go code.
// See go/protocols/flow/document_extensions.go.
pub const UUID_PLACEHOLDER: &str = "DocUUIDPlaceholder-329Bb50aa48EAa9ef";

/// JSON-Schema annotation used to track collection generations in inferred
/// schemas.
pub const X_GENERATION_ID: &str = "x-collection-generation-id";

/// Interval at which leader actor event loops tick, ensuring per-loop tracing
/// instrumentation fires periodically even when no other events arrive.
pub(crate) const ACTOR_TICK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// Describes the basic type of runtime protocol. Mirrors `runtime::RuntimeProtocol`
/// so that connector image inspection (Phase F-ported `container::flow_runtime_protocol`)
/// can return a type that's local to this crate.
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

/// Maximum byte length of a `tonic::Status` message that we build from a
/// formatted error. gRPC status text rides in an HTTP/2 trailer; an oversized
/// trailer forces the header block across many CONTINUATION frames, tripping
/// `h2`'s `too_many_continuations` guard, which aborts the connection and
/// *replaces* the real status text with an opaque transport error
///
/// The ceiling exists so that *any* error, anticipated or not, stays within a
/// single HTTP/2 frame (default `max_frame_size` is 16 KiB) and never needs a
/// CONTINUATION frame at all. The `grpc-message` trailer is percent-encoded, so
/// a byte can expand up to 3x (`%XX`); 4 KiB raw is ≤ 12 KiB encoded, fitting
/// one frame even in the worst case.
pub const MAX_STATUS_MESSAGE_LEN: usize = 4096;

// Map an anyhow::Error into a tonic::Status.
// If the error is already a Status, it's downcast (and bounded).
// Otherwise, an internal error is used to wrap a formatted anyhow::Error chain,
// bounded to MAX_STATUS_MESSAGE_LEN.
pub fn anyhow_to_status(err: anyhow::Error) -> tonic::Status {
    match err.downcast::<tonic::Status>() {
        Ok(status) => bound_status(status),
        Err(err) => bounded_unknown_status(format!("{err:?}")),
    }
}

/// Build a `tonic::Status::unknown` whose message is bounded to
/// [`MAX_STATUS_MESSAGE_LEN`] bytes. The anyhow debug format leads with the
/// error's top-level context and appends lower-level detail, so truncating the
/// tail preserves the human-meaningful prefix.
pub(crate) fn bounded_unknown_status(message: String) -> tonic::Status {
    tonic::Status::unknown(bound_message(message))
}

/// Bound the message of a `Status` we did not format ourselves — one round
/// tripped from a peer or produced by a connector — preserving its code,
/// details, and metadata. Guards the same h2 trailer limit as
/// [`bounded_unknown_status`] so that e.g. a misbehaving connector emitting a
/// huge message can't produce a status that's dropped in transit. Returns the
/// status untouched when its message already fits.
pub(crate) fn bound_status(status: tonic::Status) -> tonic::Status {
    if status.message().len() <= MAX_STATUS_MESSAGE_LEN {
        return status;
    }
    tonic::Status::with_details_and_metadata(
        status.code(),
        bound_message(status.message().to_string()),
        bytes::Bytes::copy_from_slice(status.details()),
        status.metadata().clone(),
    )
}

/// Truncate `message` to at most [`MAX_STATUS_MESSAGE_LEN`] bytes, cutting the
/// tail on a UTF-8 boundary and marking the elision. Returns it unchanged when
/// it already fits.
fn bound_message(mut message: String) -> String {
    if message.len() > MAX_STATUS_MESSAGE_LEN {
        const SUFFIX: &str = "… [truncated]";
        // Reserve room for SUFFIX and back off to a UTF-8 char boundary.
        let mut end = MAX_STATUS_MESSAGE_LEN - SUFFIX.len();
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
        message.push_str(SUFFIX);
    }
    message
}

// Map a tonic::Status into an anyhow::Error.
// If the status is an internal error, its message is extracted into a dynamic anyhow::Error.
// Otherwise the Status is wrapped by a dynamic anyhow::Error, and may be downcast again.
pub fn status_to_anyhow(status: tonic::Status) -> anyhow::Error {
    match status.code() {
        // Unwrap Unknown (only), as this code is consistently used for user-facing errors.
        // Note that non-Status errors are wrapped with Unknown when mapping back into Status.
        tonic::Code::Unknown => anyhow::anyhow!(status.message().to_owned()),
        // For all other Status types, pass through the Status in order to preserve a
        // capability to lossless-ly downcast back to the Status later.
        _ => anyhow::Error::new(status),
    }
}

struct Accumulator(doc::combine::Accumulator, simd_doc::Parser);

impl Accumulator {
    pub fn new(spec: doc::combine::Spec) -> anyhow::Result<Self> {
        Ok(Self(
            doc::combine::Accumulator::new(spec, tempfile::tempfile()?)?,
            simd_doc::Parser::new(),
        ))
    }

    pub fn memtable(&mut self) -> Result<&doc::combine::MemTable, doc::combine::Error> {
        self.0.memtable()
    }

    /// On-disk byte usage of the combiner's spill file.
    pub fn combiner_byte_usage(&self) -> u64 {
        self.0.ranges().last().map(|r| r.end).unwrap_or(0)
    }

    /// Parse one JSON document into a HeapNode backed by the Accumulator's
    /// current MemTable and Allocator.
    pub fn parse_json_doc<'a>(
        &'a mut self,
        doc_bytes: &[u8],
    ) -> anyhow::Result<(
        &'a doc::combine::MemTable,
        &'a doc::Allocator,
        doc::HeapNode<'a>,
    )> {
        let memtable = self.0.memtable()?;
        let alloc = memtable.alloc();
        Ok((memtable, alloc, self.1.parse_one(doc_bytes, alloc)?))
    }

    pub fn into_drainer(
        self,
    ) -> Result<(doc::combine::Drainer, simd_doc::Parser), doc::combine::Error> {
        Ok((self.0.into_drainer()?, self.1))
    }

    pub fn from_drainer(
        drainer: doc::combine::Drainer,
        parser: simd_doc::Parser,
    ) -> Result<Self, doc::combine::Error> {
        Ok(Self(drainer.into_new_accumulator()?, parser))
    }
}

// `verify` is a convenience for building protocol error messages in a standard,
// structured way. You call `verify` to establish a `Verify` instance, which
// is then used to assert expectations over protocol requests or responses.
// If an expectation fails, it produces a suitable error message annotated
// with the originating peer.
pub fn verify<'p>(source: &'static str, expect: &'static str, peer: &'p str) -> Verify<'p> {
    Verify {
        source,
        expect,
        peer,
    }
}

pub struct Verify<'p> {
    source: &'static str,
    expect: &'static str,
    peer: &'p str,
}

impl<'p> Verify<'p> {
    #[inline]
    pub fn ok<T>(&self, t: tonic::Result<T>) -> anyhow::Result<T> {
        match t {
            Ok(t) => Ok(t),
            Err(status) => Err(self.fail_status(status)),
        }
    }

    #[inline]
    pub fn eof<T: serde::Serialize>(&self, t: Option<tonic::Result<T>>) -> anyhow::Result<()> {
        match t {
            None => Ok(()),
            Some(Err(status)) => Err(self.fail_status(status)),
            Some(Ok(t)) => Err(self.fail_msg(t)),
        }
    }

    #[inline]
    pub fn not_eof<T>(&self, t: Option<tonic::Result<T>>) -> anyhow::Result<T> {
        if let Some(t) = t {
            Ok(self.ok(t)?)
        } else {
            Err(self.fail_err(anyhow::anyhow!("unexpected EOF")))
        }
    }

    #[must_use]
    #[cold]
    pub fn fail_msg<T: serde::Serialize>(&self, msg: T) -> anyhow::Error {
        let Self {
            source,
            expect,
            peer,
        } = self;

        let mut t = serde_json::to_string(&msg).unwrap();
        t.truncate(4096);

        anyhow::format_err!("{source} protocol error (expected {expect}) from {peer}: {t}")
    }

    #[must_use]
    #[cold]
    pub fn fail_err(&self, err: anyhow::Error) -> anyhow::Error {
        let Self {
            source,
            expect,
            peer,
        } = self;

        err.context(format!("{source} error (expected {expect}) from {peer}"))
    }

    #[must_use]
    #[cold]
    pub fn fail_status(&self, status: tonic::Status) -> anyhow::Error {
        self.fail_err(crate::status_to_anyhow(status))
    }
}
