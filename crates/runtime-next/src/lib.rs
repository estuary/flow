//! `runtime-next` hosts both the controller-facing `Shard` gRPC service
//! (per-shard, in `crate::materialize::shard`) and the `Leader` gRPC
//! service (sidecar, in `crate::leader` and `crate::materialize::leader`).
//! Each shard's `Shard` stream terminates both the controller-bound and
//! leader-bound `runtime.proto` streams, and translates between them and
//! the connector RPC. The only message that flows end-to-end unmodified
//! is `Stop` (controller → runtime-next → leader).
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

mod handler;
pub mod leader;
pub mod materialize;
pub mod publish;
pub mod recovery;
mod rocksdb;
mod task_service;

pub use container::flow_runtime_protocol;

use std::sync::Arc;

pub use leader::Service;
pub use publish::Publisher;
pub use rocksdb::{RocksDB, extend_write_batch};
pub use task_service::TaskService;
pub use tokio_context::TokioContext;

/// Maximum accepted protobuf message size on Shard / Leader streams.
pub const MAX_MESSAGE_SIZE: usize = 1 << 26; // 64MB.

/// CHANNEL_BUFFER for connector RPC pipelines, shared with `runtime`.
pub const CHANNEL_BUFFER: usize = 16;

/// Interval at which leader actor event loops tick, ensuring per-loop tracing
/// instrumentation fires periodically even when no other events arrive.
pub(crate) const ACTOR_TICK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// Construct an mpsc channel sized for runtime-next inter-task plumbing.
/// Used for controller-, leader-, and connector-bound message channels;
/// callers send via `verify_send` so a Full result fails fast as a
/// capacity-invariant violation.
pub(crate) fn new_channel<T>() -> (tokio::sync::mpsc::Sender<T>, tokio::sync::mpsc::Receiver<T>) {
    tokio::sync::mpsc::channel::<T>(32)
}

/// Non-blocking channel send that enforces capacity invariants.
///
/// Returns an error on `Full` — the caller's design must guarantee
/// sufficient capacity. A Full here means a capacity invariant is
/// violated. The error propagates up to tear down the session (fail-fast).
///
/// Silently drops on `Closed` — the peer has exited. The caller will
/// discover a more informative error from the peer's rx stream.
pub(crate) fn verify_send<T>(tx: &tokio::sync::mpsc::Sender<T>, value: T) -> anyhow::Result<()> {
    use tokio::sync::mpsc::error::TrySendError;
    match tx.try_send(value) {
        Ok(()) => Ok(()),
        Err(TrySendError::Closed(_)) => Ok(()),
        Err(TrySendError::Full(_)) => {
            anyhow::bail!("verify_send: channel full; capacity invariant violated")
        }
    }
}

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

// Map an anyhow::Error into a tonic::Status.
// If the error is already a Status, it's downcast.
// Otherwise, an internal error is used to wrap a formatted anyhow::Error chain.
pub fn anyhow_to_status(err: anyhow::Error) -> tonic::Status {
    match err.downcast::<tonic::Status>() {
        Ok(status) => status,
        Err(err) => tonic::Status::unknown(format!("{err:?}")),
    }
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

pub trait LogHandler: Send + Sync + Clone + 'static {
    fn log(&self, log: &ops::Log);

    fn as_fn(self) -> impl Fn(&ops::Log) + Send + Sync + 'static {
        move |log| self.log(log)
    }
}

impl<T: Fn(&ops::Log) + Send + Sync + Clone + 'static> LogHandler for T {
    fn log(&self, log: &ops::Log) {
        self(log)
    }
}

/// Runtime implements the various services that constitute the Flow Runtime.
#[derive(Clone)]
pub struct Runtime<L: LogHandler> {
    pub plane: Plane,
    pub container_network: String,
    pub log_handler: L,
    pub set_log_level: Option<Arc<dyn Fn(ops::LogLevel) + Send + Sync>>,
    pub task_name: String,
    pub publisher_factory: gazette::journal::ClientFactory,
}

impl<L: LogHandler> Runtime<L> {
    /// Build a new Runtime.
    /// - `plane`: the type of data plane in which this Runtime is operating.
    /// - `container_network`: the Docker container network used for connector containers.
    /// - `log_handler`: handler to which connector logs are dispatched.
    /// - `set_log_level`: callback for adjusting the log level implied by runtime requests.
    /// - `task_name`: name which is used to label any started connector containers.
    /// - `publisher_factory`: client factory for creating and appending to collection partitions.
    pub fn new(
        plane: Plane,
        container_network: String,
        log_handler: L,
        set_log_level: Option<Arc<dyn Fn(ops::LogLevel) + Send + Sync>>,
        task_name: String,
        publisher_factory: gazette::journal::ClientFactory,
    ) -> Self {
        Self {
            plane,
            container_network,
            log_handler,
            set_log_level,
            task_name,
            publisher_factory,
        }
    }

    /// Apply the dynamic log level if a setter was provided.
    pub fn set_log_level(&self, level: ops::LogLevel) {
        if level == ops::LogLevel::UndefinedLevel {
            // No-op
        } else if let Some(set_log_level) = &self.set_log_level {
            (set_log_level)(level);
        }
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
// If an expectation fails, it produces a suitable error message annotated with
// the originating peer and stream index.
pub(crate) fn verify<'p>(
    source: &'static str,
    expect: &'static str,
    peer: &'p str,
    peer_index: usize,
) -> Verify<'p> {
    Verify {
        source,
        expect,
        peer,
        peer_index,
    }
}

pub(crate) struct Verify<'p> {
    source: &'static str,
    expect: &'static str,
    peer: &'p str,
    peer_index: usize,
}

impl<'p> Verify<'p> {
    #[inline]
    pub(crate) fn ok<T>(&self, t: tonic::Result<T>) -> anyhow::Result<T> {
        match t {
            Ok(t) => Ok(t),
            Err(status) => Err(self.fail_status(status)),
        }
    }

    #[allow(dead_code)]
    #[inline]
    pub(crate) fn eof<T: serde::Serialize>(
        &self,
        t: Option<tonic::Result<T>>,
    ) -> anyhow::Result<()> {
        match t {
            None => Ok(()),
            Some(Err(status)) => Err(self.fail_status(status)),
            Some(Ok(t)) => Err(self.fail_msg(t)),
        }
    }

    #[inline]
    pub(crate) fn not_eof<T>(&self, t: Option<tonic::Result<T>>) -> anyhow::Result<T> {
        if let Some(t) = t {
            Ok(self.ok(t)?)
        } else {
            Err(self.fail_err(anyhow::anyhow!("unexpected EOF")))
        }
    }

    #[must_use]
    #[cold]
    pub(crate) fn fail_msg<T: serde::Serialize>(&self, msg: T) -> anyhow::Error {
        let Self {
            source,
            expect,
            peer,
            peer_index,
        } = self;

        let mut t = serde_json::to_string(&msg).unwrap();
        t.truncate(4096);

        anyhow::format_err!(
            "{source} protocol error (expected {expect}) from {peer}@{peer_index}: {t}"
        )
    }

    #[must_use]
    #[cold]
    pub(crate) fn fail_err(&self, err: anyhow::Error) -> anyhow::Error {
        let Self {
            source,
            expect,
            peer,
            peer_index,
        } = self;

        err.context(format!(
            "{source} error (expected {expect}) from {peer}@{peer_index}"
        ))
    }

    #[must_use]
    #[cold]
    pub(crate) fn fail_status(&self, status: tonic::Status) -> anyhow::Error {
        self.fail_err(crate::status_to_anyhow(status))
    }
}
