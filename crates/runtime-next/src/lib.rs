//! `runtime-next` hosts both the controller-facing `Shard` gRPC service
//! (per-shard, in `crate::shard`) and the `Leader` gRPC service (sidecar,
//! in `crate::leader`). Each shard's `Shard` stream terminates both the
//! controller-bound and leader-bound `runtime.proto` streams, and
//! translates between them and the connector RPC. The only messages that
//! flow end-to-end unmodified are `Stop` and `CloseNow`
//! (controller → runtime-next → leader), and `Synced`
//! (leader → runtime-next → controller).
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
    JournalPublisher, JournalPublisherFactory, Publisher, PublisherFactory, RecordingPublisher,
    RecordingPublisherFactory, new_producer,
};
pub use task_service::TaskService;
pub use tokio_context::TokioContext;

// This constant is shared between Rust and Go code.
// See go/protocols/flow/document_extensions.go.
pub const UUID_PLACEHOLDER: &str = "DocUUIDPlaceholder-329Bb50aa48EAa9ef";

/// JSON-Schema annotation used to track collection generations in inferred
/// schemas.
pub const X_GENERATION_ID: &str = "x-collection-generation-id";

/// Interval at which leader actor event loops tick, ensuring per-loop tracing
/// instrumentation fires periodically even when no other events arrive.
pub(crate) const ACTOR_TICK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// A deadline far enough out to mean "never", for arming a hoisted `Sleep` which
/// has no deadline or whose deadline was consumed.
///
/// Mirrors the private `tokio::time::Instant::far_future`, relying on the same
/// `instant_to_tick` clamp to `MAX_SAFE_MILLIS_DURATION` (~2.2 years).
pub(crate) fn far_future() -> tokio::time::Instant {
    tokio::time::Instant::now() + std::time::Duration::from_secs(86400 * 365 * 30)
}

/// Lowest-priority `select!` arm of an actor event loop: park until `wake_after`
/// elapses, re-using a `Sleep` hoisted out of the caller's loop.
///
/// ZERO means the FSMs have synchronous work queued and want to be stepped
/// again immediately, so return without touching a timer: `deadline_to_tick`
/// rounds deadlines up to the end of the current millisecond, which would cap
/// input-free FSM sequences (rotate, drain, persist) near 1000 steps/second.
///
/// Non-zero durations must not be collapsed, even far below that 1ms tick.
/// They're deadline-driven — `leader::close_policy` reports time remaining until
/// its nearest threshold, shrinking to microseconds as it nears — so returning
/// early would spin re-evaluating a deadline that hasn't passed.
///
/// `reset` to a *later* deadline lands on `extend_expiration`'s lock-free CAS,
/// so only a loop's first non-zero iteration takes the global timer-wheel mutex.
pub(crate) async fn sleep_unless_zero(
    mut sleep: std::pin::Pin<&mut tokio::time::Sleep>,
    wake_after: std::time::Duration,
) {
    if wake_after.is_zero() {
        return;
    }
    sleep
        .as_mut()
        .reset(tokio::time::Instant::now() + wake_after);
    sleep.await
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

// Status bounding lives in `proto-grpc`, which every crate speaking this
// protocol already depends on. See `proto_grpc::MAX_STATUS_MESSAGE_LEN` for
// why an unbounded status can't survive its trip over the wire.
pub(crate) use proto_grpc::bounded_unknown_status;
pub use proto_grpc::{
    CHANNEL_BUFFER, MAX_MESSAGE_SIZE, MAX_STATUS_MESSAGE_LEN, Verify, anyhow_to_status,
    status_to_anyhow, verify,
};

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

    /// Truncate `binding`'s backfill boundary: pre-boundary documents the
    /// combiner holds become stale (existence-only) and already-spilled segments
    /// are fenced. See [`doc::combine::Accumulator::truncate`].
    pub fn truncate(&mut self, binding: usize) {
        self.0.truncate(binding)
    }

    /// Drain the combiner. Stale entries — flagged or fenced by a backfill
    /// truncation — are discarded, transferring only their existence onto the
    /// fresh entry of a shared (binding, key).
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
