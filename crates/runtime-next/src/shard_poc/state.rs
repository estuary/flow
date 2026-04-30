//! Per-session bookkeeping state for the materialize shard reactor.
//!
//! These are pure POD types: the actor consumes leader/connector messages
//! and updates fields directly. There is no FSM weight here — the leader
//! makes the pipelining/policy decisions, and the shard simply reacts.

use bytes::Bytes;
use std::collections::BTreeMap;

/// Per-binding stats summary returned to the actor after a single
/// `FrontierScan` completes. Folded into the actor's `LoadDeltas`.
#[derive(Debug, Default)]
pub struct ScanComplete {
    /// Source DocsAndBytes per binding observed during the scan.
    pub binding_read: BTreeMap<u32, ops::proto::stats::DocsAndBytes>,
    /// New high-water packed-key per binding index, advanced during this scan.
    pub max_key_deltas: BTreeMap<u32, Bytes>,
    /// Combiner on-disk bytes after the scan.
    pub combiner_usage_bytes: u64,
    /// Per-binding min source-document `Clock` observed during this scan.
    /// Only bindings that received documents are present.
    pub first_source_clock: BTreeMap<u32, u64>,
    /// Per-binding max source-document `Clock` observed during this scan.
    /// Only bindings that received documents are present.
    pub last_source_clock: BTreeMap<u32, u64>,
}

/// Per-binding stored docs/bytes accumulated across the combiner drain.
/// Folded into the actor's `LoadDeltas.binding_stored` once the drain
/// completes; reported in L:StartedCommit (the leader sums across shards
/// for the stats document's per-binding `out` slot).
#[derive(Debug, Default)]
pub struct DrainStoresComplete {
    pub binding_stored: BTreeMap<u32, ops::proto::stats::DocsAndBytes>,
}

/// Per-transaction stats accumulator. Per-round counters
/// (`binding_read`, `binding_loaded`, `max_key_deltas`,
/// `combiner_usage_bytes`) are drained into each L:Loaded and into
/// L:Flushed; cross-transaction counters (`binding_stored`,
/// `first_source_clock`, `last_source_clock`) accumulate across the
/// whole transaction and drain into L:StartedCommit.
#[derive(Debug, Default, Clone)]
pub struct LoadDeltas {
    pub binding_read: BTreeMap<u32, ops::proto::stats::DocsAndBytes>,
    pub binding_loaded: BTreeMap<u32, ops::proto::stats::DocsAndBytes>,
    pub binding_stored: BTreeMap<u32, ops::proto::stats::DocsAndBytes>,
    pub max_key_deltas: BTreeMap<u32, Bytes>,
    pub combiner_usage_bytes: u64,
    pub first_source_clock: BTreeMap<u32, u64>,
    pub last_source_clock: BTreeMap<u32, u64>,
}
