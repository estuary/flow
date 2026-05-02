use bytes::Bytes;
use std::collections::BTreeMap;

/// Per-binding stats summary returned after a single shuffle Frontier scan.
#[derive(Debug, Default)]
pub struct ScanComplete {
    pub binding_read: BTreeMap<u32, ops::proto::stats::DocsAndBytes>,
    pub max_key_deltas: BTreeMap<u32, Bytes>,
    pub combiner_usage_bytes: u64,
    pub first_source_clock: BTreeMap<u32, u64>,
    pub last_source_clock: BTreeMap<u32, u64>,
}

/// Per-binding stored docs/bytes accumulated across one combiner drain.
#[derive(Debug, Default)]
pub struct DrainStoresComplete {
    pub binding_stored: BTreeMap<u32, ops::proto::stats::DocsAndBytes>,
}

/// Per-transaction deltas reported to the leader at phase boundaries.
#[derive(Debug, Default)]
pub struct Deltas {
    pub binding_read: BTreeMap<u32, ops::proto::stats::DocsAndBytes>,
    pub binding_loaded: BTreeMap<u32, ops::proto::stats::DocsAndBytes>,
    pub max_key_deltas: BTreeMap<u32, Bytes>,
    pub combiner_usage_bytes: u64,
    pub first_source_clock: BTreeMap<u32, u64>,
    pub last_source_clock: BTreeMap<u32, u64>,
}
