//! Shard-side materialize state types and module skeleton.
//!
//! POD types (`Task`, `Binding`, `BindingStats`, `Transaction`) mirror
//! `runtime`'s `materialize::mod` field-for-field so that `task.rs` can
//! be `#[path]`-shared without divergence. Trigger logic lives in the
//! sibling `materialize::leader` module (the leader sidecar fires
//! triggers); the per-shard loop here contributes only per-shard
//! source-clock min/max via `L:StartedCommit`.

use ::ops::stats::DocsAndBytes;
use proto_gazette::{consumer, uuid::Clock};
use std::collections::BTreeMap;

pub mod actor;
pub mod handler;
pub mod recovery;
pub mod startup;
pub mod state;

mod connector;
mod task;

#[allow(dead_code)] // Bindings populated and consumed by upcoming phases.
#[derive(Debug)]
pub struct Task {
    // Bindings of this materialization.
    bindings: Vec<Binding>,
    // ShardRef of this task.
    shard_ref: ops::ShardRef,
}

#[allow(dead_code)] // Fields consumed by Phase D (FSM) and Phase E (actor).
#[derive(Debug)]
struct Binding {
    collection_name: String,               // Source collection.
    delta_updates: bool,                   // Delta updates, or standard?
    journal_read_suffix: String, // Suffix attached to journal checkpoints for this binding.
    key_extractors: Vec<doc::Extractor>, // Key extractors for this collection.
    read_schema_json: bytes::Bytes, // Read JSON-Schema of collection documents.
    ser_policy: doc::SerPolicy,  // Serialization policy for this source.
    state_key: String,           // State key for this binding.
    store_document: bool,        // Are we storing the root document (often `flow_document`)?
    value_extractors: Vec<doc::Extractor>, // Field extractors for this collection.
    /// Pointer to extract the document UUID.
    uuid_ptr: json::Pointer,
}

#[allow(dead_code)] // Populated by Phase D (FSM); reduced into L:StartedCommit clocks.
#[derive(Debug, Default)]
pub struct BindingStats {
    left: DocsAndBytes,
    right: DocsAndBytes,
    out: DocsAndBytes,
    first_source_clock: Clock,
    last_source_clock: Clock,
}

#[allow(dead_code)] // Fields consumed by Phase D (FSM) and Phase E (actor).
#[derive(Debug)]
pub struct Transaction {
    checkpoint: consumer::Checkpoint,   // Recorded checkpoint.
    stats: BTreeMap<u32, BindingStats>, // Per-binding stats.
    started: bool,                      // Has the transaction been started?
    started_at: std::time::SystemTime,  // Time of first Read request.
}

impl Transaction {
    pub fn new() -> Self {
        Self {
            checkpoint: Default::default(),
            stats: Default::default(),
            started: false,
            started_at: std::time::SystemTime::UNIX_EPOCH,
        }
    }
}

impl Default for Transaction {
    fn default() -> Self {
        Self::new()
    }
}

/// LoadKeySet: HashSet of xx3_128 key hashes (effectively unique). Used by
/// the Loading state of the Head FSM to decide whether to issue C:Load.
type LoadKeySet = std::collections::HashSet<u128, std::hash::BuildHasherDefault<IdentHasher>>;

#[derive(Default)]
pub struct IdentHasher(u64);

impl std::hash::Hasher for IdentHasher {
    fn write(&mut self, bytes: &[u8]) {
        self.0 = u64::from_ne_bytes(bytes[..8].try_into().unwrap());
    }
    fn finish(&self) -> u64 {
        self.0
    }
}
