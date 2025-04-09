use ::ops::stats::DocsAndBytes;
use futures::Stream;
use proto_flow::materialize::{Request, Response};
use proto_gazette::{consumer, uuid::Clock};
use std::collections::BTreeMap;

mod connector;
mod protocol;
mod serve;
mod task;

pub trait RequestStream: Stream<Item = anyhow::Result<Request>> + Send + Unpin + 'static {}
impl<T: Stream<Item = anyhow::Result<Request>> + Send + Unpin + 'static> RequestStream for T {}

pub trait ResponseStream: Stream<Item = anyhow::Result<Response>> + Send + 'static {}
impl<T: Stream<Item = anyhow::Result<Response>> + Send + 'static> ResponseStream for T {}

#[derive(Debug)]
pub struct Task {
    // Bindings of this materialization.
    bindings: Vec<Binding>,
    // ShardRef of this task.
    shard_ref: ops::ShardRef,
}

#[derive(Debug)]
struct Binding {
    collection_name: String,               // Source collection.
    delta_updates: bool,                   // Delta updates, or standard?
    journal_read_suffix: String, // Suffix attached to journal checkpoints for this binding.
    key_extractors: Vec<doc::Extractor>, // Key extractors for this collection.
    read_schema_json: String,    // Read JSON-Schema of collection documents.
    ser_policy: doc::SerPolicy,  // Serialization policy for this source.
    state_key: String,           // State key for this binding.
    store_document: bool,        // Are we storing the root document (often `flow_document`)?
    value_extractors: Vec<doc::Extractor>, // Field extractors for this collection.
    /// Pointer to extract the document UUID.
    uuid_ptr: doc::Pointer,
}

#[derive(Debug)]
pub struct Transaction {
    checkpoint: consumer::Checkpoint, // Recorded checkpoint.
    stats: BTreeMap<u32, (DocsAndBytes, DocsAndBytes, DocsAndBytes, Clock)>, // Per-binding (left, right, out, last-source-clock) stats.
    started: bool,                     // Has the transaction been started?
    started_at: std::time::SystemTime, // Time of first Read request.
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

// LoadKeySet is a HashSet of xx3_128 key hashes, which are effectively unique.
// Its keys are already hashed so it uses a pass-through IdentHasher.
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
