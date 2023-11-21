use ::ops::stats::DocsAndBytes;
use futures::Stream;
use proto_flow::materialize::{Request, Response};
use proto_gazette::consumer;
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
    key_extractors: Vec<doc::Extractor>,   // Key extractors for this collection.
    read_schema_json: String,              // Read JSON-Schema of collection documents.
    ser_policy: doc::SerPolicy,            // Serialization policy for this source.
    store_document: bool, // Are we storing the root document (often `flow_document`)?
    value_extractors: Vec<doc::Extractor>, // Field extractors for this collection.
}

#[derive(Debug)]
pub struct Transaction {
    checkpoint: consumer::Checkpoint, // Recorded checkpoint.
    stats: BTreeMap<u32, (DocsAndBytes, DocsAndBytes, DocsAndBytes)>, // Per-binding stats.
    started_at: std::time::SystemTime, // Time of first Read request.
}

impl Transaction {
    pub fn new() -> Self {
        Self {
            checkpoint: Default::default(),
            stats: Default::default(),
            started_at: std::time::SystemTime::UNIX_EPOCH,
        }
    }
}
