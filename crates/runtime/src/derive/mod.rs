use ::ops::stats::DocsAndBytes;
use futures::Stream;
use proto_flow::derive::{Request, Response};
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
    // Target collection.
    collection_name: String,
    // JSON pointer at which document UUIDs are added.
    document_uuid_ptr: doc::Pointer,
    // Key components which are extracted from written documents.
    key_extractors: Vec<doc::Extractor>,
    // Partition values which are extracted from written documents.
    partition_extractors: Vec<doc::Extractor>,
    // Serialization policy for the Target collection.
    ser_policy: doc::SerPolicy,
    // ShardRef of this task.
    shard_ref: ops::ShardRef,
    // Transforms of this task.
    transforms: Vec<Transform>,
    // Write JSON-Schema of the derivation collection.
    write_schema_json: String,
}

#[derive(Debug)]
struct Transform {
    collection_name: String,  // Source collection.
    name: String,             // Name of this Transform.
    read_schema_json: String, // Read JSON-Schema of the derivation source collection.
}

#[derive(Debug)]
pub struct Transaction {
    checkpoint: consumer::Checkpoint,        // Recorded checkpoint.
    combined_stats: DocsAndBytes,            // Combined output stats.
    max_clock: u64,                          // Maximum clock of read documents.
    publish_stats: DocsAndBytes,             // Published (right) stats.
    read_stats: BTreeMap<u32, DocsAndBytes>, // Per-transform read document stats.
    started_at: std::time::SystemTime,       // Time of first Read request.
    updated_inference: bool,                 // Did we update our inferred Shape this transaction?
}

impl Transaction {
    pub fn new() -> Self {
        Self {
            checkpoint: Default::default(),
            combined_stats: Default::default(),
            max_clock: 0,
            publish_stats: Default::default(),
            read_stats: BTreeMap::new(),
            started_at: std::time::SystemTime::UNIX_EPOCH,
            updated_inference: false,
        }
    }
}
