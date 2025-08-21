use ::ops::stats::DocsAndBytes;
use futures::Stream;
use proto_flow::capture::{Request, Response};
use std::collections::{BTreeMap, BTreeSet};

mod connector;
mod protocol;
mod serve;
mod task;

pub trait RequestStream: Stream<Item = anyhow::Result<Request>> + Send + Unpin + 'static {}
impl<T: Stream<Item = anyhow::Result<Request>> + Send + Unpin + 'static> RequestStream for T {}

pub trait ResponseStream: Stream<Item = anyhow::Result<Response>> + Send + 'static {}
impl<T: Stream<Item = anyhow::Result<Response>> + Send + 'static> ResponseStream for T {}

#[derive(Debug, Clone)]
pub struct Task {
    // Bindings of this task.
    bindings: Vec<Binding>,
    // Does the capture connector want explicit acknowledgements?
    explicit_acknowledgements: bool,
    // Instant at which this Task is eligible for restart.
    restart: tokio::time::Instant,
    // ShardRef of this task.
    shard_ref: ops::ShardRef,
}

#[derive(Debug, Clone)]
struct Binding {
    // Target collection.
    collection_name: String,
    // Generation id of the collection, which must be output as part of updating inferred schemas.
    collection_generation_id: models::Id,
    // JSON pointer at which document UUIDs are added.
    document_uuid_ptr: doc::Pointer,
    // Key components which are extracted from written documents.
    key_extractors: Vec<doc::Extractor>,
    // Partition values which are extracted from written documents.
    partition_extractors: Vec<doc::Extractor>,
    // Partition template name for journals of the target collection.
    partition_template_name: String,
    // Serialization policy for the Target collection.
    ser_policy: doc::SerPolicy,
    // Encoded resource path + backfill state key of this binding.
    state_key: String,
    // Write schema of the target collection.
    write_schema_json: bytes::Bytes,
    // Inferred Shape of written documents.
    write_shape: doc::Shape,
}

#[derive(Debug)]
pub struct Transaction {
    // Number of captured document bytes rolled up in this transaction.
    captured_bytes: usize,
    // Number of connector checkpoints rolled up in this transaction.
    checkpoints: u32,
    // The connector instance exited at the completion of this transaction.
    connector_eof: bool,
    // Time of first connector Captured or Checkpoint response.
    started_at: std::time::SystemTime,
    // Statistics of (read documents, combined documents) for each binding.
    stats: BTreeMap<u32, (DocsAndBytes, DocsAndBytes)>,
    // Sourced schema updates of this transaction.
    sourced_schemas: BTreeMap<usize, doc::Shape>,
    // Set of bindings which updated their inferred Shape this transaction.
    updated_inferences: BTreeSet<usize>,
}

// LONG_POLL_TIMEOUT is the amount of time we'll long-poll for a ready transaction.
// If no checkpoints arrive within this timeout then we return control to the client,
// who must must long-poll again to read a captured transaction.
//
// LONG_POLL_TIMEOUT bounds live-ness when gracefully stopping or when restarting
// a capture session due to a new request::Open, as re-opening a session can only
// happen in between long-polls.
const LONG_POLL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

// COMBINER_BYTE_THRESHOLD is a coarse target on the documents which can be
// optimistically combined within a capture transaction, while awaiting the
// commit of a previous transaction. Upon reaching this threshold, further
// documents and checkpoints will not be folded into the transaction.
const COMBINER_BYTE_THRESHOLD: usize = 1 << 25; // 32MB.

impl Transaction {
    pub fn new() -> Self {
        Self {
            captured_bytes: 0,
            checkpoints: 0,
            connector_eof: false,
            started_at: std::time::SystemTime::UNIX_EPOCH,
            stats: Default::default(),
            sourced_schemas: Default::default(),
            updated_inferences: Default::default(),
        }
    }
}
