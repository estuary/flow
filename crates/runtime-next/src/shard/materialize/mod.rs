mod actor;
mod connector;
mod handler;
mod startup;
mod state;
mod task;

pub(crate) use handler::serve;

#[derive(Debug)]
pub struct Task {
    // Bindings of this materialization.
    bindings: Vec<Binding>,
    // ShardRef of this task.
    #[allow(dead_code)] // May be used for logging, later
    shard_ref: ops::ShardRef,
}

#[derive(Debug)]
struct Binding {
    collection_name: String,               // Source collection.
    delta_updates: bool,                   // Delta updates, or standard?
    key_extractors: Vec<doc::Extractor>,   // Key extractors for this collection.
    read_schema_json: bytes::Bytes,        // Read JSON-Schema of collection documents.
    ser_policy: doc::SerPolicy,            // Serialization policy for this source.
    state_key: String,                     // State key for this binding.
    store_document: bool, // Are we storing the root document (often `flow_document`)?
    value_extractors: Vec<doc::Extractor>, // Field extractors for this collection.
}

