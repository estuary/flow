mod connector;
mod handler;
mod startup;
mod task;

pub(crate) use handler::serve;

#[allow(dead_code)]
#[derive(Debug)]
pub struct Task {
    // Bindings of this materialization.
    bindings: Vec<Binding>,
    // ShardRef of this task.
    shard_ref: ops::ShardRef,

    binding_state_keys: Vec<String>,
}

#[allow(dead_code)]
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

fn connector_state_to_patches_json(
    state: Option<proto_flow::flow::ConnectorState>,
) -> bytes::Bytes {
    let Some(proto_flow::flow::ConnectorState {
        merge_patch,
        updated_json,
    }) = state
    else {
        return bytes::Bytes::new();
    };

    let mut b = Vec::with_capacity(updated_json.len() + 12);
    b.push(b'[');

    if !merge_patch {
        b.extend_from_slice(b"null,\n,");
    }
    b.extend_from_slice(&updated_json);
    b.extend_from_slice(b"\n]");

    b.into()
}
