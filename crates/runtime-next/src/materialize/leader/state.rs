#[derive(Debug)]
pub struct Task {
    pub n_shards: usize,
    pub skip_replay_determinism: bool,

    /// Collection name per binding index, used as the `ops.Stats.materialize`
    /// map key when the Actor enqueues stats docs.
    pub collection_names: Vec<String>,

    /// ShardRef embedded in every stats document. Derived from shard zero's
    /// identity at session start. For materialize the leader is the sole
    /// stats publisher, so all docs carry shard zero's ShardRef.
    pub shard_ref: ops::ShardRef,

    /// OCI image of the connector. Reported by shard zero in L:Opened.
    /// Empty for local/dekaf connectors. Embedded in trigger variables.
    pub connector_image: String,

    /// Compiled trigger templates, decrypted from `spec.materialization.triggers_json`.
    /// `None` when the spec has no triggers configured.
    pub compiled_triggers: Option<std::sync::Arc<super::triggers::CompiledTriggers>>,

    // Close-policy thresholds, each expressed as min..max.
    // - A transaction may close once `min` is met on every axis.
    // - A transaction may be extended if below `max` on every axis.
    pub open_duration: std::ops::Range<std::time::Duration>,
    pub last_commit_age: std::ops::Range<std::time::Duration>,
    pub combiner_usage_bytes: std::ops::Range<u64>,
    pub read_docs: std::ops::Range<u64>,
    pub read_bytes: std::ops::Range<u64>,
}
