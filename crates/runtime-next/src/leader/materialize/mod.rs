mod actor;
mod frontier_mapping;
mod fsm;
mod handler;
mod startup;
mod task;
mod triggers;

pub(crate) use handler::serve;

// Task configuration, as understood by the leader.
//
// Several fields express "close" vs "extend" policy thresholds:
// - A transaction may close once `min` is met on every dimension.
// - A transaction may be extended if below `max` on every dimension.
struct Task {
    // Collection name of each materialization binding.
    binding_collection_names: Vec<String>,
    // Journal read suffix of each materialization binding.
    binding_journal_read_suffixes: Vec<String>,
    // Min/max desired combiner disk byte utilization.
    combiner_usage_bytes: std::ops::Range<u64>,
    // OCI image of the connector, or empty if not an image connector.
    connector_image: String,
    // Min/max desired age of the last transaction (elapsed since last txn close).
    last_close_age: std::ops::Range<std::time::Duration>,
    // Number of shards lead by this leader.
    n_shards: usize,
    // Min/max desired duration of an open transaction (elapsed since first ready checkpoint).
    open_duration: std::ops::Range<std::time::Duration>,
    // Descriptive peer names of each shard, for logging and errors.
    peers: Vec<String>,
    // Min/max desired bytes read in a transaction.
    read_bytes: std::ops::Range<u64>,
    // Min/max desired documents read in a transaction.
    read_docs: std::ops::Range<u64>,
    // ShardRef embedded in every stats document.
    shard_ref: ops::ShardRef,
    // Compiled triggers, or None if the task has no triggers configured.
    triggers: Option<std::sync::Arc<triggers::CompiledTriggers>>,
}
