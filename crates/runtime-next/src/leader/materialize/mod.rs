mod actor;
mod frontier_mapping;
mod fsm;
mod handler;
mod startup;
mod task;
mod triggers;

pub(crate) use handler::serve;

#[derive(Clone)]
pub(crate) struct Metrics {
    /// Total transactions completed by this leader session.
    transactions: metrics::Counter,
    /// Aggregate bytes-behind across all bindings, observed at each frontier.
    bytes_behind: metrics::Gauge,
}

impl Metrics {
    pub(crate) fn new(shard_zero: &str) -> Self {
        static DESCRIBE: std::sync::Once = std::sync::Once::new();
        DESCRIBE.call_once(|| {
            metrics::describe_counter!(
                "runtime_leader_transactions",
                metrics::Unit::Count,
                "transactions completed by this leader session",
            );
            metrics::describe_gauge!(
                "runtime_leader_behind",
                metrics::Unit::Bytes,
                "aggregate bytes-behind across all bindings, observed when writing stats",
            );
        });

        let shard_zero = || shard_zero.to_string();
        Self {
            transactions: metrics::counter!(
                "runtime_leader_transactions",
                "shard_zero" => shard_zero(),
            ),
            bytes_behind: metrics::gauge!(
                "runtime_leader_behind",
                "shard_zero" => shard_zero(),
            ),
        }
    }
}

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
    // Maximum completed transactions before graceful stop. Zero means unlimited.
    max_transactions: u32,
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
