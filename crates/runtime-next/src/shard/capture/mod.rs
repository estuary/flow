mod actor;
pub(crate) mod connector;
mod drain;
mod handler;

pub(crate) use handler::serve;

#[derive(Clone)]
pub(crate) struct Metrics {
    /// Transactions completed by this shard session.
    pub(super) transactions: metrics::Counter,
    /// Per-binding inferred schema updates logged by drain.
    pub(super) inferred_schema_updates: metrics::Counter,
}

impl Metrics {
    pub(crate) fn new(shard_id: &str) -> Self {
        static DESCRIBE: std::sync::Once = std::sync::Once::new();
        DESCRIBE.call_once(|| {
            metrics::describe_counter!(
                "runtime_shard_capture_transactions",
                metrics::Unit::Count,
                "transactions completed by this shard session",
            );
            metrics::describe_counter!(
                "runtime_shard_capture_inferred_schema_updates",
                metrics::Unit::Count,
                "per-binding inferred schema updates logged by drain",
            );
        });

        let shard_id = || shard_id.to_string();
        Self {
            transactions: metrics::counter!(
                "runtime_shard_capture_transactions",
                "shard_id" => shard_id(),
            ),
            inferred_schema_updates: metrics::counter!(
                "runtime_shard_capture_inferred_schema_updates",
                "shard_id" => shard_id(),
            ),
        }
    }
}
