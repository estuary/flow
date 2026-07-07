mod actor;
pub(crate) mod connector;
mod drain;
mod handler;
mod scan;
mod startup;
mod task;

pub(crate) use handler::serve;
use task::Task;

#[derive(Clone)]
pub(crate) struct Metrics {
    /// RocksDB persists committed by this session (shard zero only).
    persists: metrics::Counter,
    /// Connector C:Published responses received.
    published_docs: metrics::Counter,
    /// Total bytes of C:Published document JSON received.
    published_bytes: metrics::Counter,
    /// Frontier scans completed (one per leader L:Load).
    scans_completed: metrics::Counter,
    /// Output combiner drains completed (one per leader L:Store).
    drains_completed: metrics::Counter,
    /// Per-session inferred schema updates logged by drain.
    inferred_schema_updates: metrics::Counter,
}

impl Metrics {
    pub(crate) fn new(shard_id: &str) -> Self {
        static DESCRIBE: std::sync::Once = std::sync::Once::new();
        DESCRIBE.call_once(|| {
            metrics::describe_counter!(
                "runtime_shard_derive_persists",
                metrics::Unit::Count,
                "RocksDB persists committed by this session",
            );
            metrics::describe_counter!(
                "runtime_shard_derive_published_docs",
                metrics::Unit::Count,
                "connector C:Published responses received",
            );
            metrics::describe_counter!(
                "runtime_shard_derive_published_bytes",
                metrics::Unit::Bytes,
                "total bytes of C:Published document JSON received",
            );
            metrics::describe_counter!(
                "runtime_shard_derive_scans_completed",
                metrics::Unit::Count,
                "frontier scans completed (one per leader L:Load)",
            );
            metrics::describe_counter!(
                "runtime_shard_derive_drains_completed",
                metrics::Unit::Count,
                "output combiner drains completed (one per leader L:Store)",
            );
            metrics::describe_counter!(
                "runtime_shard_derive_inferred_schema_updates",
                metrics::Unit::Count,
                "inferred schema updates logged by drain",
            );
        });

        let shard_id = || shard_id.to_string();
        Self {
            persists: metrics::counter!(
                "runtime_shard_derive_persists",
                "shard_id" => shard_id(),
            ),
            published_docs: metrics::counter!(
                "runtime_shard_derive_published_docs",
                "shard_id" => shard_id(),
            ),
            published_bytes: metrics::counter!(
                "runtime_shard_derive_published_bytes",
                "shard_id" => shard_id(),
            ),
            scans_completed: metrics::counter!(
                "runtime_shard_derive_scans_completed",
                "shard_id" => shard_id(),
            ),
            drains_completed: metrics::counter!(
                "runtime_shard_derive_drains_completed",
                "shard_id" => shard_id(),
            ),
            inferred_schema_updates: metrics::counter!(
                "runtime_shard_derive_inferred_schema_updates",
                "shard_id" => shard_id(),
            ),
        }
    }
}
