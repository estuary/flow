mod actor;
mod boundaries;
mod connector;
mod drain;
mod handler;
mod scan;
mod startup;
mod task;

pub(crate) use handler::serve;
use task::{Binding, Task};

#[derive(Clone)]
pub(crate) struct Metrics {
    /// RocksDB persists committed by this session.
    persists: metrics::Counter,
    /// Connector C:Loaded responses received.
    loaded_docs: metrics::Counter,
    /// Total bytes of C:Loaded document JSON received.
    loaded_bytes: metrics::Counter,
    /// Frontier scans completed (one per leader L:Load).
    scans_completed: metrics::Counter,
    /// Memtable drains completed (one per leader L:Store).
    drains_completed: metrics::Counter,
}

impl Metrics {
    pub(crate) fn new(shard_id: &str) -> Self {
        static DESCRIBE: std::sync::Once = std::sync::Once::new();
        DESCRIBE.call_once(|| {
            metrics::describe_counter!(
                "runtime_shard_materialize_persists",
                metrics::Unit::Count,
                "RocksDB persists committed by this session",
            );
            metrics::describe_counter!(
                "runtime_shard_materialize_loaded_docs",
                metrics::Unit::Count,
                "connector C:Loaded responses received",
            );
            metrics::describe_counter!(
                "runtime_shard_materialize_loaded_bytes",
                metrics::Unit::Bytes,
                "total bytes of C:Loaded document JSON received",
            );
            metrics::describe_counter!(
                "runtime_shard_materialize_scans_completed",
                metrics::Unit::Count,
                "frontier scans completed (one per leader L:Load)",
            );
            metrics::describe_counter!(
                "runtime_shard_materialize_drains_completed",
                metrics::Unit::Count,
                "memtable drains completed (one per leader L:Store)",
            );
        });

        let shard_id = || shard_id.to_string();
        Self {
            persists: metrics::counter!(
                "runtime_shard_materialize_persists",
                "shard_id" => shard_id(),
            ),
            loaded_docs: metrics::counter!(
                "runtime_shard_materialize_loaded_docs",
                "shard_id" => shard_id(),
            ),
            loaded_bytes: metrics::counter!(
                "runtime_shard_materialize_loaded_bytes",
                "shard_id" => shard_id(),
            ),
            scans_completed: metrics::counter!(
                "runtime_shard_materialize_scans_completed",
                "shard_id" => shard_id(),
            ),
            drains_completed: metrics::counter!(
                "runtime_shard_materialize_drains_completed",
                "shard_id" => shard_id(),
            ),
        }
    }
}

// Set of observed keys, used to de-duplicate sent C:Load requests.
type LoadKeys = std::collections::HashSet<u128, std::hash::BuildHasherDefault<IdentHasher>>;

/// Identity hasher over the 16-byte xxh3_128 key digests we insert.
/// We trust the digest to be well-distributed and avoid re-hashing it.
#[derive(Default)]
struct IdentHasher(u64);

impl std::hash::Hasher for IdentHasher {
    fn write(&mut self, bytes: &[u8]) {
        self.0 = u64::from_ne_bytes(bytes[..8].try_into().unwrap());
    }

    fn finish(&self) -> u64 {
        self.0
    }
}
