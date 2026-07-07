//! The `drive` layer: run runtime-next tasks locally with no Gazette broker,
//! etcd, or Go consumer. Extracted from `flowctl::preview` and shared with it.
//!
//! It hosts an in-process tonic server (`runtime_next::Service` and, optionally,
//! a `shuffle::Service`), synthesizes the SessionLoop / Join / Task envelopes a
//! controller would send, and drives N synthetic shards as tokio tasks. It is
//! generic over the three runtime-next host seams — [`PublisherFactory`],
//! [`LoggerFactory`], and [`ShuffleSessionFactory`](runtime_next::ShuffleSessionFactory)
//! — so both preview (stdout publisher + live/fixture shuffle) and the catalog-
//! test runner ([`TestPublisher`](crate::publish::TestPublisher) + a channel-fed
//! test shuffle) reuse the same drive loops.

pub mod capture_driver;
pub mod derive_driver;
pub mod driver;
pub mod segments;
pub mod services;
pub mod shards;

use runtime_next::{LoggerFactory, PublisherFactory};

/// Controls threaded into each driver: the `--initial-state` seed (used to
/// pre-seed shard zero's RocksDB), the publisher factory (installed on the shard
/// and, for materializations / derivations, the leader `Service`), and the
/// logger factory. Cheap to clone — the factories are `Clone` by their trait
/// bound.
#[derive(Clone)]
pub struct Controls<P: PublisherFactory, L: LoggerFactory> {
    pub initial_state_json: bytes::Bytes,
    pub publisher_factory: P,
    pub logger_factory: L,
}

/// Seed shard zero's RocksDB at `descriptor` with `initial_state_json` as the
/// connector-state base, then close it. Called before the runtime opens the same
/// path via its SessionLoop, so the runtime recovers the seeded state on its
/// first scan exactly as if a prior connector session had persisted it.
/// Production has no equivalent: the runtime seeds `{}` itself.
pub async fn seed_rocksdb_state(
    descriptor: proto_flow::runtime::RocksDbDescriptor,
    initial_state_json: &[u8],
) -> anyhow::Result<()> {
    let db = runtime_next::shard::rocksdb::RocksDB::open(Some(descriptor)).await?;
    _ = db.put_connector_state_base(initial_state_json).await?;
    Ok(())
}
