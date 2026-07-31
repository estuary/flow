//! `runtime-local` runs `runtime-next` tasks locally with synthetic shards, and
//! with no Gazette broker, etcd, or Go consumer.
//!
//! It hosts an in-process tonic server ([`services::Run`]) carrying
//! `runtime_next::Service` and, optionally, a caller-supplied
//! `shuffle::Service`; synthesizes the SessionLoop / Join / Task envelopes the
//! Go controller would normally send; and drives N synthetic shards as tokio
//! tasks over long-lived SessionLoop streams.
//!
//! The crate is generic over the three `runtime-next` host seams —
//! [`runtime_next::PublisherFactory`], [`runtime_next::LoggerFactory`], and
//! [`runtime_next::ShuffleSessionFactory`] — so callers decide where source
//! documents come from and where output documents, stats, and logs go. Two
//! consumers exist: `flowctl raw preview-next` (stdout publisher, live-journal
//! or fixture shuffle) and `catalog-tests` (in-memory collection store,
//! channel-fed shuffle).
//!
//! Nothing here knows about catalog tests. That layering is deliberate and is
//! enforced by the crate boundary: `catalog-tests` depends on `runtime-local`,
//! never the reverse.
//!
//! # Key entry points
//!
//! - [`services::Run`] — per-invocation resources: the tonic server, the
//!   shard-zero RocksDB directory, and the shuffle-log directory.
//! - [`driver::run_sessions`] / [`derive_driver::run_sessions`] /
//!   [`capture_driver::run_sessions`] — drive N shards of one task through a
//!   sequence of sessions.
//! - [`segments::write_transaction`] — write documents directly as
//!   `shuffle::log` segments and return the checkpoint frontier that makes them
//!   visible, bypassing journals entirely.
//! - [`segments::fixture_opener`] — the channel-fed `ShuffleSessionFactory`
//!   which relays those frontiers to the leader.

pub mod capture_driver;
pub mod derive_driver;
pub mod driver;
pub mod segments;
pub mod services;
pub mod shards;

use runtime_next::{LoggerFactory, PublisherFactory};

/// Controls threaded into each driver: an optional connector-state seed for
/// shard zero, plus the publisher and logger factories installed on each shard
/// `Service`. Cheap to clone — both factories are `Clone` by their trait bound.
#[derive(Clone)]
pub struct Controls<P: PublisherFactory, L: LoggerFactory> {
    /// Connector state to seed into shard zero's RocksDB before the runtime
    /// opens it. Empty to leave the runtime's own `{}` seed in place.
    pub initial_state_json: bytes::Bytes,
    pub publisher_factory: P,
    pub logger_factory: L,
}

/// Seed shard zero's RocksDB at `descriptor` with `initial_state_json` as the
/// connector-state base, then close it. Call before the runtime opens the same
/// path via its SessionLoop, so it recovers the seeded state on its first scan
/// exactly as if a prior connector session had persisted it. Production has no
/// equivalent: the runtime seeds `{}` itself.
pub async fn seed_connector_state(
    descriptor: proto_flow::runtime::RocksDbDescriptor,
    initial_state_json: &[u8],
) -> anyhow::Result<()> {
    let db = runtime_next::shard::rocksdb::RocksDB::open(Some(descriptor)).await?;
    _ = db.put_connector_state_base(initial_state_json).await?;
    Ok(())
}

/// Re-open shard zero's RocksDB at `descriptor` and return its reduced connector
/// state — the exact `Recover.connector_state_json` the runtime itself would
/// recover (empty if none was ever persisted). Reuses the recovery `scan`, so it
/// stays consistent with how the runtime reads state.
///
/// Safe to open directly once a run's session loop has returned: the runtime's
/// shard serve loop drops its `RocksDB` handle (releasing the exclusive lock)
/// when its request stream ends, which is strictly before its response stream
/// reaches EOF — and a session loop returns only after draining that EOF.
pub async fn read_connector_state(
    descriptor: proto_flow::runtime::RocksDbDescriptor,
) -> anyhow::Result<bytes::Bytes> {
    let db = runtime_next::shard::rocksdb::RocksDB::open(Some(descriptor)).await?;
    let (_db, recover) = db.scan(std::iter::empty::<&str>()).await?;
    Ok(recover.connector_state_json)
}

/// Raise a task's minimum transaction duration, so the leader holds each
/// transaction open for at least `delay` and batches source output into fewer,
/// larger transactions. The runtime-next analog of legacy preview's sleep
/// between transaction polls.
pub fn set_min_txn_duration(
    shard_template: Option<&mut proto_gazette::consumer::ShardSpec>,
    delay: std::time::Duration,
) {
    let Some(shard_template) = shard_template else {
        return;
    };
    let min = pbjson_types::Duration {
        seconds: delay.as_secs() as i64,
        nanos: delay.subsec_nanos() as i32,
    };
    // Keep the close-policy band well-formed if the template's configured
    // maximum is below the requested minimum.
    if shard_template
        .max_txn_duration
        .as_ref()
        .map_or(true, |max| {
            (max.seconds, max.nanos) < (min.seconds, min.nanos)
        })
    {
        shard_template.max_txn_duration = Some(min.clone());
    }
    shard_template.min_txn_duration = Some(min);
}

/// Force one-transaction-per-checkpoint in the leader by collapsing the task's
/// transaction-duration window, so each fed transaction commits as exactly one
/// runtime transaction.
///
/// A literal `max_txn_duration` of zero would deadlock the leader: `HeadIdle`
/// gates the first checkpoint load on `open_age < max_txn_duration`, and a fresh
/// transaction's `open_age` is zero. The smallest positive duration loads one
/// checkpoint, after which the Load round's IO advances the clock past the bound
/// and the transaction closes.
pub fn force_single_transaction(shard_template: Option<&mut proto_gazette::consumer::ShardSpec>) {
    if let Some(shard_template) = shard_template {
        shard_template.min_txn_duration = Some(pbjson_types::Duration {
            seconds: 0,
            nanos: 0,
        });
        shard_template.max_txn_duration = Some(pbjson_types::Duration {
            seconds: 0,
            nanos: 1,
        });
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_set_min_txn_duration() {
        let dur = |seconds, nanos| pbjson_types::Duration { seconds, nanos };

        // An unset maximum is raised alongside the minimum.
        let mut template = proto_gazette::consumer::ShardSpec::default();
        set_min_txn_duration(Some(&mut template), std::time::Duration::from_secs(10));
        assert_eq!(template.min_txn_duration, Some(dur(10, 0)));
        assert_eq!(template.max_txn_duration, Some(dur(10, 0)));

        // A maximum above the delay is left alone.
        template.max_txn_duration = Some(dur(30, 0));
        set_min_txn_duration(Some(&mut template), std::time::Duration::from_secs(10));
        assert_eq!(template.min_txn_duration, Some(dur(10, 0)));
        assert_eq!(template.max_txn_duration, Some(dur(30, 0)));

        // A maximum below the delay is raised to keep the band well-formed.
        template.max_txn_duration = Some(dur(5, 0));
        set_min_txn_duration(Some(&mut template), std::time::Duration::from_secs(10));
        assert_eq!(template.min_txn_duration, Some(dur(10, 0)));
        assert_eq!(template.max_txn_duration, Some(dur(10, 0)));
    }
}
