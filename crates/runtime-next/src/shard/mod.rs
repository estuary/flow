pub mod capture;
pub mod derive;
pub mod materialize;
pub(crate) mod recovery;
mod rocksdb;
mod service;

use rocksdb::RocksDB;
pub use service::Service;

/// Feed one transaction's per-journal append-throttle samples into the shard's
/// long-lived [`SplitPolicy`]. Called once per transaction at the commit/drain
/// boundary
pub(crate) fn observe_throttle_samples<'a>(
    policy: &mut crate::split_policy::SplitPolicy,
    samples: impl IntoIterator<Item = publisher::ThrottleSample<'a>>,
    now: std::time::Instant,
) {
    for sample in samples {
        policy.observe(sample.journal_name, sample.throttled, now);

        if policy.should_split(sample.journal_name, now) {
            // TODO actually do the split
        }
    }
}

/// Build gRPC client metadata bearing a self-signed `LEAD` token when `signer`
/// is `Some`, scoped to `shard_id`'s task prefix so a leader stream opened with
/// it can only operate on shards of this task. Empty metadata when `None`
/// (unauthenticated local contexts, e.g. `flowctl preview`).
pub(crate) fn leader_bearer(
    signer: Option<&proto_grpc::Signer>,
    shard_id: &str,
) -> tonic::Result<proto_grpc::Metadata> {
    match signer {
        Some(signer) => signer.shard_bearer(proto_flow::capability::LEAD, shard_id),
        None => Ok(proto_grpc::Metadata::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::observe_throttle_samples;
    use crate::split_policy::SplitPolicy;
    use std::time::{Duration, Instant};

    fn sample(journal_name: &str, throttled: bool) -> publisher::ThrottleSample<'_> {
        publisher::ThrottleSample {
            journal_name,
            throttled,
        }
    }

    /// The actor glue feeds throttle samples into the policy: a journal throttled
    /// every transaction becomes due for a split, while a clean one never does.
    #[test]
    fn observe_advances_only_throttled_journals() {
        let mut policy = SplitPolicy::new();
        let base = Instant::now();

        // Baseline sample, then one throttled and one clean sample every 10s for
        // 300s — past the cold-start span and the ~63s the EWMA needs to cross.
        observe_throttle_samples(
            &mut policy,
            [sample("hot", true), sample("cold", false)],
            base,
        );
        for i in 1..=30 {
            let now = base + Duration::from_secs(10 * i);
            observe_throttle_samples(
                &mut policy,
                [sample("hot", true), sample("cold", false)],
                now,
            );
        }

        let now = base + Duration::from_secs(310);
        assert!(
            policy.should_split("hot", now),
            "constantly-throttled journal should become due for a split",
        );
        assert!(
            !policy.should_split("cold", now),
            "never-throttled journal must not become due",
        );
    }
}
