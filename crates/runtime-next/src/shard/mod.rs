pub mod capture;
pub(crate) mod connector;
pub mod derive;
pub mod materialize;
pub(crate) mod recovery;
pub mod rocksdb;
mod service;
pub mod split_policy;

use anyhow::Context;
use rocksdb::RocksDB;
pub use service::Service;

/// Build the `Stopped` a shard sends its controller at a graceful stop.
///
/// `report_final_state` is the stream's `SessionLoop.report_final_state`: when
/// set, the shard's reduced connector state rides along. The session's final
/// Persist has necessarily landed by the time a handler reaches here (a
/// capture's actor persists before returning `db`; a derive or materialize
/// shard fails outright if the leader concludes a session with a Persist still
/// in flight), so the reported state reflects the last committed transaction.
pub(crate) async fn stopped_message(
    db: RocksDB,
    report_final_state: bool,
) -> anyhow::Result<(RocksDB, crate::proto::Stopped)> {
    if !report_final_state {
        return Ok((db, crate::proto::Stopped::default()));
    }
    let (db, connector_state_json) = db
        .get_connector_state()
        .await
        .context("reading final connector state for Stopped")?;

    Ok((
        db,
        crate::proto::Stopped {
            connector_state_json,
        },
    ))
}

/// Segments a shard lets its shuffle remainders pin, resolved once per session.
///
/// Remainders pin whole segment files the Log cannot reclaim, while unpinned
/// backlog is reclaimed as the shard consumes it, so the Log's backlog is
/// roughly the pinned bytes. Back-pressure engages at the full disk limit, so
/// half of it leaves the session a margin to stop within. Without the clamp, a
/// limit under two segment thresholds would stop on any remainder at all.
pub(crate) fn max_pinned_segments(labeling: &ops::proto::ShardLabeling) -> usize {
    let limit_bytes = match labeling.shuffle_disk_limit_bytes {
        0 => shuffle::DEFAULT_SHUFFLE_DISK_LIMIT_BYTES,
        limit => limit,
    };
    ((limit_bytes / 2 / shuffle::log::writer::DEFAULT_SEGMENT_THRESHOLD) as usize).max(1)
}

/// Stopping is safe because the next session's resume cut floors gaps from
/// dead producers.
pub(crate) fn stop_for_pinned_segments(
    stop_sent: &mut bool,
    pinned_segments: usize,
    max_pinned_segments: usize,
) -> bool {
    if *stop_sent || pinned_segments <= max_pinned_segments {
        return false;
    }
    *stop_sent = true;

    tracing::warn!(
        pinned_segments,
        max_pinned_segments,
        "stopping session to shed pinned shuffle log segments",
    );
    true
}

/// Feed one transaction's per-journal append-throttle samples into the shard's
/// long-lived [`SplitPolicy`]. Called once per transaction at the commit/drain
/// boundary.
///
/// The observe / dispatch / finish trio is actor glue consumed by the capture
/// and derive actors; it's `pub` so the `split_e2e` integration test can run
/// the same loop against a real broker.
///
/// [`SplitPolicy`]: crate::shard::split_policy::SplitPolicy
pub fn observe_throttle_samples<'a>(
    policy: &mut crate::shard::split_policy::SplitPolicy,
    samples: impl IntoIterator<Item = publisher::ThrottleSample<'a>>,
    now: std::time::Instant,
) {
    for sample in samples {
        policy.observe(sample.journal_name, sample.throttled, now);
    }
}

/// A parked automatic-split attempt: resolves to the target journal name and
/// the outcome of the split's List / Apply RPCs.
pub type SplitFuture =
    futures::future::BoxFuture<'static, (String, tonic::Result<publisher::SplitOutcome>)>;

/// Start at most one automatic split among the journals now due, returning a
/// detached future for the actor to park. Due journals which no Mapped binding
/// can split (e.g. the fixed ops-stats journal) are terminally ignored — they
/// can never become splittable.
///
/// A dispatched journal stays "due" until its outcome lands: the actor's
/// single-flight parking of the returned future is what prevents duplicate
/// dispatch, and an RPC error leaves a still-hot journal due for retry.
pub fn start_due_split<P: crate::Publisher>(
    policy: &mut crate::shard::split_policy::SplitPolicy,
    publisher: &P,
    now: std::time::Instant,
) -> Option<SplitFuture> {
    use futures::FutureExt;

    let due: Vec<String> = policy
        .due_for_split(now)
        .into_iter()
        .map(String::from)
        .collect();

    for journal in due {
        let Some(split) = publisher.split_partition(&journal) else {
            policy.ignore(&journal);
            continue;
        };
        return Some(async move { (journal, split.await) }.boxed());
    }
    None
}

/// Apply a completed split attempt's outcome to the policy. Runs on the actor
/// — the sole owner of policy state — which keeps the policy lock-free.
pub fn finish_split(
    policy: &mut crate::shard::split_policy::SplitPolicy,
    journal: &str,
    outcome: tonic::Result<publisher::SplitOutcome>,
    now: std::time::Instant,
) {
    match outcome {
        // The split applied: the journal's layout changed. Cool down and reset
        // its EWMA so pressure must re-accumulate against the narrower journal.
        Ok(publisher::SplitOutcome::Split) => {
            tracing::info!(journal, "completed automatic journal split");
            policy.mark_attempted(journal, now);
        }
        // Lost the CAS to a contending shard's split. The layout still changed,
        // so cool down and reset exactly as for our own split.
        Ok(publisher::SplitOutcome::Lost) => {
            tracing::info!(
                journal,
                "automatic journal split lost a race to another writer; cooling down"
            );
            policy.mark_attempted(journal, now);
        }
        // Too narrow to split, and a journal's width never grows: terminally
        // stop observing it.
        Ok(publisher::SplitOutcome::AtFloor) => {
            tracing::info!(
                journal,
                "journal is at the minimum split width; will not auto-split"
            );
            policy.ignore(journal);
        }
        // Absent from the partition watch (e.g. deleted mid-flight). Forget it:
        // a journal that no longer exists is never written again, so we can
        // just forget it.  If it comes back, that's fine it will just restart the clock
        Ok(publisher::SplitOutcome::Transient) => {
            policy.forget(journal);
        }
        // Splits are opportunistic: an RPC failure must not fail the shard.
        // Leave state untouched so a still-hot journal is retried.
        Err(status) => {
            tracing::warn!(journal, %status, "automatic journal split failed (will retry while due)");
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
    use super::{
        finish_split, max_pinned_segments, observe_throttle_samples, start_due_split,
        stop_for_pinned_segments,
    };
    use crate::shard::split_policy::SplitPolicy;
    use publisher::SplitOutcome;
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

    #[test]
    fn pinned_segments_policy() {
        let segment = shuffle::log::writer::DEFAULT_SEGMENT_THRESHOLD;
        let labeled = |shuffle_disk_limit_bytes| ops::proto::ShardLabeling {
            shuffle_disk_limit_bytes,
            ..Default::default()
        };

        assert_eq!(max_pinned_segments(&labeled(8 * segment)), 4);
        assert_eq!(
            max_pinned_segments(&labeled(segment)),
            1,
            "a limit under two segment thresholds clamps rather than stopping on any remainder",
        );
        assert_eq!(
            max_pinned_segments(&labeled(0)),
            max_pinned_segments(&labeled(shuffle::DEFAULT_SHUFFLE_DISK_LIMIT_BYTES)),
            "a zero limit takes the sidecar default",
        );

        let mut stop_sent = false;
        assert!(!stop_for_pinned_segments(&mut stop_sent, 4, 4));
        assert!(stop_for_pinned_segments(&mut stop_sent, 5, 4));
        assert!(
            !stop_for_pinned_segments(&mut stop_sent, 100, 4),
            "the session is stopped at most once",
        );
    }

    const J: &str = "test/collection/v1/pivot=00";

    /// A policy whose every observed journal is immediately due: no threshold,
    /// no cold-start span. Lets these tests drive due-ness with a single
    /// sample instead of fabricating minutes of clock history.
    fn due_policy() -> (SplitPolicy, Instant) {
        let now = Instant::now();
        let mut policy = SplitPolicy::with_config(crate::shard::split_policy::Config {
            threshold: -1.0,
            min_observation_span: Duration::ZERO,
            ..Default::default()
        });
        policy.observe(J, true, now);
        assert!(policy.should_split(J, now));
        (policy, now)
    }

    /// Split and Lost both mean the journal's layout changed: the journal
    /// enters cooldown (with its EWMA reset) and becomes due again only once
    /// the cooldown passes.
    #[test]
    fn finish_split_applied_and_lost_start_cooldown() {
        for outcome in [SplitOutcome::Split, SplitOutcome::Lost] {
            let (mut policy, now) = due_policy();
            finish_split(&mut policy, J, Ok(outcome), now);

            // Renewed pressure within the cooldown must not re-trigger.
            let later = now + Duration::from_secs(60);
            policy.observe(J, true, later);
            assert!(
                !policy.should_split(J, later),
                "{outcome:?} must start a cooldown",
            );

            // A still-hot journal is due again once the cooldown passes.
            let after = now + Duration::from_secs(31 * 60);
            policy.observe(J, true, after);
            assert!(policy.should_split(J, after), "{outcome:?} cooldown ended");
        }
    }

    /// AtFloor is terminal: the journal's state is dropped and continued
    /// throttling never re-accumulates pressure or re-triggers.
    #[test]
    fn finish_split_at_floor_is_terminal() {
        let (mut policy, now) = due_policy();
        finish_split(&mut policy, J, Ok(SplitOutcome::AtFloor), now);

        let later = now + Duration::from_secs(31 * 60);
        policy.observe(J, true, later);
        assert!(!policy.should_split(J, later));
        assert!(policy.due_for_split(later).is_empty());
    }

    /// A Transient outcome forgets the journal: it's absent from the listing
    /// (e.g. deleted), and a journal that's gone is never written again, so its
    /// frozen EWMA can never decay. Dropping it stops it being perpetually due.
    #[test]
    fn finish_split_transient_forgets_journal() {
        let (mut policy, now) = due_policy();
        finish_split(&mut policy, J, Ok(SplitOutcome::Transient), now);
        assert!(!policy.should_split(J, now));
        assert!(policy.due_for_split(now).is_empty());
    }

    /// An RPC error leaves the policy untouched, so a still-hot journal is
    /// immediately due for retry.
    #[test]
    fn finish_split_error_leaves_state_for_retry() {
        let (mut policy, now) = due_policy();
        finish_split(
            &mut policy,
            J,
            Err(tonic::Status::unavailable("broker")),
            now,
        );
        assert!(policy.should_split(J, now));
    }

    /// A due journal which no Mapped binding can split (here: any journal of
    /// a Preview publisher) takes the terminal off-ramp rather than being
    /// re-evaluated forever.
    #[test]
    fn start_due_split_terminally_ignores_unsplittable_journals() {
        let publisher = crate::publish::RecordingPublisher::default();
        let (mut policy, now) = due_policy();

        assert!(start_due_split(&mut policy, &publisher, now).is_none());
        assert!(!policy.should_split(J, now), "terminally ignored");

        // And with nothing due at all, dispatch is a no-op.
        assert!(start_due_split(&mut policy, &publisher, now).is_none());
    }

    /// One evaluation starts at most one split, and dispatch itself doesn't
    /// alter policy state: both hot journals stay due until an outcome lands,
    /// and the actor's single-flight parking prevents duplicate dispatch.
    #[tokio::test]
    async fn start_due_split_starts_exactly_one() {
        let spec = proto_flow::flow::CollectionSpec {
            name: "test/collection".to_string(),
            partition_template: Some(proto_gazette::broker::JournalSpec {
                name: "test/collection/v1".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let publisher = crate::JournalPublisher::new_test_real([&spec]);

        let (mut policy, now) = due_policy();
        let j2 = "test/collection/v1/pivot=80";
        policy.observe(j2, true, now);
        assert_eq!(policy.due_for_split(now), vec![J, j2]);

        let split = start_due_split(&mut policy, &publisher, now);
        assert!(split.is_some());
        assert_eq!(policy.due_for_split(now), vec![J, j2], "both remain due");
    }
}
