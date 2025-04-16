use crate::{
    controllers::{ControllerErrorExt, Inbox},
    controlplane::ControlPlane,
};
use anyhow::Context;
use chrono::{DateTime, Utc};
use models::status::activation::{ActivationStatus, ShardFailure};

use super::{backoff_data_plane_activate, executor::Event, ControllerState, NextRun};

/// We retain rows in the `shard_failures` table for at most this long. Failures
/// are deleted every time the `build_id` changes, or if they're older than this
/// threshold. The controller keeps a count of recent failures in the status,
/// and it periodically cleans up old failures as long as that count is
/// positive.
const FAILURE_RETENTION: chrono::Duration = chrono::Duration::hours(24);

/// Activates the spec in the data plane if necessary.
pub async fn update_activation<C: ControlPlane>(
    status: &mut ActivationStatus,
    state: &ControllerState,
    events: &Inbox,
    control_plane: &C,
) -> anyhow::Result<Option<NextRun>> {
    let now = control_plane.current_time();
    let failure_retention_threshold = now - FAILURE_RETENTION;
    // Did we receive at least one shard failure message?
    let shard_failures = events
        .iter()
        .filter(|(_, e)| matches!(e, Some(Event::ShardFailed)))
        .count();

    // Activating a new build always takes precedence over failure handling.
    // We'll ignore any shard failures from previous builds.
    if state.last_build_id > status.last_activated {
        if shard_failures > 0 {
            tracing::info!(
                count = shard_failures,
                "ignoring shard failures, activating a new build"
            );
        };
        do_activate(now, state, status, control_plane).await?;
        // Delete any shard failure records from previous builds. This effectively resets
        // the retry backoff for any failures that happen after this activation.
        control_plane
            .delete_shard_failures(
                state.catalog_name.clone(),
                status.last_activated,
                failure_retention_threshold,
            )
            .await?;
        status.recent_failure_count = 0;
        return Ok(None);
    }

    // Update our shard failure information. We can skip this if we know that there's
    // been no recent failures.
    if shard_failures > 0 || status.recent_failure_count > 0 {
        // Delete any shard failure records that are from previous builds, or that are too old.
        control_plane
            .delete_shard_failures(
                state.catalog_name.clone(),
                status.last_activated,
                failure_retention_threshold,
            )
            .await?;

        // Fetch the set of recent shard failures, and determine
        // an appropriate time to re-activate any failed shards.
        let failures = control_plane
            .get_shard_failures(state.catalog_name.clone())
            .await?;
        status.recent_failure_count = failures
            .iter()
            .filter(|f| {
                f.shard.build == status.last_activated && f.ts > failure_retention_threshold
            })
            .count() as u32;

        // Determine a time for the next restart attempt. If we've already
        // determined a `next_retry` time, then we won't push it back in
        // response to additional failures. This ensures that we won't backoff
        // indefinitely. Note that this could return `now` in order to restart
        // failed shards immediately.
        if shard_failures > 0 && status.next_retry.is_none() {
            let next = get_next_retry_time(now, &failures);
            tracing::info!(next_retry = ?next, "observed shard failure and determined next retry time");
            status.next_retry = next;
        }

        // Update the `lastFailure` status field
        if let Some(latest) = failures.iter().max_by_key(|f| f.ts) {
            let last_failure_ts = status
                .last_failure
                .as_ref()
                .map(|f| f.ts)
                .unwrap_or(DateTime::<Utc>::MIN_UTC);
            if latest.ts > last_failure_ts {
                status.last_failure = Some(latest.clone());
            } else {
                // This just means that we observed an out of order failure event, which is fine.
                tracing::debug!(
                    last_failure = ?status.last_failure,
                    latest_event = ?latest,
                    event_count = failures.len(),
                    "shard failure event received out of order (this is ok)");
            }
        }
    }

    if let Some(rt) = status.next_retry {
        if rt <= now {
            tracing::info!(
                recent_failure_count = status.recent_failure_count,
                "restarting failed task shards"
            );
            do_activate(now, state, status, control_plane).await?;
        } else {
            tracing::debug!(restart_at = %rt, "waiting for backoff before restarting failed task shards")
        }
    }

    if status.next_retry.is_some() {
        return Ok(status
            .next_retry
            .map(|rt| NextRun::after(rt).with_jitter_percent(0)));
    } else if status.recent_failure_count > 0 {
        return Ok(Some(NextRun::after_minutes(60)));
    } else {
        return Ok(None);
    }
}

fn get_next_retry_time(now: DateTime<Utc>, failures: &[ShardFailure]) -> Option<DateTime<Utc>> {
    use itertools::Itertools;

    // Divide the total number of failures by the number of unique shards that
    // have failed. This is to avoid exceedingly steep backoffs when a sharded
    // task fails. I.e. if all shards fail once, we want to re-start all of them
    // immediately and only start backing off when we see subsequent failures of
    // the same shards.
    let shard_count = failures
        .iter()
        .unique_by(|f| (f.shard.key_begin.as_str(), f.shard.r_clock_begin.as_str()))
        .count();
    let consecutive_failures = (failures.len() as f32 / shard_count as f32).ceil() as u32;
    let next = match consecutive_failures {
        0..=2 => Some(now),
        moar => {
            // Limit the backoff to at most 15 minutes, since we don't yet have
            // a way to alert users to failures. We use a pretty substantial
            // jitter to try to stagger restarts, since various data plane
            // issues can cause many shards to fail at around the same time.
            let backoff = NextRun::after_minutes(10.min(moar * 2))
                .with_jitter_percent(50)
                .compute_duration();
            Some(now + backoff)
        }
    };
    tracing::info!(
        ?next,
        %now,
        %consecutive_failures,
        %shard_count,
        recent_failures = %failures.len(), "determined next retry time for failed task");
    next
}

async fn do_activate<C: ControlPlane>(
    now: DateTime<Utc>,
    state: &ControllerState,
    status: &mut ActivationStatus,
    control_plane: &C,
) -> anyhow::Result<()> {
    let name = state.catalog_name.clone();
    let built_spec = state.built_spec.as_ref().expect("built_spec must be Some");

    crate::timeout(
        std::time::Duration::from_secs(60),
        control_plane.data_plane_activate(name, built_spec, state.data_plane_id),
        || "Timeout while activating into data-plane",
    )
    .await
    .with_retry(backoff_data_plane_activate(state.failures))
    .context("failed to activate into data-plane")?;

    tracing::debug!(last_activated = %state.last_build_id, "activated");
    status.last_activated = state.last_build_id;
    status.last_activated_at = Some(now);
    // Clear a scheduled retry, since the activation was successful.
    status.next_retry.take();

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::Duration;

    #[test]
    fn test_restart_backoffs() {
        let no_backoff = Duration::zero();
        let zero = "00000000";
        let one = "11111111";

        backoff_test(no_backoff, &[(zero, zero)]);
        backoff_test(no_backoff, &[(zero, zero), (zero, zero)]);
        backoff_test(
            Duration::minutes(6),
            &[(zero, zero), (zero, zero), (zero, zero)],
        );
        backoff_test(
            Duration::minutes(8),
            &[(zero, zero), (zero, zero), (zero, zero), (zero, zero)],
        );
        backoff_test(
            Duration::minutes(10),
            &[
                (zero, zero),
                (zero, zero),
                (zero, zero),
                (zero, zero),
                (zero, zero),
                (zero, zero),
                (zero, zero),
                (zero, zero),
            ],
        );

        // One or two failures of each shard should be retried immediately
        backoff_test(
            no_backoff,
            &[(zero, zero), (zero, one), (one, zero), (one, one)],
        );
        backoff_test(
            no_backoff,
            &[
                (zero, zero),
                (zero, zero),
                (zero, one),
                (zero, one),
                (one, zero),
                (one, zero),
                (one, one),
                (one, one),
            ],
        );

        backoff_test(
            no_backoff,
            &[
                (zero, zero),
                (zero, one), // different shard
                (zero, zero),
                (zero, zero),
            ],
        );
        backoff_test(
            Duration::minutes(6),
            &[
                (zero, zero),
                (zero, one), // different shard
                (zero, zero),
                (zero, zero),
                (zero, zero),
            ],
        );
    }

    fn backoff_test(expected: Duration, failures: &[(&str, &str)]) {
        let min = expected
            .checked_sub(&Duration::seconds(10))
            .unwrap_or(Duration::zero());
        let max = expected + (expected / 2);
        let backoff = compute_backoff(failures);
        assert!(
            backoff >= min,
            "expected backoff to be at least {min:?}, got {backoff:?}, for failures: {failures:?}"
        );
        assert!(
            backoff <= max,
            "expected backoff to be at most {max:?}, got {backoff:?}, for failures: {failures:?}"
        );
    }

    fn compute_backoff(failures: &[(&str, &str)]) -> Duration {
        let ts: DateTime<Utc> = "2024-04-05T06:07:08.09Z".parse().unwrap();

        let shard_failures = failures
            .into_iter()
            .enumerate()
            .map(|(i, (key, rclock))| ShardFailure {
                shard: models::status::ShardRef {
                    name: "test/task".to_string(),
                    key_begin: key.to_string(),
                    r_clock_begin: rclock.to_string(),
                    build: models::Id::zero(),
                },
                ts: ts + Duration::minutes(i as i64),
                message: "oh no".to_string(),
                fields: serde_json::from_value(serde_json::json!({
                    "error": "some error message"
                }))
                .unwrap(),
            })
            .collect::<Vec<_>>();

        let now: DateTime<Utc> = "2024-04-05T08:07:08.09Z".parse().unwrap();
        let next = get_next_retry_time(now, &shard_failures).unwrap();
        next - now
    }
    // fn failure(key_begin: &str, rclock_begin: &str)
}
