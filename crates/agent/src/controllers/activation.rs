use std::str::FromStr;

use crate::{
    controllers::{ControllerErrorExt, Inbox},
    controlplane::ControlPlane,
};
use anyhow::Context;
use chrono::{DateTime, Utc};
use gazette::consumer::{self, replica_status};
use models::{
    status::activation::{ActivationStatus, ShardFailure, ShardStatusCheck, ShardsStatus},
    AnySpec,
};

use super::{backoff_data_plane_activate, executor::Event, ControllerState, NextRun};

/// We retain rows in the `shard_failures` table for at most this long. Failures
/// are deleted every time the `build_id` changes, or if they're older than this
/// threshold. The controller keeps a count of recent failures in the status,
/// and it periodically cleans up old failures as long as that count is
/// positive.
const FAILURE_RETENTION: chrono::Duration = chrono::Duration::hours(24);

/// The default interval after which we'll attempt to re-activate task shards that are
/// observed via periodic shards listing to have failed.
static REACTIVATE_INTERVAL: std::sync::LazyLock<chrono::Duration> =
    std::sync::LazyLock::new(|| {
        if let Ok(val) = std::env::var("FLOW_REACTIVATE_INTERVAL") {
            let parsed: humantime::Duration =
                FromStr::from_str(&val).expect("invalid FLOW_REACTIVATE_INTERVAL");
            chrono::Duration::from_std(parsed.into())
                .expect("FLOW_REACTIVATE_INTERVAL out of range")
        } else {
            chrono::Duration::minutes(15)
        }
    });

static MAX_SHARD_HEALTH_CHECK_INTERVAL: std::sync::LazyLock<chrono::Duration> =
    std::sync::LazyLock::new(|| {
        if let Ok(val) = std::env::var("FLOW_MAX_SHARD_STATUS_INTERVAL") {
            let parsed: humantime::Duration =
                FromStr::from_str(&val).expect("invalid FLOW_MAX_SHARD_STATUS_INTERVAL");
            chrono::Duration::from_std(parsed.into())
                .expect("FLOW_MAX_SHARD_STATUS_INTERVAL out of range")
        } else {
            chrono::Duration::minutes(15)
        }
    });

/// After we activate task shards, if the shards are still in a `Failed` status
/// and no `ShardFailed` event was observed, then we'll re-attempt activation
/// after this interval. This can happen due to problems with the ops catalog,
/// or (hopefully rare) edge cases in the data plane.
fn get_reactivate_interval(state: &ControllerState) -> chrono::Duration {
    // Special case for ops catalog tasks, which we need to ensure get restarted
    // quickly.
    if is_ops_catalog_task(state) {
        return chrono::Duration::seconds(30);
    }
    *REACTIVATE_INTERVAL
}

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
        let has_task_shards = has_task_shards(state);
        if has_task_shards {
            // Reset the shard health status, as we'll need to await another successful check.
            status.shard_status = Some(ShardStatusCheck {
                count: 0,
                first_ts: now,
                last_ts: now,
                status: ShardsStatus::Pending,
            });
        } else {
            // Clear an existing shard status in case the spec has transitioned
            // to no longer having shards.
            status.shard_status.take();
        }
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
        // We'll check again soon to see whether the shard is actually up
        return Ok(has_task_shards.then_some(NextRun::after_minutes(5)));
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
            status.next_retry = Some(next);
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
            if has_task_shards(state) {
                // Reset the shard health status, as we'll need to await another successful check.
                status.shard_status = Some(ShardStatusCheck {
                    count: 0,
                    first_ts: now,
                    last_ts: now,
                    status: ShardsStatus::Pending,
                });
                return Ok(Some(NextRun::from_duration(shard_health_check_interval(
                    state,
                    0,
                    ShardsStatus::Pending,
                ))));
            }
        } else {
            tracing::debug!(restart_at = %rt, "waiting for backoff before restarting failed task shards")
        }
    }

    // If we're waiting to restart failed shards, then we're done for now.
    // Also return now if this is not a task with shards to monitor.
    if status.next_retry.is_some() || !has_task_shards(state) {
        return Ok(status
            .next_retry
            .map(|rt| NextRun::after(rt).with_jitter_percent(0)));
    }

    // At this point we're finished with any activations that are needed, and
    // it's time to ensure that task shards have actually started successfully.
    let next_run = update_shard_health(status, control_plane, state).await?;
    Ok(next_run)
}

async fn update_shard_health<C: ControlPlane>(
    status: &mut ActivationStatus,
    control_plane: &C,
    state: &ControllerState,
) -> anyhow::Result<Option<NextRun>> {
    let (current_shard_status, count) = status
        .shard_status
        .as_ref()
        .map(|s| (s.status, s.count))
        .unwrap_or((ShardsStatus::Pending, 0));
    let wait_interval = shard_health_check_interval(state, count, current_shard_status);
    let Some(from) = status
        .shard_status
        .as_ref()
        .map(|check| check.last_ts)
        .or(status.last_activated_at)
    else {
        anyhow::bail!(
            "internal controller error: attempted to health check a spec that was never activated"
        );
    };

    let next = from + wait_interval;
    let now = control_plane.current_time();
    if now < next {
        // We're still waiting for the next health check
        return Ok(Some(NextRun::after(next)));
    }

    // Check the status of the shards
    let Some(task_type) = state
        .live_spec
        .as_ref()
        .and_then(|s| to_ops_task_type(s.catalog_type()))
    else {
        anyhow::bail!("internal controller error: attempted to health check a catalog test");
    };

    let list_response = control_plane
        .list_task_shards(state.data_plane_id, task_type, state.catalog_name.clone())
        .await
        .context("listing task shards")?;

    if list_response.status != proto_gazette::consumer::Status::Ok as i32 {
        return Err(anyhow::anyhow!(
            "shard list response status not Ok, was {}",
            list_response.status().as_str_name()
        ))
        .with_retry(NextRun::after_minutes(1))
        .map_err(Into::into);
    }

    // Determine aggregate status from list response
    let new_status = aggregate_shard_status(state.last_build_id, &list_response.shards);

    let (first_ts, count) = match &status.shard_status {
        Some(prev) if prev.status == new_status => (prev.first_ts, prev.count + 1),
        Some(prev_status) => {
            tracing::info!(?prev_status, prev_count = %prev_status.count, new_status = ?new_status, "task shard health status changed");
            (now, 1)
        }
        None => (now, 1),
    };
    status.shard_status = Some(ShardStatusCheck {
        count,
        first_ts,
        last_ts: now,
        status: new_status,
    });

    // If there's been at least 3 failed checks in a row, and it's been longer than the
    // re-activation interval, then re-activate failed shards. We require at least two
    // failed checks in a row because sometimes shards can temporarily be in failed state
    // just due normal infrastructure changes in the data plane.
    let time_since_activation = now - status.last_activated_at.unwrap();
    if new_status == ShardsStatus::Failed
        && time_since_activation >= get_reactivate_interval(state)
        && count >= 3
    {
        // If we've reached this section, then we have _not_ received any
        // ShardFailed events, which would have caused us to set a `next_retry`.
        // This can happen if the ops catalog is broken, or in certain edge
        // cases in the data plane.
        tracing::warn!(%time_since_activation, failed_checks = %count, "re-activating task shards because they still show as Failed after prior activation");
        do_activate(now, state, status, control_plane).await?;
    }

    let next_check = shard_health_check_interval(state, count, new_status);
    tracing::debug!(%count, ?next_check, ?new_status, "finished task shard health check");
    Ok(Some(NextRun::from_duration(next_check)))
}

fn is_ops_catalog_task(state: &ControllerState) -> bool {
    state.catalog_name.starts_with("ops/") || state.catalog_name.starts_with("ops.us-central1.v1/")
}

fn aggregate_shard_status(
    last_build_id: models::Id,
    shards: &[consumer::list_response::Shard],
) -> ShardsStatus {
    use ShardsStatus::*;

    if shards.is_empty() {
        tracing::warn!("shard listing was empty");
    }

    shards
        .iter()
        .map(|shard| {
            // Map any shards with a stale build id to a Pending status. In the
            // happy path, we should never observe this condition, but it can
            // technically happen, since we don't pass a minimum etcd revision
            // with our list request. So if we see an old build id here, it's
            // just stale data.
            let matching_build = shard.spec.as_ref().is_some_and(|spec| {
                spec.labels.as_ref().is_some_and(|labels| {
                    ::labels::values(labels, ::labels::BUILD)
                        .first()
                        .is_some_and(|build_label| {
                            let parsed = models::Id::from_hex(&build_label.value).ok();
                            Some(last_build_id) == parsed
                        })
                })
            });
            if !matching_build {
                return Pending;
            }

            // First determine a status for each shard, from the status of each of its replicas.
            // As long as _any_ replica is Ok, the whole shard is Ok. This is why we have redundency.
            shard
                .status
                .iter()
                .map(|s| match s.code() {
                    replica_status::Code::Idle => Pending,
                    replica_status::Code::Backfill => Pending,
                    replica_status::Code::Standby => Pending,
                    replica_status::Code::Primary => Ok,
                    replica_status::Code::Failed => Failed,
                })
                .fold(
                    ShardsStatus::Pending,
                    |shard_status, replica_status| match (shard_status, replica_status) {
                        (Ok, _) => Ok,
                        (_, Ok) => Ok,
                        (Pending | Failed, Failed) => Failed,
                        (Failed, Pending) => Failed,
                        (Pending, Pending) => Pending,
                    },
                )
        })
        // Now reduce the statuses of each shard into an aggregate for the whole task. Unlike before, _all_
        // the shards must be Ok in order for the result to be Ok.
        .reduce(|l, r| match (l, r) {
            (Ok, Ok) => Ok,
            (Failed, _) => Failed,
            (_, Failed) => Failed,
            (Ok | Pending, Pending) => Pending,
            (Pending, Ok) => Pending,
        })
        .unwrap_or(Pending) // Pending if shards list was empty
}

fn to_ops_task_type(catalog_type: models::CatalogType) -> Option<ops::TaskType> {
    match catalog_type {
        models::CatalogType::Capture => Some(ops::TaskType::Capture),
        models::CatalogType::Collection => Some(ops::TaskType::Derivation),
        models::CatalogType::Materialization => Some(ops::TaskType::Materialization),
        models::CatalogType::Test => None,
    }
}

fn shard_health_check_interval(
    state: &ControllerState,
    prev_checks: u32,
    current_status: ShardsStatus,
) -> chrono::Duration {
    use chrono::Duration;

    // Special case for ops catalog tasks, we health check them more frequently
    // because we're unable to get `ShardFailed` events for them if they fail.
    let max_duration = if is_ops_catalog_task(state) {
        Duration::minutes(5)
    } else {
        *MAX_SHARD_HEALTH_CHECK_INTERVAL
    };

    // If the status is OK, then backoff much more quickly
    let duration = if current_status == ShardsStatus::Ok {
        match prev_checks {
            0..3 => Duration::minutes(3),
            3..6 => Duration::minutes(10),
            6..10 => Duration::minutes(60),
            _ => max_duration,
        }
    } else {
        match prev_checks {
            0..3 => Duration::seconds(30),
            3..10 => Duration::minutes(1),
            10..20 => Duration::minutes(3),
            _ => Duration::minutes(60),
        }
    };

    duration.min(max_duration)
}

fn get_next_retry_time(now: DateTime<Utc>, failures: &[ShardFailure]) -> DateTime<Utc> {
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
        0..=2 => now,
        moar => {
            // Limit the backoff to at most 15 minutes, since we don't yet have
            // a way to alert users to failures. We use a pretty substantial
            // jitter to try to stagger restarts, since various data plane
            // issues can cause many shards to fail at around the same time.
            let backoff = NextRun::after_minutes(10.min(moar * 2))
                .with_jitter_percent(50)
                .compute_duration();
            now + backoff
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

fn has_task_shards(state: &ControllerState) -> bool {
    match state.live_spec.as_ref() {
        Some(&AnySpec::Capture(ref cap)) if !cap.shards.disable => {
            // There's currently no such thing as a dekaf capture, but it seemed best to handle captures and materializations
            matches!(
                &cap.endpoint,
                &models::CaptureEndpoint::Connector(ref conn) if !conn.image.starts_with(models::DEKAF_IMAGE_NAME_PREFIX)
            )
        }
        Some(&AnySpec::Collection(ref coll)) => coll
            .derive
            .as_ref()
            .is_some_and(|derive| !derive.shards.disable),
        Some(&AnySpec::Materialization(ref mat)) if !mat.shards.disable => {
            matches!(
                &mat.endpoint,
                &models::MaterializationEndpoint::Connector(ref conn) if !conn.image.starts_with(models::DEKAF_IMAGE_NAME_PREFIX)
            )
        }
        _ => false,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::Duration;
    use itertools::Itertools;

    #[test]
    fn test_aggregate_shard_status() {
        use consumer::replica_status::Code;

        fn mock_shard(
            id: &str,
            build: models::Id,
            replica_statuses: &[Code],
        ) -> consumer::list_response::Shard {
            let shard_labels =
                ::labels::build_set(Some((labels::BUILD.to_string(), build.to_string())));
            let spec = consumer::ShardSpec {
                id: id.to_string(),
                labels: Some(shard_labels),
                ..Default::default()
            };
            let shard_status = replica_statuses
                .into_iter()
                .map(|c| consumer::ReplicaStatus {
                    code: (*c) as i32,
                    errors: Vec::new(),
                })
                .collect_vec();
            consumer::list_response::Shard {
                spec: Some(spec),
                mod_revision: 11,
                route: None,
                status: shard_status,
                create_revision: 10,
            }
        }

        let stale_build = models::Id::new([2; 8]);
        let current_build = models::Id::new([8; 8]);
        let actual = aggregate_shard_status(
            current_build,
            &[mock_shard("shard/one", current_build, &[Code::Primary])],
        );
        assert_eq!(ShardsStatus::Ok, actual);

        // stale build results in Pending
        let actual = aggregate_shard_status(
            current_build,
            &[mock_shard("shard/one", stale_build, &[Code::Primary])],
        );
        assert_eq!(ShardsStatus::Pending, actual);

        // No shards maps to Pending
        let actual = aggregate_shard_status(current_build, &[]);
        assert_eq!(ShardsStatus::Pending, actual);

        // As long as one replica of each is primary, the result should be Ok
        let actual = aggregate_shard_status(
            current_build,
            &[
                mock_shard(
                    "shard/one",
                    current_build,
                    &[Code::Primary, Code::Backfill, Code::Failed],
                ),
                mock_shard("shard/two", current_build, &[Code::Standby, Code::Primary]),
                mock_shard(
                    "shard/three",
                    current_build,
                    &[Code::Primary, Code::Standby, Code::Idle],
                ),
            ],
        );
        assert_eq!(ShardsStatus::Ok, actual);

        // One shard has a stale build id
        let actual = aggregate_shard_status(
            current_build,
            &[
                mock_shard("shard/one", current_build, &[Code::Primary]),
                mock_shard("shard/two", stale_build, &[Code::Primary]),
                mock_shard("shard/three", current_build, &[Code::Primary]),
            ],
        );
        assert_eq!(ShardsStatus::Pending, actual);

        // one shard still backfilling
        let actual = aggregate_shard_status(
            current_build,
            &[
                mock_shard("shard/one", current_build, &[Code::Primary]),
                mock_shard("shard/two", current_build, &[Code::Backfill]),
                mock_shard("shard/three", current_build, &[Code::Primary]),
            ],
        );
        assert_eq!(ShardsStatus::Pending, actual);

        // One shard failed
        let actual = aggregate_shard_status(
            current_build,
            &[
                mock_shard("shard/one", current_build, &[Code::Primary]),
                mock_shard("shard/two", current_build, &[Code::Failed]),
                mock_shard("shard/three", current_build, &[Code::Primary]),
            ],
        );
        assert_eq!(ShardsStatus::Failed, actual);
    }

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
        let next = get_next_retry_time(now, &shard_failures);
        next - now
    }
}
