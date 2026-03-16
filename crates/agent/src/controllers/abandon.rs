use super::{ControllerConfig, ControllerState, NextRun, alerts};
use crate::controllers::activation::has_task_shards;
use crate::controllers::publication_status::PendingPublication;
use crate::controlplane::ControlPlane;
use chrono::{DateTime, Utc};
use models::status::publications::PublicationStatus;
use models::status::{AlertType, Alerts};

/// Extra field keys stored in alert `extra` maps for notification templates.
const EXTRA_DISABLE_AT: &str = "disable_at";
const EXTRA_FAILING_SINCE: &str = "failing_since";

/// Why a task is being auto-disabled, used as the publication detail message.
#[derive(Debug, PartialEq, Eq)]
enum DisableReason {
    ChronicallyFailing,
    Idle,
}

#[derive(Clone, Copy, Default)]
struct AbandonmentTimestamps {
    last_data_movement_ts: Option<DateTime<Utc>>,
    last_user_pub_at: Option<DateTime<Utc>>,
}

impl std::fmt::Display for DisableReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChronicallyFailing => f.write_str("auto-disabling chronically failing task"),
            Self::Idle => f.write_str("auto-disabling idle task"),
        }
    }
}

pub async fn evaluate_abandoned<C: ControlPlane>(
    alerts_status: &mut Alerts,
    publications: &mut PublicationStatus,
    state: &ControllerState,
    control_plane: &C,
) -> anyhow::Result<Option<NextRun>> {
    // Tasks without shards (disabled, Dekaf, etc.) don't need abandon monitoring.
    if !has_task_shards(state) {
        resolve_all_abandon_alerts(alerts_status);
        return Ok(None);
    }

    let now = control_plane.current_time();
    let config = control_plane.controller_config();
    let timestamps =
        fetch_abandonment_timestamps(alerts_status, state, control_plane, now, &config).await?;

    let disable_reason = evaluate_alerts(alerts_status, state, now, timestamps, &config);

    if let Some(reason) = disable_reason {
        if config.disable_abandoned_tasks {
            if maybe_disable_task(reason, state, publications, control_plane).await? {
                return Ok(Some(NextRun::immediately()));
            }
        }
    }

    // Safety-net wake: controllers already wake every ~2 hours for shard health
    // checks, so this 24-hour timer just ensures abandon evaluation isn't skipped
    // indefinitely if those checks stop scheduling wakes.
    Ok(Some(NextRun::after_minutes(24 * 60)))
}

async fn fetch_abandonment_timestamps<C: ControlPlane>(
    alerts_status: &Alerts,
    state: &ControllerState,
    control_plane: &C,
    now: DateTime<Utc>,
    config: &ControllerConfig,
) -> anyhow::Result<AbandonmentTimestamps> {
    let last_user_pub_at = control_plane
        .fetch_last_user_publication_at(state.live_spec_id)
        .await?;

    let has_failure_alerts = alerts_status.contains_key(&AlertType::ShardFailed)
        || alerts_status.contains_key(&AlertType::TaskChronicallyFailing);
    let user_pub_stale =
        last_user_pub_at.map_or(true, |ts| (now - ts) >= config.abandon_user_pub_recency);

    let last_data_movement_ts = if has_failure_alerts || !user_pub_stale {
        None
    } else {
        control_plane
            .fetch_last_data_movement_ts(state.catalog_name.clone())
            .await?
    };

    Ok(AbandonmentTimestamps {
        last_data_movement_ts,
        last_user_pub_at,
    })
}

/// Pure evaluation of abandon alert state. Returns `Some(reason)` when the
/// grace period for an auto-disable sequence has expired and the task should
/// be disabled, or `None` if no disable is needed.
fn evaluate_alerts(
    alerts_status: &mut Alerts,
    state: &ControllerState,
    now: DateTime<Utc>,
    timestamps: AbandonmentTimestamps,
    config: &ControllerConfig,
) -> Option<DisableReason> {
    // Tasks that are disabled, Dekaf, or lack shards don't get abandonment checks.
    // Silently resolve any alerts that may have been firing so re-enablement starts clean.
    let Some(spec) = state.live_spec.as_ref().filter(|_| has_task_shards(state)) else {
        resolve_all_abandon_alerts(alerts_status);
        return None;
    };

    let catalog_type = spec.catalog_type();

    let user_pub_stale = timestamps
        .last_user_pub_at
        .map_or(true, |ts| (now - ts) >= config.abandon_user_pub_recency);

    // Sequence 1: Chronically Failing
    // Fires when ShardFailed has been continuously active for an extended period
    // and no user has published changes recently (a recent publication signals active debugging).
    let shard_failed_since = alerts_status
        .get(&AlertType::ShardFailed)
        .map(|a| a.first_ts);

    let is_chronically_failing = shard_failed_since
        .is_some_and(|first_ts| (now - first_ts) >= config.chronically_failing_threshold)
        && user_pub_stale;

    let mut disable_reason = None;

    if is_chronically_failing {
        let shard_failed_first_ts = shard_failed_since.unwrap();

        // Only fire the alert once. On subsequent evaluations the stored
        // disable_at and failing_since are the source of truth, so we don't
        // shift the disable date if the env config changes between runs.
        if !alerts_status.contains_key(&AlertType::TaskChronicallyFailing) {
            let failing_since = shard_failed_first_ts.format("%Y-%m-%d").to_string();
            let disable_at = (now + config.chronically_failing_disable_after)
                .format("%Y-%m-%d")
                .to_string();

            alerts::set_alert_firing(
                alerts_status,
                AlertType::TaskChronicallyFailing,
                now,
                format!("task shards have been failing since {failing_since}"),
                1, // Each abandonment alert is only ever fired once.
                catalog_type,
            );
            let alert = alerts_status
                .get_mut(&AlertType::TaskChronicallyFailing)
                .expect("just inserted");
            alert
                .extra
                .insert(EXTRA_DISABLE_AT.to_string(), disable_at.into());
            alert
                .extra
                .insert(EXTRA_FAILING_SINCE.to_string(), failing_since.into());
        }

        let alert = alerts_status
            .get(&AlertType::TaskChronicallyFailing)
            .expect("TaskChronicallyFailing alert was just set");
        let disable_at = alert
            .extra
            .get(EXTRA_DISABLE_AT)
            .expect("disable_at was just inserted")
            .as_str()
            .expect("disable_at must be a string")
            .to_string();
        let failing_since = alert
            .extra
            .get(EXTRA_FAILING_SINCE)
            .expect("failing_since was just inserted")
            .as_str()
            .expect("failing_since must be a string")
            .to_string();
        let disable_at = chrono::NaiveDate::parse_from_str(&disable_at, "%Y-%m-%d")
            .expect("disable_at was set from a valid date");

        if now.date_naive() >= disable_at {
            if !alerts_status.contains_key(&AlertType::TaskAutoDisabledFailing) {
                alerts::set_alert_firing(
                    alerts_status,
                    AlertType::TaskAutoDisabledFailing,
                    now,
                    format!("task auto-disabled after shards failing since {failing_since}"),
                    1,
                    catalog_type,
                );
                alerts_status
                    .get_mut(&AlertType::TaskAutoDisabledFailing)
                    .expect("just inserted")
                    .extra
                    .insert(
                        EXTRA_FAILING_SINCE.to_string(),
                        failing_since.clone().into(),
                    );
            }
            disable_reason = Some(DisableReason::ChronicallyFailing);
        }
    } else {
        alerts::resolve_alert(alerts_status, AlertType::TaskChronicallyFailing);
        alerts::resolve_alert(alerts_status, AlertType::TaskAutoDisabledFailing);
    }

    // Sequence 2: Idle Task
    // Fires when no data has moved AND no user publication for extended periods.
    // Suppressed when ShardFailed or TaskChronicallyFailing is active to avoid
    // sending both "your task is failing" and "your task is idle" simultaneously.
    let has_failure_alerts = alerts_status.contains_key(&AlertType::ShardFailed)
        || alerts_status.contains_key(&AlertType::TaskChronicallyFailing);

    let data_stale = timestamps
        .last_data_movement_ts
        .map_or(true, |ts| (now - ts) >= config.abandon_idle_threshold);

    let is_idle = !has_failure_alerts && data_stale && user_pub_stale;

    if is_idle {
        if !alerts_status.contains_key(&AlertType::TaskIdle) {
            let disable_at = (now + config.abandon_idle_disable_after)
                .format("%Y-%m-%d")
                .to_string();

            alerts::set_alert_firing(
                alerts_status,
                AlertType::TaskIdle,
                now,
                format!(
                    "task has not moved data{}",
                    timestamps
                        .last_data_movement_ts
                        .map(|ts| format!(" since {}", ts.format("%Y-%m-%d")))
                        .unwrap_or_else(|| " since it was created".to_string())
                ),
                1,
                catalog_type,
            );
            alerts_status
                .get_mut(&AlertType::TaskIdle)
                .expect("just inserted")
                .extra
                .insert(EXTRA_DISABLE_AT.to_string(), disable_at.into());
        }

        let disable_at = alerts_status
            .get(&AlertType::TaskIdle)
            .expect("TaskIdle alert was just set")
            .extra
            .get(EXTRA_DISABLE_AT)
            .expect("disable_at was just inserted")
            .as_str()
            .expect("disable_at must be a string");
        let disable_at = chrono::NaiveDate::parse_from_str(disable_at, "%Y-%m-%d")
            .expect("disable_at was set from a valid date");

        if now.date_naive() >= disable_at {
            if !alerts_status.contains_key(&AlertType::TaskAutoDisabledIdle) {
                alerts::set_alert_firing(
                    alerts_status,
                    AlertType::TaskAutoDisabledIdle,
                    now,
                    "task auto-disabled due to inactivity".to_string(),
                    1,
                    catalog_type,
                );
            }
            disable_reason = Some(DisableReason::Idle);
        }
    } else {
        alerts::resolve_alert(alerts_status, AlertType::TaskIdle);
        alerts::resolve_alert(alerts_status, AlertType::TaskAutoDisabledIdle);
    }

    disable_reason
}

/// Publishes the spec with `shards.disable = true`. Returns `true` if the
/// publication succeeded, signaling the caller to return `NextRun::immediately()`
/// so the controller re-enters and processes the now-disabled spec.
async fn maybe_disable_task<C: ControlPlane>(
    reason: DisableReason,
    state: &ControllerState,
    publications: &mut PublicationStatus,
    control_plane: &C,
) -> anyhow::Result<bool> {
    let mut spec = state
        .live_spec
        .clone()
        .expect("must have live spec to disable");

    set_spec_disabled(&mut spec);
    let mut pending = PendingPublication::update_model(
        &state.catalog_name,
        state.last_pub_id,
        spec,
        &reason.to_string(),
    );

    // `finish()` normally fires BackgroundPublicationFailed after repeated
    // publication failures. Pass None to suppress that: if we're disabling
    // an abandoned task, an additional alert about the disable publication
    // failing is just noise. The failure is still recorded in publication
    // history and we'll retry on the next controller run.
    let result = pending
        .finish(state, publications, None, control_plane)
        .await?;
    Ok(result.status.is_success())
}

fn set_spec_disabled(spec: &mut models::AnySpec) {
    match spec {
        models::AnySpec::Capture(c) => c.shards.disable = true,
        models::AnySpec::Collection(c) => {
            c.derive
                .as_mut()
                .expect("collection without derivation should not have task shards")
                .shards
                .disable = true;
        }
        models::AnySpec::Materialization(m) => m.shards.disable = true,
        models::AnySpec::Test(_) => unreachable!("tests do not have shards"),
    }
}

fn resolve_all_abandon_alerts(alerts_status: &mut Alerts) {
    alerts::resolve_alert(alerts_status, AlertType::TaskChronicallyFailing);
    alerts::resolve_alert(alerts_status, AlertType::TaskAutoDisabledFailing);
    alerts::resolve_alert(alerts_status, AlertType::TaskIdle);
    alerts::resolve_alert(alerts_status, AlertType::TaskAutoDisabledIdle);
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::{Duration, TimeZone};
    use models::status::{AlertState, ControllerAlert, ControllerStatus};
    use models::{AnySpec, Id};

    fn fixed_now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap()
    }

    fn mock_state(live_spec: Option<AnySpec>, created_at: DateTime<Utc>) -> ControllerState {
        ControllerState {
            live_spec_id: Id::zero(),
            catalog_name: "test/task".to_string(),
            live_spec,
            built_spec: None,
            controller_updated_at: created_at,
            live_spec_updated_at: created_at,
            created_at,
            failures: 0,
            error: None,
            last_pub_id: Id::zero(),
            last_build_id: Id::zero(),
            logs_token: uuid::Uuid::nil(),
            controller_version: 2,
            current_status: ControllerStatus::Uninitialized,
            data_plane_id: Id::zero(),
            data_plane_name: None,
            live_dependency_hash: None,
        }
    }

    fn enabled_capture() -> AnySpec {
        AnySpec::Capture(models::CaptureDef {
            endpoint: models::CaptureEndpoint::Connector(models::ConnectorConfig {
                image: "source/test:test".to_string(),
                config: models::RawValue::from_str("{}").unwrap(),
            }),
            bindings: vec![],
            shards: Default::default(),
            auto_discover: None,
            interval: std::time::Duration::from_secs(300),
            redact_salt: None,
            expect_pub_id: None,
            delete: false,
            reset: false,
        })
    }

    fn disabled_capture() -> AnySpec {
        let mut cap = enabled_capture();
        if let AnySpec::Capture(ref mut c) = cap {
            c.shards.disable = true;
        }
        cap
    }

    fn enabled_materialization() -> AnySpec {
        AnySpec::Materialization(models::MaterializationDef {
            endpoint: models::MaterializationEndpoint::Connector(models::ConnectorConfig {
                image: "materialize/test:test".to_string(),
                config: models::RawValue::from_str("{}").unwrap(),
            }),
            bindings: vec![],
            shards: Default::default(),
            source: None,
            on_incompatible_schema_change: Default::default(),
            expect_pub_id: None,
            delete: false,
            reset: false,
        })
    }

    /// ShardFailed is external state from shard health monitoring, not produced
    /// by `evaluate_alerts`, so tests must always pre-populate it.
    fn shard_failed_alert(first_ts: DateTime<Utc>, last_ts: DateTime<Utc>) -> ControllerAlert {
        ControllerAlert {
            state: AlertState::Firing,
            spec_type: models::CatalogType::Capture,
            first_ts,
            last_ts: Some(last_ts),
            error: "shard failed".to_string(),
            count: 5,
            resolved_at: None,
            extra: Default::default(),
        }
    }

    // Sequence 1: Chronically Failing

    #[test]
    fn no_shard_failed_means_not_chronically_failing() {
        let config = ControllerConfig::default();
        let now = fixed_now();

        let state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        let mut alerts: Alerts = Default::default();

        evaluate_alerts(
            &mut alerts,
            &state,
            now,
            AbandonmentTimestamps::default(),
            &config,
        );

        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @r#"
        {
          "task_idle": {
            "count": 1,
            "disable_at": "2025-06-08",
            "error": "task has not moved data since it was created",
            "first_ts": "2025-06-01T12:00:00Z",
            "last_ts": null,
            "resolved_at": null,
            "spec_type": "capture",
            "state": "firing"
          }
        }
        "#);
    }

    #[test]
    fn recent_shard_failed_not_chronically_failing() {
        let config = ControllerConfig::default();
        let now = fixed_now();

        let state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        let mut alerts: Alerts = Default::default();
        alerts.insert(
            AlertType::ShardFailed,
            shard_failed_alert(now - Duration::days(10), now),
        );

        evaluate_alerts(
            &mut alerts,
            &state,
            now,
            AbandonmentTimestamps::default(),
            &config,
        );

        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @r#"
        {
          "shard_failed": {
            "count": 5,
            "error": "shard failed",
            "first_ts": "2025-05-22T12:00:00Z",
            "last_ts": "2025-06-01T12:00:00Z",
            "resolved_at": null,
            "spec_type": "capture",
            "state": "firing"
          }
        }
        "#);
    }

    #[test]
    fn recent_user_pub_suppresses_chronically_failing() {
        let config = ControllerConfig::default();
        let now = fixed_now();

        let state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        let mut alerts: Alerts = Default::default();
        alerts.insert(
            AlertType::ShardFailed,
            shard_failed_alert(now - Duration::days(35), now),
        );

        evaluate_alerts(
            &mut alerts,
            &state,
            now,
            AbandonmentTimestamps {
                last_user_pub_at: Some(now - Duration::days(5)),
                ..Default::default()
            },
            &config,
        );

        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @r#"
        {
          "shard_failed": {
            "count": 5,
            "error": "shard failed",
            "first_ts": "2025-04-27T12:00:00Z",
            "last_ts": "2025-06-01T12:00:00Z",
            "resolved_at": null,
            "spec_type": "capture",
            "state": "firing"
          }
        }
        "#);
    }

    #[test]
    fn stale_user_pub_does_not_suppress_chronically_failing() {
        let config = ControllerConfig::default();
        let now = fixed_now();

        let state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        let mut alerts: Alerts = Default::default();
        alerts.insert(
            AlertType::ShardFailed,
            shard_failed_alert(
                now - config.chronically_failing_threshold - Duration::days(5),
                now,
            ),
        );

        evaluate_alerts(
            &mut alerts,
            &state,
            now,
            AbandonmentTimestamps {
                last_user_pub_at: Some(now - Duration::days(20)),
                ..Default::default()
            },
            &config,
        );

        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @r#"
        {
          "shard_failed": {
            "count": 5,
            "error": "shard failed",
            "first_ts": "2025-04-27T12:00:00Z",
            "last_ts": "2025-06-01T12:00:00Z",
            "resolved_at": null,
            "spec_type": "capture",
            "state": "firing"
          },
          "task_chronically_failing": {
            "count": 1,
            "disable_at": "2025-06-08",
            "error": "task shards have been failing since 2025-04-27",
            "failing_since": "2025-04-27",
            "first_ts": "2025-06-01T12:00:00Z",
            "last_ts": null,
            "resolved_at": null,
            "spec_type": "capture",
            "state": "firing"
          }
        }
        "#);
    }

    /// A shard starts failing and stays broken. Over time the controller detects
    /// chronic failure, warns with a grace period, then auto-disables. When the
    /// shard later recovers and data starts moving again, all alerts resolve.
    #[test]
    fn chronically_failing_lifecycle() {
        let config = ControllerConfig::default();
        let shard_failed_at = fixed_now();
        let created_at = shard_failed_at - Duration::days(90);
        let state = mock_state(Some(enabled_capture()), created_at);
        let timestamps = AbandonmentTimestamps::default();
        let mut alerts: Alerts = Default::default();

        alerts.insert(
            AlertType::ShardFailed,
            shard_failed_alert(shard_failed_at, shard_failed_at),
        );

        // Tick 1: 29 days of failure, below the 30-day chronically-failing threshold.
        let now = shard_failed_at + config.chronically_failing_threshold - Duration::days(1);
        let reason = evaluate_alerts(&mut alerts, &state, now, timestamps, &config);
        assert!(reason.is_none());
        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @r#"
        {
          "shard_failed": {
            "count": 5,
            "error": "shard failed",
            "first_ts": "2025-06-01T12:00:00Z",
            "last_ts": "2025-06-01T12:00:00Z",
            "resolved_at": null,
            "spec_type": "capture",
            "state": "firing"
          }
        }
        "#);

        // Tick 2: 31 days of failure, over threshold. TaskChronicallyFailing fires
        // with a 7-day grace period before auto-disable.
        let now = shard_failed_at + config.chronically_failing_threshold + Duration::days(1);
        let reason = evaluate_alerts(&mut alerts, &state, now, timestamps, &config);
        assert!(reason.is_none());
        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @r#"
        {
          "shard_failed": {
            "count": 5,
            "error": "shard failed",
            "first_ts": "2025-06-01T12:00:00Z",
            "last_ts": "2025-06-01T12:00:00Z",
            "resolved_at": null,
            "spec_type": "capture",
            "state": "firing"
          },
          "task_chronically_failing": {
            "count": 1,
            "disable_at": "2025-07-09",
            "error": "task shards have been failing since 2025-06-01",
            "failing_since": "2025-06-01",
            "first_ts": "2025-07-02T12:00:00Z",
            "last_ts": null,
            "resolved_at": null,
            "spec_type": "capture",
            "state": "firing"
          }
        }
        "#);

        // Tick 3: 4 days into grace period, still within the window.
        let now = shard_failed_at + config.chronically_failing_threshold + Duration::days(5);
        let reason = evaluate_alerts(&mut alerts, &state, now, timestamps, &config);
        assert!(reason.is_none());
        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @r#"
        {
          "shard_failed": {
            "count": 5,
            "error": "shard failed",
            "first_ts": "2025-06-01T12:00:00Z",
            "last_ts": "2025-06-01T12:00:00Z",
            "resolved_at": null,
            "spec_type": "capture",
            "state": "firing"
          },
          "task_chronically_failing": {
            "count": 1,
            "disable_at": "2025-07-09",
            "error": "task shards have been failing since 2025-06-01",
            "failing_since": "2025-06-01",
            "first_ts": "2025-07-02T12:00:00Z",
            "last_ts": null,
            "resolved_at": null,
            "spec_type": "capture",
            "state": "firing"
          }
        }
        "#);

        // Tick 4: Grace period expired. The disable_at was set at tick 2
        // (threshold + 1d) plus DISABLE_AFTER, so we need to reach that date.
        let chronically_fired_at =
            shard_failed_at + config.chronically_failing_threshold + Duration::days(1);
        let now = chronically_fired_at + config.chronically_failing_disable_after;
        let reason = evaluate_alerts(&mut alerts, &state, now, timestamps, &config);
        assert_eq!(reason, Some(DisableReason::ChronicallyFailing));
        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @r#"
        {
          "shard_failed": {
            "count": 5,
            "error": "shard failed",
            "first_ts": "2025-06-01T12:00:00Z",
            "last_ts": "2025-06-01T12:00:00Z",
            "resolved_at": null,
            "spec_type": "capture",
            "state": "firing"
          },
          "task_auto_disabled_failing": {
            "count": 1,
            "error": "task auto-disabled after shards failing since 2025-06-01",
            "failing_since": "2025-06-01",
            "first_ts": "2025-07-09T12:00:00Z",
            "last_ts": null,
            "resolved_at": null,
            "spec_type": "capture",
            "state": "firing"
          },
          "task_chronically_failing": {
            "count": 1,
            "disable_at": "2025-07-09",
            "error": "task shards have been failing since 2025-06-01",
            "failing_since": "2025-06-01",
            "first_ts": "2025-07-02T12:00:00Z",
            "last_ts": null,
            "resolved_at": null,
            "spec_type": "capture",
            "state": "firing"
          }
        }
        "#);

        // Tick 5: Shard recovers (ShardFailed removed) and data starts moving
        // again. All failing-sequence alerts resolve cleanly.
        alerts.remove(&AlertType::ShardFailed);
        let now =
            chronically_fired_at + config.chronically_failing_disable_after + Duration::days(1);
        let reason = evaluate_alerts(
            &mut alerts,
            &state,
            now,
            AbandonmentTimestamps {
                last_data_movement_ts: Some(now - Duration::days(1)),
                ..Default::default()
            },
            &config,
        );
        assert!(reason.is_none());
        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @"{}");
    }

    /// Once `TaskChronicallyFailing` fires with a `disable_at` date, that stored
    /// date is honored on subsequent evaluations rather than being recomputed.
    /// This prevents the disable date from shifting if the grace-period config
    /// changes between controller runs.
    #[test]
    fn chronically_failing_stored_disable_at_honored() {
        let mut config = ControllerConfig::default();
        let base = fixed_now();
        let shard_failed_at = base - config.chronically_failing_threshold - Duration::days(15);
        let created_at = shard_failed_at - Duration::days(30);
        let state = mock_state(Some(enabled_capture()), created_at);
        let timestamps = AbandonmentTimestamps::default();
        let mut alerts: Alerts = Default::default();

        alerts.insert(
            AlertType::ShardFailed,
            shard_failed_alert(shard_failed_at, base),
        );

        // Tick 1: Grace period is 7 days. TaskChronicallyFailing fires with
        // disable_at = base + 7d.
        let reason = evaluate_alerts(&mut alerts, &state, base, timestamps, &config);
        assert!(reason.is_none());

        let original_disable_at = alerts
            .get(&AlertType::TaskChronicallyFailing)
            .expect("just fired")
            .extra
            .get(EXTRA_DISABLE_AT)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        // Shrink the grace period to 1 day. If disable_at were recomputed,
        // tick 2 would set it to now+1d and immediately trigger auto-disable
        // on tick 3.
        config.chronically_failing_disable_after = Duration::days(1);

        // Tick 2: 3 days later. Past the new 1-day grace period, but still
        // before the original 7-day disable_at. The stored date wins.
        let now = base + Duration::days(3);
        let reason = evaluate_alerts(&mut alerts, &state, now, timestamps, &config);
        assert!(reason.is_none());

        // Verify the stored disable_at hasn't changed.
        let current_disable_at = alerts
            .get(&AlertType::TaskChronicallyFailing)
            .unwrap()
            .extra
            .get(EXTRA_DISABLE_AT)
            .unwrap()
            .as_str()
            .unwrap();
        assert_eq!(original_disable_at, current_disable_at);
    }

    // Sequence 2: Idle tasks

    #[test]
    fn new_task_with_recent_pub_not_idle() {
        let config = ControllerConfig::default();
        let now = fixed_now();

        // The initial publication that created the task sets last_user_pub_at,
        // which suppresses idle detection for 14 days.
        let state = mock_state(Some(enabled_capture()), now - Duration::days(10));
        let mut alerts: Alerts = Default::default();

        evaluate_alerts(
            &mut alerts,
            &state,
            now,
            AbandonmentTimestamps {
                last_user_pub_at: Some(now - Duration::days(10)),
                ..Default::default()
            },
            &config,
        );

        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @"{}");
    }

    #[test]
    fn recent_data_movement_prevents_idle() {
        let config = ControllerConfig::default();
        let now = fixed_now();

        let state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        let mut alerts: Alerts = Default::default();

        evaluate_alerts(
            &mut alerts,
            &state,
            now,
            AbandonmentTimestamps {
                last_data_movement_ts: Some(now - Duration::days(5)),
                ..Default::default()
            },
            &config,
        );

        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @"{}");
    }

    #[test]
    fn recent_user_pub_prevents_idle() {
        let config = ControllerConfig::default();
        let now = fixed_now();

        let state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        let mut alerts: Alerts = Default::default();

        evaluate_alerts(
            &mut alerts,
            &state,
            now,
            AbandonmentTimestamps {
                last_user_pub_at: Some(now - Duration::days(5)),
                ..Default::default()
            },
            &config,
        );

        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @"{}");
    }

    #[test]
    fn stale_data_but_recent_user_pub_not_idle() {
        let config = ControllerConfig::default();
        let now = fixed_now();

        let state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        let mut alerts: Alerts = Default::default();

        evaluate_alerts(
            &mut alerts,
            &state,
            now,
            AbandonmentTimestamps {
                last_data_movement_ts: Some(
                    now - config.abandon_idle_threshold - Duration::days(5),
                ),
                last_user_pub_at: Some(now - Duration::days(3)),
            },
            &config,
        );

        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @"{}");
    }

    #[test]
    fn shard_failed_suppresses_idle() {
        let config = ControllerConfig::default();
        let now = fixed_now();

        let state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        let mut alerts: Alerts = Default::default();
        alerts.insert(
            AlertType::ShardFailed,
            shard_failed_alert(now - Duration::days(5), now),
        );

        evaluate_alerts(
            &mut alerts,
            &state,
            now,
            AbandonmentTimestamps::default(),
            &config,
        );

        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @r#"
        {
          "shard_failed": {
            "count": 5,
            "error": "shard failed",
            "first_ts": "2025-05-27T12:00:00Z",
            "last_ts": "2025-06-01T12:00:00Z",
            "resolved_at": null,
            "spec_type": "capture",
            "state": "firing"
          }
        }
        "#);
    }

    #[test]
    fn chronically_failing_suppresses_idle() {
        let config = ControllerConfig::default();
        let now = fixed_now();

        let state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        let mut alerts: Alerts = Default::default();
        alerts.insert(
            AlertType::ShardFailed,
            shard_failed_alert(now - Duration::days(40), now),
        );

        evaluate_alerts(
            &mut alerts,
            &state,
            now,
            AbandonmentTimestamps::default(),
            &config,
        );

        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @r#"
        {
          "shard_failed": {
            "count": 5,
            "error": "shard failed",
            "first_ts": "2025-04-22T12:00:00Z",
            "last_ts": "2025-06-01T12:00:00Z",
            "resolved_at": null,
            "spec_type": "capture",
            "state": "firing"
          },
          "task_chronically_failing": {
            "count": 1,
            "disable_at": "2025-06-08",
            "error": "task shards have been failing since 2025-04-22",
            "failing_since": "2025-04-22",
            "first_ts": "2025-06-01T12:00:00Z",
            "last_ts": null,
            "resolved_at": null,
            "spec_type": "capture",
            "state": "firing"
          }
        }
        "#);
    }

    /// Full idle lifecycle: task starts healthy, becomes idle, receives a warning
    /// with grace period, gets auto-disabled after the grace period, and all alerts
    /// resolve when data starts moving again.
    #[test]
    fn idle_lifecycle() {
        let config = ControllerConfig::default();
        let base = fixed_now();
        let created_at = base - Duration::days(90);
        let state = mock_state(Some(enabled_materialization()), created_at);
        let mut alerts: Alerts = Default::default();

        // Tick 1: Task has recent data movement. No alerts fire.
        let reason = evaluate_alerts(
            &mut alerts,
            &state,
            base,
            AbandonmentTimestamps {
                last_data_movement_ts: Some(base - Duration::days(1)),
                ..Default::default()
            },
            &config,
        );
        assert!(reason.is_none());
        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @"{}");

        // Tick 2: No data movement, no user pub, no failure alerts. TaskIdle fires
        // with a 7-day grace period.
        let now = base + config.abandon_idle_threshold;
        let reason = evaluate_alerts(
            &mut alerts,
            &state,
            now,
            AbandonmentTimestamps::default(),
            &config,
        );
        assert!(reason.is_none());
        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @r#"
        {
          "task_idle": {
            "count": 1,
            "disable_at": "2025-07-08",
            "error": "task has not moved data since it was created",
            "first_ts": "2025-07-01T12:00:00Z",
            "last_ts": null,
            "resolved_at": null,
            "spec_type": "materialization",
            "state": "firing"
          }
        }
        "#);

        // Tick 3: Grace period expired. TaskAutoDisabledIdle fires.
        let now = now + config.abandon_idle_disable_after;
        let reason = evaluate_alerts(
            &mut alerts,
            &state,
            now,
            AbandonmentTimestamps::default(),
            &config,
        );
        assert_eq!(reason, Some(DisableReason::Idle));
        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @r#"
        {
          "task_auto_disabled_idle": {
            "count": 1,
            "error": "task auto-disabled due to inactivity",
            "first_ts": "2025-07-08T12:00:00Z",
            "last_ts": null,
            "resolved_at": null,
            "spec_type": "materialization",
            "state": "firing"
          },
          "task_idle": {
            "count": 1,
            "disable_at": "2025-07-08",
            "error": "task has not moved data since it was created",
            "first_ts": "2025-07-01T12:00:00Z",
            "last_ts": null,
            "resolved_at": null,
            "spec_type": "materialization",
            "state": "firing"
          }
        }
        "#);

        // Tick 4: Data starts moving again. All idle alerts resolve.
        let now = now + Duration::days(1);
        let reason = evaluate_alerts(
            &mut alerts,
            &state,
            now,
            AbandonmentTimestamps {
                last_data_movement_ts: Some(now - Duration::days(1)),
                ..Default::default()
            },
            &config,
        );
        assert!(reason.is_none());
        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @"{}");
    }

    /// Once `TaskIdle` fires with a `disable_at` date, that stored date is
    /// honored on subsequent evaluations rather than being recomputed.
    #[test]
    fn idle_stored_disable_at_honored() {
        let mut config = ControllerConfig::default();
        let base = fixed_now();
        let created_at = base - Duration::days(90);
        let state = mock_state(Some(enabled_materialization()), created_at);
        let timestamps = AbandonmentTimestamps::default();
        let mut alerts: Alerts = Default::default();

        // Tick 1: Grace period is 7 days. TaskIdle fires with disable_at = base + 7d.
        let reason = evaluate_alerts(&mut alerts, &state, base, timestamps, &config);
        assert!(reason.is_none());

        let original_disable_at = alerts
            .get(&AlertType::TaskIdle)
            .expect("just fired")
            .extra
            .get(EXTRA_DISABLE_AT)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        // Shrink the grace period to 1 day. If disable_at were recomputed,
        // tick 2 would set it to now+1d and immediately trigger auto-disable
        // on tick 3.
        config.abandon_idle_disable_after = Duration::days(1);

        // Tick 2: 3 days later. Past the new 1-day grace period, but still
        // before the original 7-day disable_at. The stored date wins.
        let now = base + Duration::days(3);
        let reason = evaluate_alerts(&mut alerts, &state, now, timestamps, &config);
        assert!(reason.is_none());

        // Verify the stored disable_at hasn't changed.
        let current_disable_at = alerts
            .get(&AlertType::TaskIdle)
            .unwrap()
            .extra
            .get(EXTRA_DISABLE_AT)
            .unwrap()
            .as_str()
            .unwrap();
        assert_eq!(original_disable_at, current_disable_at);
    }

    /// When a task becomes disabled (no shards), all abandonment alerts resolve
    /// regardless of which sequence produced them.
    #[test]
    fn disabled_task_resolves_abandon_alerts() {
        let config = ControllerConfig::default();
        let base = fixed_now();
        let created_at = base - Duration::days(90);
        let state = mock_state(Some(enabled_capture()), created_at);
        let timestamps = AbandonmentTimestamps::default();
        let mut alerts: Alerts = Default::default();

        // Tick 1: Task is idle, no data, no user pub.
        let reason = evaluate_alerts(&mut alerts, &state, base, timestamps, &config);
        assert!(reason.is_none());
        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @r#"
        {
          "task_idle": {
            "count": 1,
            "disable_at": "2025-06-08",
            "error": "task has not moved data since it was created",
            "first_ts": "2025-06-01T12:00:00Z",
            "last_ts": null,
            "resolved_at": null,
            "spec_type": "capture",
            "state": "firing"
          }
        }
        "#);

        // Tick 2: Task is now disabled. All abandonment alerts resolve.
        let disabled_state = mock_state(Some(disabled_capture()), created_at);
        let now = base + Duration::days(1);
        let reason = evaluate_alerts(&mut alerts, &disabled_state, now, timestamps, &config);
        assert!(reason.is_none());
        insta::assert_json_snapshot!(serde_json::to_value(&alerts).unwrap(), @"{}");
    }

    #[test]
    fn set_spec_disabled_capture() {
        let mut spec = enabled_capture();
        set_spec_disabled(&mut spec);
        match &spec {
            AnySpec::Capture(c) => assert!(c.shards.disable),
            _ => panic!("expected capture"),
        }
    }

    #[test]
    fn set_spec_disabled_materialization() {
        let mut spec = enabled_materialization();
        set_spec_disabled(&mut spec);
        match &spec {
            AnySpec::Materialization(m) => assert!(m.shards.disable),
            _ => panic!("expected materialization"),
        }
    }

    #[test]
    fn set_spec_disabled_derivation() {
        let collection: models::CollectionDef = serde_json::from_value(serde_json::json!({
            "key": ["/id"],
            "schema": {"type": "object"},
            "derive": {
                "using": {"sqlite": {"migrations": []}},
                "transforms": []
            }
        }))
        .unwrap();
        let mut spec = AnySpec::Collection(collection);
        set_spec_disabled(&mut spec);
        match &spec {
            AnySpec::Collection(c) => {
                assert!(c.derive.as_ref().unwrap().shards.disable);
            }
            _ => panic!("expected collection"),
        }
    }
}
