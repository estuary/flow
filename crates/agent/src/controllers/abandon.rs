use super::{ControllerState, NextRun, alerts, env_duration};
use crate::controllers::activation::has_task_shards;
use chrono::{DateTime, Utc};
use models::status::{AlertType, Alerts};

/// ShardFailed must be continuously firing for this long before we consider the task chronically failing.
static CHRONICALLY_FAILING_THRESHOLD: std::sync::LazyLock<chrono::Duration> =
    std::sync::LazyLock::new(|| {
        env_duration(
            "CHRONICALLY_FAILING_THRESHOLD",
            chrono::Duration::days(30),
        )
    });

/// Grace period after firing the chronically-failing warning before auto-disabling.
static CHRONICALLY_FAILING_DISABLE_AFTER: std::sync::LazyLock<chrono::Duration> =
    std::sync::LazyLock::new(|| {
        env_duration(
            "CHRONICALLY_FAILING_DISABLE_AFTER",
            chrono::Duration::days(7),
        )
    });

/// No data movement for this long triggers the idle detection.
static IDLE_THRESHOLD: std::sync::LazyLock<chrono::Duration> =
    std::sync::LazyLock::new(|| env_duration("IDLE_THRESHOLD", chrono::Duration::days(30)));

/// No user publication for this long is required for idle detection.
static USER_PUB_THRESHOLD: std::sync::LazyLock<chrono::Duration> =
    std::sync::LazyLock::new(|| env_duration("USER_PUB_THRESHOLD", chrono::Duration::days(14)));

/// Grace period after firing the idle warning before auto-disabling.
static IDLE_DISABLE_AFTER: std::sync::LazyLock<chrono::Duration> =
    std::sync::LazyLock::new(|| env_duration("IDLE_DISABLE_AFTER", chrono::Duration::days(7)));

pub fn evaluate_abandoned(
    alerts_status: &mut Alerts,
    state: &ControllerState,
    now: DateTime<Utc>,
) -> Option<NextRun> {
    // Tasks that are disabled, Dekaf, or lack shards don't get abandonment checks.
    // Silently resolve any alerts that may have been firing so re-enablement starts clean.
    let Some(spec) = state.live_spec.as_ref().filter(|_| has_task_shards(state)) else {
        resolve_all_abandon_alerts(alerts_status);
        return None;
    };

    let catalog_type = spec.catalog_type();

    // Sequence 1: Chronically Failing
    // Fires when ShardFailed has been continuously active for > CHRONICALLY_FAILING_THRESHOLD.
    let shard_failed_since = alerts_status
        .get(&AlertType::ShardFailed)
        .map(|a| a.first_ts);

    let is_chronically_failing = shard_failed_since
        .is_some_and(|first_ts| (now - first_ts) >= *CHRONICALLY_FAILING_THRESHOLD);

    if is_chronically_failing {
        let shard_failed_first_ts = shard_failed_since.unwrap();

        alerts::set_alert_firing(
            alerts_status,
            AlertType::TaskChronicallyFailing,
            now,
            format!(
                "task shards have been failing since {}",
                shard_failed_first_ts.format("%Y-%m-%d")
            ),
            0,
            catalog_type,
        );

        let first_ts = alerts_status
            .get(&AlertType::TaskChronicallyFailing)
            .map(|a| a.first_ts)
            .unwrap_or(now);
        let disable_at = first_ts + *CHRONICALLY_FAILING_DISABLE_AFTER;

        if let Some(alert) = alerts_status.get_mut(&AlertType::TaskChronicallyFailing) {
            alert.extra.insert(
                "disable_at".to_string(),
                serde_json::Value::from(disable_at.format("%Y-%m-%d").to_string()),
            );
        }

        if now >= disable_at {
            alerts::set_alert_firing(
                alerts_status,
                AlertType::TaskAutoDisabledFailing,
                now,
                format!(
                    "task auto-disabled after shards failing since {}",
                    shard_failed_first_ts.format("%Y-%m-%d")
                ),
                0,
                catalog_type,
            );
            // TODO: publish with shards.disable = true
        }
    } else {
        alerts::resolve_alert(alerts_status, AlertType::TaskChronicallyFailing);
        alerts::resolve_alert(alerts_status, AlertType::TaskAutoDisabledFailing);
    }

    // Sequence 2: Idle Task
    // Fires when no data has moved AND no user publication for extended periods.
    // Suppressed when ShardFailed or TaskChronicallyFailing is active (avoid duplicate emails).
    let has_failure_alerts = alerts_status.contains_key(&AlertType::ShardFailed)
        || alerts_status.contains_key(&AlertType::TaskChronicallyFailing);

    let data_stale = state
        .last_data_movement_ts
        .map_or(true, |ts| (now - ts) >= *IDLE_THRESHOLD);
    let user_pub_stale = state
        .last_user_pub_at
        .map_or(true, |ts| (now - ts) >= *USER_PUB_THRESHOLD);
    let old_enough = (now - state.created_at) >= *IDLE_THRESHOLD;

    let is_idle = !has_failure_alerts && data_stale && user_pub_stale && old_enough;

    if is_idle {
        alerts::set_alert_firing(
            alerts_status,
            AlertType::TaskIdle,
            now,
            format!(
                "task has not moved data{}",
                state
                    .last_data_movement_ts
                    .map(|ts| format!(" since {}", ts.format("%Y-%m-%d")))
                    .unwrap_or_else(|| " since it was created".to_string())
            ),
            0,
            catalog_type,
        );

        let first_ts = alerts_status
            .get(&AlertType::TaskIdle)
            .map(|a| a.first_ts)
            .unwrap_or(now);
        let disable_at = first_ts + *IDLE_DISABLE_AFTER;

        if let Some(alert) = alerts_status.get_mut(&AlertType::TaskIdle) {
            alert.extra.insert(
                "disable_at".to_string(),
                serde_json::Value::from(disable_at.format("%Y-%m-%d").to_string()),
            );
        }

        if now >= disable_at {
            alerts::set_alert_firing(
                alerts_status,
                AlertType::TaskAutoDisabledIdle,
                now,
                "task auto-disabled due to inactivity".to_string(),
                0,
                catalog_type,
            );
            // TODO: publish with shards.disable = true
        }
    } else {
        alerts::resolve_alert(alerts_status, AlertType::TaskIdle);
        alerts::resolve_alert(alerts_status, AlertType::TaskAutoDisabledIdle);
    }

    Some(NextRun::after_minutes(24 * 60))
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
            last_connector_status_ts: None,
            last_data_movement_ts: None,
            last_user_pub_at: None,
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

    fn shard_failed_alert(first_ts: DateTime<Utc>, now: DateTime<Utc>) -> ControllerAlert {
        ControllerAlert {
            state: AlertState::Firing,
            spec_type: models::CatalogType::Capture,
            first_ts,
            last_ts: Some(now),
            error: "shard failed".to_string(),
            count: 5,
            resolved_at: None,
            extra: Default::default(),
        }
    }

    fn firing_alert(first_ts: DateTime<Utc>) -> ControllerAlert {
        ControllerAlert {
            state: AlertState::Firing,
            spec_type: models::CatalogType::Capture,
            first_ts,
            last_ts: None,
            error: "test alert".to_string(),
            count: 0,
            resolved_at: None,
            extra: Default::default(),
        }
    }

    // -- Sequence 1: Chronically Failing --

    #[test]
    fn no_shards_resolves_all_alerts() {
        let now = fixed_now();
        let state = mock_state(Some(disabled_capture()), now - Duration::days(60));
        let mut alerts: Alerts = Default::default();
        alerts.insert(AlertType::TaskChronicallyFailing, firing_alert(now));
        alerts.insert(AlertType::TaskAutoDisabledFailing, firing_alert(now));
        alerts.insert(AlertType::TaskIdle, firing_alert(now));
        alerts.insert(AlertType::TaskAutoDisabledIdle, firing_alert(now));

        let result = evaluate_abandoned(&mut alerts, &state, now);

        assert!(result.is_none());
        assert!(alerts.is_empty());
    }

    #[test]
    fn no_shard_failed_means_not_chronically_failing() {
        let now = fixed_now();
        let state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        let mut alerts: Alerts = Default::default();

        evaluate_abandoned(&mut alerts, &state, now);

        assert!(!alerts.contains_key(&AlertType::TaskChronicallyFailing));
    }

    #[test]
    fn recent_shard_failed_not_chronically_failing() {
        let now = fixed_now();
        let state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        let mut alerts: Alerts = Default::default();
        // ShardFailed started 10 days ago (under the 30-day threshold)
        alerts.insert(
            AlertType::ShardFailed,
            shard_failed_alert(now - Duration::days(10), now),
        );

        evaluate_abandoned(&mut alerts, &state, now);

        assert!(!alerts.contains_key(&AlertType::TaskChronicallyFailing));
    }

    #[test]
    fn shard_failed_over_threshold_fires_chronically_failing() {
        let now = fixed_now();
        let state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        let mut alerts: Alerts = Default::default();
        alerts.insert(
            AlertType::ShardFailed,
            shard_failed_alert(now - *CHRONICALLY_FAILING_THRESHOLD - Duration::days(1), now),
        );

        evaluate_abandoned(&mut alerts, &state, now);

        let alert = alerts.get(&AlertType::TaskChronicallyFailing).unwrap();
        assert_eq!(alert.state, AlertState::Firing);
        assert!(alert.error.contains("failing since"));
        assert!(alert.extra.contains_key("disable_at"));
        // Grace period just started, auto-disable should not fire yet
        assert!(!alerts.contains_key(&AlertType::TaskAutoDisabledFailing));
    }

    #[test]
    fn chronically_failing_grace_period_expired_fires_auto_disable() {
        let now = fixed_now();
        let state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        let mut alerts: Alerts = Default::default();
        alerts.insert(
            AlertType::ShardFailed,
            shard_failed_alert(now - Duration::days(45), now),
        );
        // TaskChronicallyFailing has been firing for longer than the grace period
        alerts.insert(
            AlertType::TaskChronicallyFailing,
            firing_alert(now - *CHRONICALLY_FAILING_DISABLE_AFTER - Duration::days(1)),
        );

        evaluate_abandoned(&mut alerts, &state, now);

        assert!(alerts.contains_key(&AlertType::TaskChronicallyFailing));
        let auto_disabled = alerts.get(&AlertType::TaskAutoDisabledFailing).unwrap();
        assert_eq!(auto_disabled.state, AlertState::Firing);
        assert!(auto_disabled.error.contains("auto-disabled"));
    }

    #[test]
    fn shard_failed_resolves_clears_chronically_failing() {
        let now = fixed_now();
        let state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        let mut alerts: Alerts = Default::default();
        // No ShardFailed alert (it resolved)
        alerts.insert(AlertType::TaskChronicallyFailing, firing_alert(now - Duration::days(5)));
        alerts.insert(AlertType::TaskAutoDisabledFailing, firing_alert(now - Duration::days(1)));

        evaluate_abandoned(&mut alerts, &state, now);

        assert!(!alerts.contains_key(&AlertType::TaskChronicallyFailing));
        assert!(!alerts.contains_key(&AlertType::TaskAutoDisabledFailing));
    }

    // -- Sequence 2: Idle --

    #[test]
    fn new_task_not_idle() {
        let now = fixed_now();
        // Created recently (under IDLE_THRESHOLD)
        let state = mock_state(Some(enabled_capture()), now - Duration::days(10));
        let mut alerts: Alerts = Default::default();

        evaluate_abandoned(&mut alerts, &state, now);

        assert!(!alerts.contains_key(&AlertType::TaskIdle));
    }

    #[test]
    fn old_task_no_data_no_user_pub_is_idle() {
        let now = fixed_now();
        let mut state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        state.last_data_movement_ts = None;
        state.last_user_pub_at = None;
        let mut alerts: Alerts = Default::default();

        evaluate_abandoned(&mut alerts, &state, now);

        let alert = alerts.get(&AlertType::TaskIdle).unwrap();
        assert_eq!(alert.state, AlertState::Firing);
        assert!(alert.error.contains("has not moved data"));
        assert!(alert.extra.contains_key("disable_at"));
    }

    #[test]
    fn recent_data_movement_prevents_idle() {
        let now = fixed_now();
        let mut state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        state.last_data_movement_ts = Some(now - Duration::days(5));
        state.last_user_pub_at = None;
        let mut alerts: Alerts = Default::default();

        evaluate_abandoned(&mut alerts, &state, now);

        assert!(!alerts.contains_key(&AlertType::TaskIdle));
    }

    #[test]
    fn recent_user_pub_prevents_idle() {
        let now = fixed_now();
        let mut state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        state.last_data_movement_ts = None;
        state.last_user_pub_at = Some(now - Duration::days(5));
        let mut alerts: Alerts = Default::default();

        evaluate_abandoned(&mut alerts, &state, now);

        assert!(!alerts.contains_key(&AlertType::TaskIdle));
    }

    #[test]
    fn shard_failed_suppresses_idle() {
        let now = fixed_now();
        let mut state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        state.last_data_movement_ts = None;
        state.last_user_pub_at = None;
        let mut alerts: Alerts = Default::default();
        // ShardFailed is active, so idle detection should be suppressed
        alerts.insert(
            AlertType::ShardFailed,
            shard_failed_alert(now - Duration::days(5), now),
        );

        evaluate_abandoned(&mut alerts, &state, now);

        assert!(!alerts.contains_key(&AlertType::TaskIdle));
    }

    #[test]
    fn chronically_failing_suppresses_idle() {
        let now = fixed_now();
        let mut state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        state.last_data_movement_ts = None;
        state.last_user_pub_at = None;
        let mut alerts: Alerts = Default::default();
        alerts.insert(
            AlertType::ShardFailed,
            shard_failed_alert(now - Duration::days(40), now),
        );
        // TaskChronicallyFailing is set by the first part of evaluate_abandoned

        evaluate_abandoned(&mut alerts, &state, now);

        // TaskChronicallyFailing should fire (ShardFailed > 30 days)
        assert!(alerts.contains_key(&AlertType::TaskChronicallyFailing));
        // But TaskIdle should NOT fire (suppressed by ShardFailed/ChronicallyFailing)
        assert!(!alerts.contains_key(&AlertType::TaskIdle));
    }

    #[test]
    fn idle_grace_period_expired_fires_auto_disable() {
        let now = fixed_now();
        let mut state = mock_state(Some(enabled_materialization()), now - Duration::days(60));
        state.last_data_movement_ts = None;
        state.last_user_pub_at = None;
        let mut alerts: Alerts = Default::default();
        // TaskIdle has been firing for longer than the grace period
        alerts.insert(
            AlertType::TaskIdle,
            firing_alert(now - *IDLE_DISABLE_AFTER - Duration::days(1)),
        );

        evaluate_abandoned(&mut alerts, &state, now);

        assert!(alerts.contains_key(&AlertType::TaskIdle));
        let auto_disabled = alerts.get(&AlertType::TaskAutoDisabledIdle).unwrap();
        assert_eq!(auto_disabled.state, AlertState::Firing);
        assert!(auto_disabled.error.contains("auto-disabled"));
    }

    #[test]
    fn idle_recovery_resolves_alerts() {
        let now = fixed_now();
        let mut state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        // Data moved recently, recovering from idle
        state.last_data_movement_ts = Some(now - Duration::days(5));
        let mut alerts: Alerts = Default::default();
        alerts.insert(AlertType::TaskIdle, firing_alert(now - Duration::days(10)));
        alerts.insert(AlertType::TaskAutoDisabledIdle, firing_alert(now - Duration::days(1)));

        evaluate_abandoned(&mut alerts, &state, now);

        assert!(!alerts.contains_key(&AlertType::TaskIdle));
        assert!(!alerts.contains_key(&AlertType::TaskAutoDisabledIdle));
    }

    #[test]
    fn stale_data_but_recent_user_pub_not_idle() {
        let now = fixed_now();
        let mut state = mock_state(Some(enabled_capture()), now - Duration::days(60));
        state.last_data_movement_ts = Some(now - *IDLE_THRESHOLD - Duration::days(5));
        state.last_user_pub_at = Some(now - Duration::days(3));
        let mut alerts: Alerts = Default::default();

        evaluate_abandoned(&mut alerts, &state, now);

        assert!(!alerts.contains_key(&AlertType::TaskIdle));
    }
}
