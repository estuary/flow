use super::{ControllerState, NextRun, alerts, env_duration};
use crate::controllers::activation::has_task_shards;
use chrono::{DateTime, Utc};
use models::status::{AlertType, Alerts, activation::ActivationStatus};

/// Duration without sustained PRIMARY before the TaskAbandoned alert fires.
static ABANDONED_TASK_THRESHOLD: std::sync::LazyLock<chrono::Duration> =
    std::sync::LazyLock::new(|| env_duration("ABANDONED_TASK_THRESHOLD", chrono::Duration::days(14)));

/// Duration after the TaskAbandoned alert fires before the task is automatically disabled.
static ABANDONED_TASK_DISABLE_AFTER: std::sync::LazyLock<chrono::Duration> =
    std::sync::LazyLock::new(|| env_duration("ABANDONED_TASK_DISABLE_AFTER", chrono::Duration::days(7)));

pub fn evaluate_abandoned(
    alerts_status: &mut Alerts,
    activation: &ActivationStatus,
    state: &ControllerState,
    now: DateTime<Utc>,
) -> Option<NextRun> {
    let Some(spec) = state.live_spec.as_ref().filter(|_| has_task_shards(state)) else {
        alerts::resolve_alert(alerts_status, AlertType::TaskAbandoned);
        alerts::resolve_alert(alerts_status, AlertType::TaskAutoDisabled);
        return None;
    };

    let cutoff_ts = now - *ABANDONED_TASK_THRESHOLD;

    // Fall back to created_at when sustained PRIMARY has never been observed,
    // so new tasks aren't flagged until they've existed for the full threshold.
    let last_primary = activation
        .last_sustained_primary_ts
        .unwrap_or(state.created_at);

    let is_abandoned =
        last_primary < cutoff_ts && state.last_connector_status_ts.map_or(true, |t| t < cutoff_ts);

    if is_abandoned {
        alerts::set_alert_firing(
            alerts_status,
            AlertType::TaskAbandoned,
            now,
            format!("task has had no sustained PRIMARY shard since {last_primary}"),
            activation.restarts_since_last_primary,
            spec.catalog_type(),
        );

        // Read back the alert's first_ts to compute the disable date and
        // check whether the grace period has expired.
        let first_ts = alerts_status
            .get(&AlertType::TaskAbandoned)
            .map(|a| a.first_ts)
            .unwrap_or(now);

        let disable_at = first_ts + *ABANDONED_TASK_DISABLE_AFTER;
        let last_primary_str = activation
            .last_sustained_primary_ts
            .map(|ts| ts.format("%Y-%m-%d").to_string());

        if let Some(alert) = alerts_status.get_mut(&AlertType::TaskAbandoned) {
            alert.extra.insert(
                "disable_at".to_string(),
                serde_json::Value::from(disable_at.format("%Y-%m-%d").to_string()),
            );
            if let Some(ref ts_str) = last_primary_str {
                alert.extra.insert(
                    "last_primary_ts".to_string(),
                    serde_json::Value::from(ts_str.clone()),
                );
            }
        }

        // Once the grace period expires, fire the auto-disable notification
        // and disable the task.
        if now >= disable_at {
            alerts::set_alert_firing(
                alerts_status,
                AlertType::TaskAutoDisabled,
                now,
                format!("task auto-disabled after being abandoned since {last_primary}"),
                0,
                spec.catalog_type(),
            );
            if let Some(ref ts_str) = last_primary_str {
                if let Some(alert) = alerts_status.get_mut(&AlertType::TaskAutoDisabled) {
                    alert.extra.insert(
                        "last_primary_ts".to_string(),
                        serde_json::Value::from(ts_str.clone()),
                    );
                }
            }
            // TODO: actually disable the task by publishing with shards.disable = true.
        }
    } else {
        alerts::resolve_alert(alerts_status, AlertType::TaskAbandoned);
        alerts::resolve_alert(alerts_status, AlertType::TaskAutoDisabled);
    }

    // Schedule next evaluation in ~24h. The controller will wake at
    // the soonest of all stage next-runs, so this just ensures we
    // re-evaluate at least daily.
    Some(NextRun::after_minutes(24 * 60))
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

    fn mock_state(
        live_spec: Option<AnySpec>,
        created_at: DateTime<Utc>,
        last_connector_status_ts: Option<DateTime<Utc>>,
    ) -> ControllerState {
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
            last_connector_status_ts,
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

    fn firing_alert(now: DateTime<Utc>) -> ControllerAlert {
        ControllerAlert {
            state: AlertState::Firing,
            spec_type: models::CatalogType::Capture,
            first_ts: now - Duration::days(1),
            last_ts: Some(now - Duration::hours(1)),
            error: "old alert".to_string(),
            count: 3,
            resolved_at: None,
            extra: Default::default(),
        }
    }


    #[test]
    fn no_shards_resolves_both_alerts() {
        let now = fixed_now();
        let state = mock_state(Some(disabled_capture()), now - *ABANDONED_TASK_THRESHOLD * 2, None);
        let activation = ActivationStatus::default();
        let mut alerts: Alerts = Default::default();
        alerts.insert(AlertType::TaskAbandoned, firing_alert(now));
        alerts.insert(AlertType::TaskAutoDisabled, firing_alert(now));

        let result = evaluate_abandoned(&mut alerts, &activation, &state, now);

        assert!(result.is_none());
        assert!(!alerts.contains_key(&AlertType::TaskAbandoned));
        assert!(!alerts.contains_key(&AlertType::TaskAutoDisabled));
    }

    #[test]
    fn new_task_not_abandoned() {
        let now = fixed_now();
        let state = mock_state(Some(enabled_capture()), now - *ABANDONED_TASK_THRESHOLD / 2, None);
        let activation = ActivationStatus::default();
        let mut alerts: Alerts = Default::default();

        let result = evaluate_abandoned(&mut alerts, &activation, &state, now);

        assert_eq!(result.unwrap().after_seconds, 24 * 60 * 60);
        assert!(!alerts.contains_key(&AlertType::TaskAbandoned));
    }

    #[test]
    fn old_task_no_signals_abandoned() {
        let now = fixed_now();
        let state = mock_state(Some(enabled_capture()), now - *ABANDONED_TASK_THRESHOLD - Duration::days(6), None);
        let activation = ActivationStatus {
            restarts_since_last_primary: 5,
            ..Default::default()
        };
        let mut alerts: Alerts = Default::default();

        evaluate_abandoned(&mut alerts, &activation, &state, now);

        let alert = alerts.get(&AlertType::TaskAbandoned).unwrap();
        assert_eq!(alert.state, AlertState::Firing);
        assert_eq!(alert.count, 5);
        assert_eq!(alert.spec_type, models::CatalogType::Capture);
        assert!(alert.error.contains("no sustained PRIMARY shard since"));
        // disable_at should be first_ts + grace period
        let expect_disable_at = (now + *ABANDONED_TASK_DISABLE_AFTER).format("%Y-%m-%d").to_string();
        assert_eq!(
            alert.extra.get("disable_at"),
            Some(&serde_json::json!(expect_disable_at)),
        );
        // last_sustained_primary_ts was None, so last_primary_ts should be absent
        assert_eq!(alert.extra.get("last_primary_ts"), None);
        // Grace period just started, so auto-disable should not fire yet
        assert!(!alerts.contains_key(&AlertType::TaskAutoDisabled));
    }

    #[test]
    fn recent_primary_prevents_abandonment() {
        let now = fixed_now();
        let state = mock_state(Some(enabled_capture()), now - *ABANDONED_TASK_THRESHOLD * 4, None);
        let activation = ActivationStatus {
            last_sustained_primary_ts: Some(now - *ABANDONED_TASK_THRESHOLD / 3),
            ..Default::default()
        };
        let mut alerts: Alerts = Default::default();

        evaluate_abandoned(&mut alerts, &activation, &state, now);

        assert!(!alerts.contains_key(&AlertType::TaskAbandoned));
    }

    #[test]
    fn recent_connector_status_prevents_abandonment() {
        let now = fixed_now();
        let state = mock_state(
            Some(enabled_capture()),
            now - *ABANDONED_TASK_THRESHOLD * 4,
            Some(now - *ABANDONED_TASK_THRESHOLD / 4),
        );
        let activation = ActivationStatus {
            last_sustained_primary_ts: Some(now - *ABANDONED_TASK_THRESHOLD - Duration::days(6)),
            ..Default::default()
        };
        let mut alerts: Alerts = Default::default();

        evaluate_abandoned(&mut alerts, &activation, &state, now);

        assert!(!alerts.contains_key(&AlertType::TaskAbandoned));
    }

    #[test]
    fn both_stale_signals_abandoned() {
        let now = fixed_now();
        let primary_age = *ABANDONED_TASK_THRESHOLD + Duration::days(6);
        let state = mock_state(
            Some(enabled_materialization()),
            now - *ABANDONED_TASK_THRESHOLD * 4,
            Some(now - *ABANDONED_TASK_THRESHOLD - Duration::days(1)),
        );
        let activation = ActivationStatus {
            last_sustained_primary_ts: Some(now - primary_age),
            restarts_since_last_primary: 12,
            ..Default::default()
        };
        let mut alerts: Alerts = Default::default();

        evaluate_abandoned(&mut alerts, &activation, &state, now);

        let alert = alerts.get(&AlertType::TaskAbandoned).unwrap();
        assert_eq!(alert.state, AlertState::Firing);
        assert_eq!(alert.count, 12);
        assert_eq!(alert.spec_type, models::CatalogType::Materialization);
        let expect_date = (now - primary_age).format("%Y-%m-%d").to_string();
        assert_eq!(
            alert.extra.get("last_primary_ts"),
            Some(&serde_json::json!(expect_date)),
        );
    }

    #[test]
    fn grace_period_expired_fires_auto_disable() {
        let now = fixed_now();
        let state = mock_state(Some(enabled_capture()), now - *ABANDONED_TASK_THRESHOLD * 2, None);
        let activation = ActivationStatus {
            restarts_since_last_primary: 8,
            ..Default::default()
        };
        let mut alerts: Alerts = Default::default();
        let mut abandoned_alert = firing_alert(now);
        abandoned_alert.first_ts = now - *ABANDONED_TASK_DISABLE_AFTER - Duration::days(3);
        alerts.insert(AlertType::TaskAbandoned, abandoned_alert);

        evaluate_abandoned(&mut alerts, &activation, &state, now);

        assert!(alerts.contains_key(&AlertType::TaskAbandoned));
        let auto_disabled = alerts.get(&AlertType::TaskAutoDisabled).unwrap();
        assert_eq!(auto_disabled.state, AlertState::Firing);
        assert_eq!(auto_disabled.spec_type, models::CatalogType::Capture);
        assert!(auto_disabled.error.contains("auto-disabled"));
    }

    #[test]
    fn grace_period_not_expired_no_auto_disable() {
        let now = fixed_now();
        let state = mock_state(Some(enabled_capture()), now - *ABANDONED_TASK_THRESHOLD - Duration::days(6), None);
        let activation = ActivationStatus::default();
        let mut alerts: Alerts = Default::default();
        let mut abandoned_alert = firing_alert(now);
        abandoned_alert.first_ts = now - *ABANDONED_TASK_DISABLE_AFTER / 2;
        alerts.insert(AlertType::TaskAbandoned, abandoned_alert);

        evaluate_abandoned(&mut alerts, &activation, &state, now);

        assert!(alerts.contains_key(&AlertType::TaskAbandoned));
        assert!(!alerts.contains_key(&AlertType::TaskAutoDisabled));
    }

    #[test]
    fn recovery_resolves_both_alerts() {
        let now = fixed_now();
        let state = mock_state(Some(enabled_capture()), now - *ABANDONED_TASK_THRESHOLD * 4, None);
        let activation = ActivationStatus {
            last_sustained_primary_ts: Some(now - Duration::days(2)),
            ..Default::default()
        };
        let mut alerts: Alerts = Default::default();
        alerts.insert(AlertType::TaskAbandoned, firing_alert(now));
        alerts.insert(AlertType::TaskAutoDisabled, firing_alert(now));

        evaluate_abandoned(&mut alerts, &activation, &state, now);

        assert!(!alerts.contains_key(&AlertType::TaskAbandoned));
        assert!(!alerts.contains_key(&AlertType::TaskAutoDisabled));
    }
}
