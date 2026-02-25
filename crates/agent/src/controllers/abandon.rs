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
        if let Some(alert) = alerts_status.get_mut(&AlertType::TaskAbandoned) {
            alert.extra.insert(
                "disable_after_days".to_string(),
                serde_json::Value::from(ABANDONED_TASK_DISABLE_AFTER.num_days()),
            );
            // Pass the actual timestamp so the email can say "since <date>"
            // vs "since it was created" when PRIMARY was never observed.
            if let Some(ts) = activation.last_sustained_primary_ts {
                alert.extra.insert(
                    "last_primary_ts".to_string(),
                    serde_json::Value::from(ts.format("%Y-%m-%d").to_string()),
                );
            }
        }
    } else {
        alerts::resolve_alert(alerts_status, AlertType::TaskAbandoned);
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
    fn no_shards_not_evaluated() {
        let now = fixed_now();
        let state = mock_state(Some(disabled_capture()), now - Duration::days(30), None);
        let activation = ActivationStatus::default();
        let mut alerts: Alerts = Default::default();
        alerts.insert(AlertType::TaskAbandoned, firing_alert(now));

        let result = evaluate_abandoned(&mut alerts, &activation, &state, now);

        assert!(result.is_none());
        assert!(!alerts.contains_key(&AlertType::TaskAbandoned));
    }

    #[test]
    fn new_task_not_abandoned() {
        let now = fixed_now();
        let state = mock_state(Some(enabled_capture()), now - Duration::days(7), None);
        let activation = ActivationStatus::default();
        let mut alerts: Alerts = Default::default();

        let result = evaluate_abandoned(&mut alerts, &activation, &state, now);

        assert_eq!(result.unwrap().after_seconds, 24 * 60 * 60);
        assert!(!alerts.contains_key(&AlertType::TaskAbandoned));
    }

    #[test]
    fn old_task_no_signals_abandoned() {
        let now = fixed_now();
        let state = mock_state(Some(enabled_capture()), now - Duration::days(20), None);
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
        assert_eq!(alert.extra.get("disable_after_days"), Some(&serde_json::json!(7)));
        // last_sustained_primary_ts was None, so last_primary_ts should be absent
        assert_eq!(alert.extra.get("last_primary_ts"), None);
    }

    #[test]
    fn recent_primary_prevents_abandonment() {
        let now = fixed_now();
        let state = mock_state(Some(enabled_capture()), now - Duration::days(60), None);
        let activation = ActivationStatus {
            last_sustained_primary_ts: Some(now - Duration::days(5)),
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
            now - Duration::days(60),
            Some(now - Duration::days(3)),
        );
        let activation = ActivationStatus {
            last_sustained_primary_ts: Some(now - Duration::days(20)),
            ..Default::default()
        };
        let mut alerts: Alerts = Default::default();

        evaluate_abandoned(&mut alerts, &activation, &state, now);

        assert!(!alerts.contains_key(&AlertType::TaskAbandoned));
    }

    #[test]
    fn both_stale_signals_abandoned() {
        let now = fixed_now();
        let state = mock_state(
            Some(enabled_materialization()),
            now - Duration::days(60),
            Some(now - Duration::days(15)),
        );
        let activation = ActivationStatus {
            last_sustained_primary_ts: Some(now - Duration::days(20)),
            restarts_since_last_primary: 12,
            ..Default::default()
        };
        let mut alerts: Alerts = Default::default();

        evaluate_abandoned(&mut alerts, &activation, &state, now);

        let alert = alerts.get(&AlertType::TaskAbandoned).unwrap();
        assert_eq!(alert.state, AlertState::Firing);
        assert_eq!(alert.count, 12);
        assert_eq!(alert.spec_type, models::CatalogType::Materialization);
        // last_sustained_primary_ts was Some, so last_primary_ts should be present
        assert_eq!(
            alert.extra.get("last_primary_ts"),
            Some(&serde_json::json!("2025-05-12")),
        );
    }

    #[test]
    fn alert_resolves_on_recovery() {
        let now = fixed_now();
        let state = mock_state(Some(enabled_capture()), now - Duration::days(60), None);
        let activation = ActivationStatus {
            last_sustained_primary_ts: Some(now - Duration::days(2)),
            ..Default::default()
        };
        let mut alerts: Alerts = Default::default();
        alerts.insert(AlertType::TaskAbandoned, firing_alert(now));

        evaluate_abandoned(&mut alerts, &activation, &state, now);

        assert!(!alerts.contains_key(&AlertType::TaskAbandoned));
    }
}
