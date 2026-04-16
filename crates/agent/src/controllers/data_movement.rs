//! Per-task evaluation of the `DataMovementStalled` alert.
//!
//! The threshold comes from `alert_configs`
//! (`dataMovementStalled.threshold`). If no threshold is configured there,
//! evaluation falls back to `alert_data_processing`.

use super::{ControllerState, NextRun, alerts};
use crate::controlplane::ControlPlane;
use chrono::{DurationRound, TimeDelta};
use models::AnySpec;
use models::status::{AlertType, Alerts};
use serde_json::json;

const RECHECK_MINUTES: u32 = 60;

pub async fn evaluate_data_movement_stalled<C: ControlPlane>(
    alerts_status: &mut Alerts,
    state: &ControllerState,
    control_plane: &C,
    alert_cfg: Option<&models::AlertConfig>,
) -> anyhow::Result<Option<NextRun>> {
    let Some(spec) = state.live_spec.as_ref().filter(|s| !spec_is_disabled(s)) else {
        alerts::resolve_alert(alerts_status, AlertType::DataMovementStalled);
        return Ok(None);
    };

    if alert_cfg
        .and_then(|a| a.data_movement_stalled.as_ref())
        .and_then(|d| d.enabled)
        == Some(false)
    {
        alerts::resolve_alert(alerts_status, AlertType::DataMovementStalled);
        return Ok(Some(NextRun::after_minutes(RECHECK_MINUTES)));
    }

    let configured_threshold = alert_cfg
        .and_then(|a| a.data_movement_stalled.as_ref())
        .and_then(|d| d.threshold)
        .and_then(|d| chrono::Duration::from_std(d).ok());

    let threshold = match configured_threshold {
        Some(t) => t,
        None => {
            // TODO: remove once this fallback path is no longer needed.
            // Fall back to `alert_data_processing`.
            match control_plane
                .fetch_legacy_data_movement_stalled_threshold(state.catalog_name.clone())
                .await?
            {
                Some(t) => t,
                None => {
                    alerts::resolve_alert(alerts_status, AlertType::DataMovementStalled);
                    return Ok(None);
                }
            }
        }
    };

    let now = control_plane.current_time();

    // Specs younger than the configured threshold should not fire. The
    // hour-truncation on the anchor adds up to 59 min of slack, so a spec
    // that was just created (and therefore has no data movement yet) won't
    // immediately fire a false positive.
    let anchor = (now - threshold).duration_trunc(TimeDelta::hours(1))?;
    if state.created_at > anchor {
        alerts::resolve_alert(alerts_status, AlertType::DataMovementStalled);
        return Ok(Some(NextRun::after_minutes(RECHECK_MINUTES)));
    }

    let since = now - threshold;
    let bytes = control_plane
        .fetch_bytes_processed_since(state.catalog_name.clone(), since)
        .await?;

    if bytes == 0 {
        let catalog_type = spec.catalog_type();
        if !alerts_status.contains_key(&AlertType::DataMovementStalled) {
            alerts::set_alert_firing(
                alerts_status,
                AlertType::DataMovementStalled,
                now,
                format!(
                    "task has not moved data in the last {}",
                    humantime::format_duration(threshold.to_std().unwrap_or_default())
                ),
                1,
                catalog_type,
            );
            // Fields required by the notification template.
            let alert = alerts_status
                .get_mut(&AlertType::DataMovementStalled)
                .expect("just inserted");
            alert.extra.insert("bytes_processed".to_string(), json!(0));
            alert.extra.insert(
                "evaluation_interval".to_string(),
                json!(
                    humantime::format_duration(threshold.to_std().unwrap_or_default()).to_string()
                ),
            );
        }
        Ok(Some(NextRun::after_minutes(RECHECK_MINUTES)))
    } else {
        alerts::resolve_alert(alerts_status, AlertType::DataMovementStalled);
        Ok(Some(NextRun::after_minutes(RECHECK_MINUTES)))
    }
}

fn spec_is_disabled(spec: &AnySpec) -> bool {
    match spec {
        AnySpec::Capture(c) => c.shards.disable,
        AnySpec::Materialization(m) => m.shards.disable,
        AnySpec::Collection(c) => c.derive.as_ref().is_some_and(|d| d.shards.disable),
        AnySpec::Test(_) => true, // tests have no data movement
    }
}
