use std::collections::HashMap;

use chrono::{DateTime, Utc};
use models::status::{AlertState, AlertType, Alerts, ControllerAlert};

pub fn set_alert_firing(
    statuses: &mut Alerts,
    alert_type: AlertType,
    now: DateTime<Utc>,
    error: String,
    count: u32,
    spec_type: models::CatalogType,
) {
    // If the current status is for a resolved alert of the same type, remove
    // that entry and re-start with a fresh alert.
    if statuses
        .get(&alert_type)
        .is_some_and(|s| s.state == AlertState::Resolved)
    {
        statuses.remove(&alert_type);
    }

    if let Some(existing) = statuses.get_mut(&alert_type) {
        existing.last_ts = Some(now);
        let prev_error = std::mem::take(&mut existing.error);
        existing
            .extra
            .insert("prev_error".to_owned(), prev_error.into());
        existing.count = count;
        existing.spec_type = spec_type;
    } else {
        tracing::info!(%alert_type, "alert started firing");
        statuses.insert(
            alert_type,
            ControllerAlert {
                state: AlertState::Firing,
                first_ts: now,
                last_ts: None,
                error: error.to_string(),
                count,
                spec_type,
                resolved_at: None,
                extra: HashMap::new(),
            },
        );
    }
}

/// Clears the given alert entirely, removing it from `statuses`.
/// This does not allow for the presence of separate `resolved_arguments`.
/// If separate `resovlved_arguments` are needed, then we'll have to
pub fn resolve_alert(statuses: &mut Alerts, alert_type: AlertType) {
    if let Some(_cleared) = statuses.remove(&alert_type) {
        tracing::info!(%alert_type, "alert resolved");
    }
}
