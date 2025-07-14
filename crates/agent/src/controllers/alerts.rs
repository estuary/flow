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
        existing.count = count;
        existing.spec_type = spec_type;
        existing.error = error;
    } else {
        tracing::info!(%alert_type, %spec_type, "alert started firing");
        statuses.insert(
            alert_type,
            ControllerAlert {
                state: AlertState::Firing,
                first_ts: now,
                last_ts: None,
                error,
                count,
                spec_type,
                resolved_at: None,
                extra: HashMap::new(),
            },
        );
    }
}

/// Clears the given alert entirely, removing it from `statuses`.
///
/// Note: The `alert_history` schema allows for having different arguments when an
/// alert is resolved vs when it starts firing, so that different information
/// can be passed into the alert resolution email. So far, I haven't actually
/// seen a use case for doing that with any of the alerts we have so far.
/// I tried to leave open the possibility that we could use that portion of the
/// API if we needed to, but didn't want to add the complexity to the controller
/// code unless it actually became necessary. If we end up needing to provide
/// separate `resolved_arguments` for an alert, that can be done by leaving the
/// alert status in place and updating the `state` to `Resolved` (and then scheduling
/// a subsequent removal of the alert from `statuses` some time in the future).
pub fn resolve_alert(statuses: &mut Alerts, alert_type: AlertType) {
    if let Some(_cleared) = statuses.remove(&alert_type) {
        tracing::info!(%alert_type, "alert resolved");
    }
}
