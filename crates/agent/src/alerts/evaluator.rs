use std::collections::HashMap;

use chrono::{DateTime, Utc};
use control_plane_api::alerts::{Alert, AlertAction, ArgsObject, FireAlert, ResolveAlert};
use models::status::AlertType;

/// This function inspects an alert view and compares it against the given
/// array of `open_alerts`, returning a list of alert state transitions.
/// Alerts only have two possible state transitions:
///
/// - Alerts that `current_alert_state` shows as firing, but are _not_
///   already included in `open_alerts` will result in an `AlertAction::Fire`.
/// - Alerts that are in `open_alerts`, but which are _not_ shown to be firing
///   in `current_alert_state`, will result in `AlertAction::Resolve`.
///
/// Normally, it's sufficient for the `current_alert_state` to simply omit any
/// alerts that have resolved. But it's also possible to include the alert
/// with `firing: false`, in which case any `base_arguments` will become the
/// `resolved_arguments` in alert history.
pub fn evaluate_alert_actions(
    eval_timestamp: DateTime<Utc>,
    id_gen: &mut models::IdGenerator,
    open_alerts: Vec<Alert>,
    current_alert_state: Vec<AlertViewRow>,
) -> Vec<AlertAction> {
    let mut indexed_open: HashMap<(String, AlertType), (models::Id, DateTime<Utc>, ArgsObject)> =
        open_alerts
            .into_iter()
            .map(|alert| {
                let Alert {
                    id,
                    catalog_name,
                    alert_type,
                    fired_at,
                    resolved_at: _,
                    arguments,
                    resolved_arguments: _,
                } = alert;

                let key = (catalog_name, alert_type);
                let value = (id, fired_at, arguments.0);
                (key, value)
            })
            .collect();

    let mut actions = Vec::new();
    for view_alert in current_alert_state {
        let AlertViewRow {
            catalog_name,
            alert_type,
            base_arguments: arguments,
            firing,
        } = view_alert;
        let key = (catalog_name, alert_type);

        if let Some((_open_key, (id, _fired_at, _open_args))) = indexed_open.remove_entry(&key) {
            if !firing {
                // The alert is currently open, but the view now shows it as resolved
                let (catalog_name, alert_type) = key;
                actions.push(AlertAction::Resolve(ResolveAlert {
                    id,
                    catalog_name,
                    alert_type,
                    resolved_at: eval_timestamp,
                    base_resolved_arguments: arguments.map(|j| j.0),
                }));
            }
        } else {
            // The alert is not currently open, so if the view shows it as
            // firing, then we need to fire the alert now.
            if firing {
                let (catalog_name, alert_type) = key;
                let id = id_gen.next();
                actions.push(AlertAction::Fire(FireAlert {
                    id,
                    catalog_name,
                    alert_type,
                    fired_at: eval_timestamp,
                    base_arguments: arguments.map(|j| j.0).unwrap_or_default(),
                }));
            }
        }
    }

    // Any open alerts remaining in `indexed_open` can now be resolved, since they no
    // longer appear in the corresponding alert view.
    for ((catalog_name, alert_type), (id, _fired_at, _)) in indexed_open {
        actions.push(AlertAction::Resolve(ResolveAlert {
            id,
            catalog_name,
            alert_type,
            resolved_at: eval_timestamp,
            base_resolved_arguments: None,
        }));
    }
    actions
}

/// A possible alert, which may or may not be firing.
pub struct AlertViewRow {
    pub catalog_name: String,
    pub alert_type: AlertType,
    /// Optional arguments for the alert, _not_ including any recipients. If an
    /// alert view row with `arguments: None` triggers the firing of an alert,
    /// then the effective arguments will be an empty object. The `recipients`
    /// will be added to these arguments automatically if this alert is firing
    /// or resolving, overwriting any previous value.
    pub base_arguments: Option<sqlx::types::Json<ArgsObject>>,
    /// Whether the alert should currently be firing. Normally, it's sufficient
    /// for a view to simply not include a previously firing alert in order for
    /// it to be resolved. But certain alert types may wish to provide a
    /// different set of arguments for the alert resolution, and they can do so
    /// by including a row with `firing: false` and providing `arguments`.
    pub firing: bool,
}
