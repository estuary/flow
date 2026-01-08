//! Alerts overview
//!
//! The `alert_history` table is the singular source of truth for alerts that
//! have fired, including both currently active alerts and historical alerts
//! that have since resolved. Each row in `alert_history` gets a corresponding
//! `AlertNotifications` task in `internal.tasks`, which is responsible for
//! sending all notifications related to the alert. The `task_id` of the
//! notification task will be the same as the `alert_history.id`. The task gets
//! created when the alert is fired, and sticks around until all resolution
//! notifications have been successfully sent. Alerts may be fired via a few
//! different means:
//!
//! - The `tenant_alerts` and `alert_data_movement_stalled` views: These are
//!   holdovers from the old alerting system, which expose firing alerts as
//!   database views.
//! - Controllers: Are responsible for any new alerts for live_specs.
//!
//! This module contains two submodules:
//! - evaluator: code for evaluating alert conditions and firing or resolving
//!   alerts from both of the above sources.
//! - notifier: The automation task for sending alert notifications.
//!
//! These modules both link to `control_plane_api::alerts`, which has low-level
//! functions for querying, firing, and resolving alerts.
//!
//! ### Alert arguments
//!
//! The `alert_history` table has both `arguments` and `resolved_arguments`, but
//! generally most alerts only ever use `arguments`. `resolved_arguments` allows
//! the notifications to render using different data than the alert fired with,
//! but it's a rarely used feature. If `resolved_arguments` is null, then `arguments`
//! are used instead when sending resolution notifications.
//!
//! For historical reasons, the subscribers of each alert are represented as a `recipients` property
//! in the `arguments` or `resolved_arguments`. For example:
//! ```ignore
//! { "recipients": [ {"email": "foo@bar.test", "full_name": "Foo Bar"}, {"email": "alerts@acmeCo.test"} ] }
//! ```
//!
//! The `recipients` of each alert get resolved and added to the arguments
//! automatically by `control_plane_api::alerts`. If an alert resolves _without_
//! explicit `resolved_arguments`, then the set of alert subcribers will _not_
//! be re-evaluated, and thus the resolution notifications will be sent to
//! exactly the same list of recipients as the fired notifications. If an alert
//! provides explicit `resolved_arguments`, though, the recipients will be
//! re-determined at the time of the alert resolution, which may result in
//! resolutions being sent to different recipients.
mod evaluator;
mod notifier;

pub use self::notifier::{AlertNotifications, EmailSender, NotifierState, Sender};

pub use evaluator::{
    AlertEvaluator, AlertView, AlertViewRow, DataMovementStalledAlerts, EvaluatorMessage,
    EvaluatorState, TenantAlerts, evaluate_alert_actions, new_data_movement_alerts_executor,
    new_tenant_alerts_executor,
};
