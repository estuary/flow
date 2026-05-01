use anyhow::Context;
use automations::{Action, Executor, TaskType, task_types};
use chrono::{DateTime, Utc};
use control_plane_api::alerts::{Alert, fetch_alert_by_id};
use notifications::Renderer;

pub use crate::email::{EmailSender, Sender};

#[derive(Default, Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NotifierState {
    pub(crate) fired_completed: Option<DateTime<Utc>>,
    pub(crate) max_idempotency_key: Option<String>,
    pub(crate) last_error: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub(crate) failures: u32,
}

fn is_zero(i: &u32) -> bool {
    *i == 0
}

#[derive(Debug)]
pub struct AlertNotifications<ES> {
    renderer: Renderer,
    sender: ES,
}

impl<ES: EmailSender> AlertNotifications<ES> {
    pub fn new(
        dashboard_base_url: impl Into<String>,
        sender: ES,
    ) -> anyhow::Result<AlertNotifications<ES>> {
        let renderer = Renderer::try_new(dashboard_base_url.into())?;
        Ok(AlertNotifications { renderer, sender })
    }

    async fn try_send_notifications(
        &self,
        state: &mut NotifierState,
        alert: Alert,
    ) -> anyhow::Result<AlertOutcome> {
        let is_resolved = alert.resolved_at.is_some();
        let alert_state = into_alert_state(alert);
        let emails: Vec<notifications::NotificationEmail> = {
            let mut all = self
                .renderer
                .render_emails(&alert_state)
                .context("rendering alert emails")?;
            // Sort all the emails by the idempotency key, so that we can skip
            // sending ones that we know have succeeded.
            all.sort_by(|l, r| l.idempotency_key.cmp(&r.idempotency_key));
            all.into_iter()
                .skip_while(|email| {
                    state
                        .max_idempotency_key
                        .as_deref()
                        .is_some_and(|max| email.idempotency_key.as_str() <= max)
                })
                .collect()
        };
        let email_count = emails.len();

        tracing::debug!(%is_resolved, %email_count, "rendered alert notification emails");

        for notification in emails {
            // Have we already sent the email for this idempotency key?
            if state
                .max_idempotency_key
                .as_deref()
                .is_some_and(|max| notification.idempotency_key.as_str() <= max)
            {
                continue;
            }

            let idempotency_key = notification.idempotency_key.clone();
            self.sender.send(notification).await.with_context(|| {
                format!("sending alert notification with idempotency key '{idempotency_key}'")
            })?;
            state.max_idempotency_key = Some(idempotency_key);
        }

        tracing::info!(%email_count, %is_resolved, "finished sending alert notifications");
        if is_resolved {
            Ok(AlertOutcome::ResolvedSent)
        } else {
            state.fired_completed = Some(Utc::now());
            Ok(AlertOutcome::AwaitResolution)
        }
    }
}

pub enum AlertOutcome {
    AwaitResolution,
    ResolvedSent,
    BackoffErr { backoff: std::time::Duration },
}

impl automations::Outcome for AlertOutcome {
    async fn apply<'s>(self, _txn: &'s mut sqlx::PgConnection) -> anyhow::Result<Action> {
        match self {
            AlertOutcome::AwaitResolution => Ok(Action::Suspend),
            AlertOutcome::ResolvedSent => Ok(Action::Done),
            AlertOutcome::BackoffErr { backoff } => Ok(Action::Sleep(backoff)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "event", rename_all = "camelCase")]
pub enum NotifierMessage {
    ManualTrigger,
    Fired,
    Resolved,
}

impl<ES: EmailSender> Executor for AlertNotifications<ES> {
    const TASK_TYPE: TaskType = task_types::ALERT_NOTIFICATIONS;
    type Receive = NotifierMessage;
    type State = NotifierState;
    type Outcome = AlertOutcome;

    #[tracing::instrument(skip_all, fields(%task_id, catalog_name, alert_type, fired_at))]
    async fn poll<'s>(
        &'s self,
        pool: &'s sqlx::PgPool,
        task_id: models::Id,
        _parent_id: Option<models::Id>,
        state: &'s mut Self::State,
        inbox: &'s mut std::collections::VecDeque<(models::Id, Option<Self::Receive>)>,
    ) -> anyhow::Result<Self::Outcome> {
        let Some(alert) = fetch_alert_by_id(task_id, pool)
            .await
            .context("fetching from alert_history")?
        else {
            tracing::warn!(%task_id, ?state, "no alert found for task");
            return Ok(AlertOutcome::ResolvedSent);
        };

        tracing::Span::current()
            .record("catalog_name", alert.catalog_name.as_str())
            .record("alert_type", alert.alert_type.name())
            .record("fired_at", alert.fired_at.to_rfc3339());

        let is_resolved = alert.resolved_at.is_some();
        if state.fired_completed.is_some() && !is_resolved {
            tracing::info!(
                "AlertNotifications task polled after fired sent and before resolved, nothing to do"
            );
            return Ok(AlertOutcome::AwaitResolution);
        }

        if is_resolved && state.fired_completed.is_none() {
            tracing::warn!(?alert.resolved_at, ?state.max_idempotency_key, "alert resolved before all fired emails could be sent");
            state.fired_completed = Some(Utc::now());
            state.max_idempotency_key.take();
        }

        match self.try_send_notifications(state, alert).await {
            Ok(result) => {
                // Always clear the inbox after a successful run, or else we'll keep getting polled
                inbox.clear();

                // We've succeeded, so clear any error state.
                state.last_error.take();
                state.failures = 0;
                // Important to clear this, because we'll re-use it for sending resolution emails.
                state.max_idempotency_key.take();
                Ok(result)
            }
            Err(error) => {
                tracing::warn!(%error, "sending alert notifications failed");
                state.last_error = Some(format!("{error:#}"));
                state.failures += 1;
                // Always clear the inbox after a successful run, or else we'll keep getting polled
                inbox.clear();

                let backoff_secs = state.failures.min(5) as u64 * 60;
                let jitter_secs = rand::random_range(1..backoff_secs);
                let backoff = std::time::Duration::from_secs(backoff_secs + jitter_secs);
                Ok(AlertOutcome::BackoffErr { backoff })
            }
        }
    }
}

fn into_alert_state(alert: Alert) -> notifications::AlertState {
    let Alert {
        id,
        catalog_name,
        alert_type,
        fired_at,
        resolved_at,
        arguments,
        resolved_arguments,
    } = alert;

    notifications::AlertState {
        alert_id: id,
        catalog_name: models::Name::new(catalog_name),
        alert_type,
        fired_at,
        resolved_at,
        arguments: arguments.0,
        resolved_arguments: resolved_arguments.map(|ra| ra.0).unwrap_or_default(),
    }
}
