use anyhow::Context;
use automations::{Action, Executor, TaskType, task_types};
use chrono::{DateTime, Utc};
use control_plane_api::alerts::{Alert, fetch_alert_by_id};
use notifications::Renderer;

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

pub trait EmailSender: std::fmt::Debug + Send + Sync + 'static {
    fn send<'s>(
        &'s self,
        email: notifications::NotificationEmail,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send + 's;
}

/// Sends emails using the resend
#[derive(Debug)]
pub struct ResendSender {
    from_address: String,
    reply_to_address: String,
    resend_client: resend_rs::Resend,
    retry_options: resend_rs::rate_limit::RetryOptions,
}

impl ResendSender {
    async fn send(&self, notification: notifications::NotificationEmail) -> anyhow::Result<()> {
        let notifications::NotificationEmail {
            idempotency_key,
            recipient: notifications::Recipient { email, .. },
            subject,
            body,
        } = notification;

        let Self {
            from_address,
            reply_to_address,
            resend_client,
            retry_options,
        } = self;

        let resend_req =
            resend_rs::types::CreateEmailBaseOptions::new(from_address, [email.as_str()], subject)
                .with_reply(reply_to_address.as_str())
                .with_html(body.as_str())
                .with_idempotency_key(idempotency_key.as_str());

        // Note on retries: We don't technically need to handle retries here, as
        // we could instead return and just schedule ourselves to run again.
        // It's common for many alerts to fire more or less simultaneously, and
        // the resend rate limit is only 9 req/s. So my current thinking is that
        // it's better to handle retries here, so that we don't end up having a
        // bunch of other notifier tasks run and then have this one hit another
        // rate limit error when we retry it. Better to retry each notification
        // until it succeeds. If we exhaust the number of retries, we'll return
        // an error and back off somewhat longer.
        let response = resend_rs::rate_limit::send_with_retry_opts(
            || async { resend_client.emails.send(resend_req.clone()).await },
            retry_options,
        )
        .await
        .context("calling resend API")?;

        tracing::debug!(%idempotency_key, to = %email, email_id = %response.id, "successfully sent alert email");

        Ok(())
    }
}

#[derive(Debug)]
pub enum Sender {
    Disabled,
    Resend(ResendSender),
}

impl Sender {
    pub fn resend(
        api_key: &str,
        from_address: String,
        reply_to_address: String,
        http_client: reqwest::Client,
    ) -> Sender {
        let resend_client = resend_rs::Resend::with_client(api_key, http_client);
        let inner = ResendSender {
            from_address,
            reply_to_address,
            resend_client,
            retry_options: resend_rs::rate_limit::RetryOptions {
                duration_ms: 150,
                jitter_range_ms: 0..1000,
                max_retries: 5,
            },
        };
        Sender::Resend(inner)
    }
}

impl EmailSender for Sender {
    async fn send<'s>(
        &'s self,
        notification: notifications::NotificationEmail,
    ) -> anyhow::Result<()> {
        match self {
            Sender::Disabled => {
                tracing::warn!(
                    to = %notification.recipient.email,
                    subject = %notification.subject,
                    idempotency_key = %notification.idempotency_key,
                    "skipping sending alert email (disabled)"
                );
                return Ok(());
            }
            Sender::Resend(resend) => resend.send(notification).await,
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
