/// Sends emails using the Resend API with retry logic for rate limiting.
#[derive(Debug)]
pub struct ResendSender {
    from_address: String,
    reply_to_address: String,
    resend_client: resend_rs::Resend,
    retry_options: resend_rs::rate_limit::RetryOptions,
}

impl ResendSender {
    pub async fn send(&self, notification: notifications::NotificationEmail) -> anyhow::Result<()> {
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

        let response = resend_rs::rate_limit::send_with_retry_opts(
            || async { resend_client.emails.send(resend_req.clone()).await },
            retry_options,
        )
        .await
        .context("calling resend API")?;

        tracing::debug!(%idempotency_key, to = %email, email_id = %response.id, "successfully sent email");

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

pub trait EmailSender: std::fmt::Debug + Send + Sync + 'static {
    fn send<'s>(
        &'s self,
        email: notifications::NotificationEmail,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send + 's;
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
                    "skipping sending email (disabled)"
                );
                return Ok(());
            }
            Sender::Resend(resend) => resend.send(notification).await,
        }
    }
}

use anyhow::Context;
use std::sync::Arc;

impl EmailSender for Arc<Sender> {
    async fn send<'s>(
        &'s self,
        email: notifications::NotificationEmail,
    ) -> anyhow::Result<()> {
        (**self).send(email).await
    }
}
