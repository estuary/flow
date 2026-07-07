mod billing_contact;
mod outcome;
mod quotas;

use anyhow::Context;
use automations::{Action, Executor, task_types};
use control_plane_api::billing::BillingProvider;
use std::sync::Arc;

use crate::tenant_controller::outcome::Outcome;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    Wake,
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct TenantControllerState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub billing_contact: Option<billing_contact::BillingContactStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_status: Option<quotas::QuotaUpdateStatus>,
}

pub struct TenantController {
    billing_provider: Option<Arc<dyn BillingProvider>>,
}

impl TenantController {
    pub fn new(billing_provider: Option<Arc<dyn BillingProvider>>) -> Self {
        Self { billing_provider }
    }
}

/// NOTE(BB): This was copied from the crates/billing-integrations/src/publish.rs
/// I wasn't sure if we wanted to add the dependency between the two crates, because
/// Nothing in there is public.
///
/// SHould we promote this to another crate and share it between the two crates
/// that are using it?
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
    PartialOrd,
    Eq,
    Ord,
    Hash,
    Copy,
    sqlx::Type,
)]
#[sqlx(type_name = "payment_provider_type", rename_all = "lowercase")]
pub enum PaymentProvider {
    Stripe,
    External,
}
pub(crate) struct Tenant {
    pub tenant: String,
    pub billing_email: Option<String>,
    pub billing_name: Option<String>,
    pub billing_address: Option<serde_json::Value>,
    pub payment_provider: Option<PaymentProvider>,
}

async fn fetch_tenant_by_controller_task(
    pool: &sqlx::PgPool,
    task_id: models::Id,
) -> anyhow::Result<Option<Tenant>> {
    let row = sqlx::query_as!(
        Tenant,
        r#"
        SELECT tenant as "tenant!", billing_email, billing_name, billing_address, payment_provider as "payment_provider: PaymentProvider"
        FROM tenants
        WHERE controller_task_id = $1
        "#,
        task_id as models::Id,
    )
    .fetch_optional(pool)
    .await?;

    Ok(row)
}

impl Executor for TenantController {
    const TASK_TYPE: automations::TaskType = task_types::TENANT_CONTROLLER;

    type Receive = Message;
    type State = TenantControllerState;
    type Outcome = Action;

    async fn poll<'s>(
        &'s self,
        pool: &'s sqlx::PgPool,
        task_id: models::Id,
        _parent_id: Option<models::Id>,
        state: &'s mut Self::State,
        inbox: &'s mut std::collections::VecDeque<(models::Id, Option<Self::Receive>)>,
    ) -> anyhow::Result<Self::Outcome> {
        let Some(tenant) = fetch_tenant_by_controller_task(pool, task_id)
            .await
            .context("fetching tenant for controller task")?
        else {
            tracing::warn!(%task_id, "no tenant found for controller task, completing");
            return Ok(Action::Done);
        };

        // A wake message means the desired billing contact changed. Cancel any
        // pending retry backoff so the new state is reconciled now rather than
        // after the backoff window (a timer-driven poll arrives with an empty
        // inbox and keeps its backoff).
        let woken_by_message = !inbox.is_empty();
        inbox.drain(..);

        let billing_status = state.billing_contact.get_or_insert_with(Default::default);
        let quota_status = state.quota_status.get_or_insert_with(Default::default);
        if woken_by_message {
            billing_status.failures = 0;
            billing_status.next_retry = None;
            quota_status.failures = 0;
            quota_status.next_retry = None;
        }
        // NOTE(BB): Logically because of the structure of the functions
        // the only error that's allowed to reach this point is a date time
        // overflow error

        // Processing all of the different operations, and recording their name
        // so we can emit better error messages.
        let mut results = vec![];
        results.push((
            "billing contact",
            billing_contact::reconcile(billing_status, &tenant, &self.billing_provider).await,
        ));
        results.push((
            "quota updates",
            quotas::update_quotas(quota_status, pool, &tenant, &self.billing_provider).await,
        ));

        // Processing all error after all operations are completed, building an error message
        // and returning that instead.
        let mut error_messages = vec![];
        let mut total_outcome = Outcome::Idle;
        for (operation, response) in results {
            match response {
                Ok(outcome) => total_outcome = total_outcome.next_action(outcome),
                Err(err) => error_messages
                    .push(format!("{operation} produced the following error: {err:#}")),
            }
        }
        // Check for error sand return
        if !error_messages.is_empty() {
            return Err(anyhow::anyhow!(error_messages.join("\n")));
        }

        match total_outcome {
            Outcome::Idle => Ok(Action::Suspend),
            Outcome::WaitForRetry(duration) => Ok(Action::Sleep(duration)),
        }
    }
}
