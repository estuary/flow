mod billing_contact;
mod outcome;
mod quotas;

use crate::tenant_controller::outcome::Outcome;
use anyhow::Context;
use automations::{Action, Executor, task_types};
use billing_types::PaymentProvider;
use control_plane_api::billing::BillingProvider;
use std::sync::Arc;

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct TaskStatus {
    #[serde(default, skip_serializing_if = "is_zero")]
    pub failures: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

fn is_zero(i: &u32) -> bool {
    *i == 0
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    Wake,
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct TenantControllerState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub billing_contact: Option<TaskStatus>,
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

// NOTE(BB): This was copied from the crates/billing-integrations/src/publish.rs
// I wasn't sure if we wanted to add the dependency between the two crates, because
// Nothing in there is public.
//
// Should we promote this to another crate and share it between the two crates
// that are using it?

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
            quota_status.task_status.failures = 0;
            quota_status.task_status.next_retry = None;
        }

        let result =
            billing_contact::reconcile(billing_status, &tenant, &self.billing_provider).await?;
        let result = result.combine(
            quotas::update_quotas(quota_status, pool, &tenant, &self.billing_provider).await?,
        );

        match result {
            Outcome::Idle => Ok(Action::Suspend),
            Outcome::WaitForRetry(duration) => Ok(Action::Sleep(duration)),
        }
    }
}
