mod billing_contact;
mod quotas;

use crate::storage::tenants::{Tenant, TenantStore};
use anyhow::Context;
use automations::{Action, Executor, task_types};
use control_plane_api::billing::BillingProvider;
use std::sync::Arc;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    Wake,
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct TenantControllerState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub billing_contact: Option<billing_contact::BillingContactStatus>,
}

pub struct TenantController {
    billing_provider: Option<Arc<dyn BillingProvider>>,
    tenant_provider: Arc<dyn TenantStore>,
}

impl TenantController {
    pub fn new(
        billing_provider: Option<Arc<dyn BillingProvider>>,
        tenant_provider: Arc<dyn TenantStore>,
    ) -> Self {
        Self {
            billing_provider,
            tenant_provider,
        }
    }
}

impl Executor for TenantController {
    const TASK_TYPE: automations::TaskType = task_types::TENANT_CONTROLLER;

    type Receive = Message;
    type State = TenantControllerState;
    type Outcome = Action;

    async fn poll<'s>(
        &'s self,
        _pool: &'s sqlx::PgPool,
        task_id: models::Id,
        _parent_id: Option<models::Id>,
        state: &'s mut Self::State,
        inbox: &'s mut std::collections::VecDeque<(models::Id, Option<Self::Receive>)>,
    ) -> anyhow::Result<Self::Outcome> {
        let Some(tenant_row) = self
            .tenant_provider
            .get_tenant_by_controller_task(task_id)
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
        if woken_by_message {
            billing_status.failures = 0;
            billing_status.next_retry = None;
        }
        let billing_outcome = billing_contact::reconcile(
            billing_status,
            &tenant_row,
            &self.billing_provider,
            &self.tenant_provider,
        )
        .await?;

        // let quota_update =
        //     quotas::update_quotas(billing_status, &tenant_row, &self.billing_provider, pool)
        //         .await?;
        match billing_outcome {
            billing_contact::Outcome::Idle => Ok(Action::Suspend),
            billing_contact::Outcome::WaitForRetry(duration) => Ok(Action::Sleep(duration)),
        }
    }
}
