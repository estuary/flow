mod billing_contact;

use std::sync::Arc;

use anyhow::Context;
use automations::{Action, Executor, task_types};
use control_plane_api::billing::BillingProvider;

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
}

impl TenantController {
    pub fn new(billing_provider: Option<Arc<dyn BillingProvider>>) -> Self {
        Self { billing_provider }
    }
}

pub(crate) struct TenantRow {
    pub tenant: String,
    pub billing_email: Option<String>,
    pub billing_address: Option<serde_json::Value>,
}

async fn fetch_tenant_by_controller_task(
    pool: &sqlx::PgPool,
    task_id: models::Id,
) -> anyhow::Result<Option<TenantRow>> {
    let row = sqlx::query_as!(
        TenantRow,
        r#"
        SELECT tenant as "tenant!", billing_email, billing_address
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
        let Some(tenant_row) = fetch_tenant_by_controller_task(pool, task_id)
            .await
            .context("fetching tenant for controller task")?
        else {
            tracing::warn!(%task_id, "no tenant found for controller task, completing");
            return Ok(Action::Done);
        };

        inbox.drain(..);

        let billing_status = state.billing_contact.get_or_insert_with(Default::default);
        let billing_outcome =
            billing_contact::reconcile(billing_status, &tenant_row, &self.billing_provider).await?;

        match billing_outcome {
            billing_contact::Outcome::Idle => Ok(Action::Suspend),
            billing_contact::Outcome::WaitForRetry(duration) => Ok(Action::Sleep(duration)),
        }
    }
}
