//! The tenant controller is a per-tenant automations task which:
//! - reconciles the tenant's billing contact to its Stripe customer,
//! - raises the tenant's quotas once it has a payment method, and
//! - starts the tenant's free trial once its usage exceeds the free tier.
//!
//! Billing contact and quotas react to wake messages, sent when billing
//! details change. The free trial check runs daily, at a time of day fixed
//! per tenant so that checks of all tenants spread across the day.

mod billing_contact;
mod free_trial;
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

pub fn retry_backoff(failures: u32) -> std::time::Duration {
    match failures {
        0 => std::time::Duration::ZERO,
        1 => std::time::Duration::from_secs(60),
        2 => std::time::Duration::from_secs(300),
        _ => std::time::Duration::from_secs(900),
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub free_trial: Option<free_trial::FreeTrialStatus>,
}

pub struct TenantController {
    billing_provider: Option<Arc<dyn BillingProvider>>,
    catalog_stats: Option<Arc<catalog_stats::Client>>,
}

impl TenantController {
    pub fn new(
        billing_provider: Option<Arc<dyn BillingProvider>>,
        catalog_stats: Option<Arc<catalog_stats::Client>>,
    ) -> Self {
        Self {
            billing_provider,
            catalog_stats,
        }
    }
}

pub(crate) struct Tenant {
    pub tenant: String,
    pub billing_email: Option<String>,
    pub billing_name: Option<String>,
    pub billing_address: Option<serde_json::Value>,
    pub payment_provider: Option<PaymentProvider>,
    pub trial_start: Option<chrono::NaiveDate>,
}

async fn fetch_tenant_by_controller_task(
    pool: &sqlx::PgPool,
    task_id: models::Id,
) -> anyhow::Result<Option<Tenant>> {
    let row = sqlx::query_as!(
        Tenant,
        r#"
        SELECT tenant as "tenant!", billing_email, billing_name, billing_address, payment_provider as "payment_provider: PaymentProvider", trial_start
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

        // Billing contact and quotas each search Stripe for the tenant's
        // customer, so they run only when woken by a message or to retry their
        // own failure. Daily polls for the free trial check must not fan out
        // into a Stripe search per tenant.
        let mut result = Outcome::Idle;
        if woken_by_message || billing_status.failures != 0 {
            result = result.combine(
                billing_contact::reconcile(billing_status, &tenant, &self.billing_provider).await?,
            );
        }
        if woken_by_message || quota_status.task_status.failures != 0 {
            result = result.combine(
                quotas::update_quotas(quota_status, pool, &tenant, &self.billing_provider).await?,
            );
        }

        let free_trial_status = state.free_trial.get_or_insert_with(Default::default);
        result = result.combine(
            free_trial::check(
                free_trial_status,
                pool,
                &tenant,
                &self.catalog_stats,
                chrono::Utc::now(),
            )
            .await?,
        );

        match result.wait() {
            None => Ok(Action::Suspend),
            Some(duration) => Ok(Action::Sleep(duration)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use control_plane_api::billing::InMemoryBillingProvider;

    const TENANT: &str = "tenantControllerPoll/";

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn only_messages_reconcile_billing(pool: sqlx::PgPool) {
        // The tenant's desired billing email differs from its Stripe
        // customer's, so each billing reconciliation updates the customer.
        let provider = Arc::new(InMemoryBillingProvider::new());
        provider.add_customer(TENANT, "cus_test", None);

        let client = free_trial::tests::emulator().await;
        catalog_stats::test_util::delete_rows_with_prefix(&client, TENANT)
            .await
            .unwrap();

        sqlx::query("INSERT INTO tenants (tenant, billing_email) VALUES ($1, 'billing@acme.co')")
            .bind(TENANT)
            .execute(&pool)
            .await
            .unwrap();
        // The AFTER INSERT trigger records the controller, so read it back.
        let task_id: models::Id =
            sqlx::query_scalar("SELECT controller_task_id FROM tenants WHERE tenant = $1")
                .bind(TENANT)
                .fetch_one(&pool)
                .await
                .unwrap();

        let controller = TenantController::new(Some(provider.clone()), Some(client));
        let mut state = TenantControllerState::default();
        let mut inbox = std::collections::VecDeque::new();
        let one_day = std::time::Duration::from_secs(86_400);

        // A timer-driven poll checks for a free trial and sleeps until the
        // next check, without touching Stripe.
        let action = controller
            .poll(&pool, task_id, None, &mut state, &mut inbox)
            .await
            .unwrap();
        assert!(
            matches!(action, Action::Sleep(wait) if wait <= one_day),
            "{action:?}"
        );
        assert_eq!(provider.update_billing_profile_call_count(), 0);
        let next_check = state.free_trial.as_ref().unwrap().next_check;
        assert!(next_check.is_some());

        // A wake message reconciles billing, and keeps the scheduled check.
        inbox.push_back((models::Id::zero(), Some(Message::Wake)));
        let action = controller
            .poll(&pool, task_id, None, &mut state, &mut inbox)
            .await
            .unwrap();
        assert!(
            matches!(action, Action::Sleep(wait) if wait <= one_day),
            "{action:?}"
        );
        assert_eq!(provider.update_billing_profile_call_count(), 1);
        assert_eq!(state.free_trial.as_ref().unwrap().next_check, next_check);
    }
}
