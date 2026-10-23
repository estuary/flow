//! Reconciles tenant state through the automations framework.

mod alerts;
mod billing_contact;
mod free_trial;
mod outcome;
mod quotas;

use crate::tenant_controller::outcome::Outcome;
use anyhow::Context;
use automations::{Action, Executor, task_types};
use billing_types::{CollectionMethod, PaymentProvider, PlanKind};
use control_plane_api::billing::{self, BillingProvider};
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alerts: Option<TaskStatus>,
}

pub struct TenantController {
    billing_provider: Option<Arc<dyn BillingProvider>>,
    catalog_stats: Arc<catalog_stats::Client>,
}

impl TenantController {
    pub fn new(
        billing_provider: Option<Arc<dyn BillingProvider>>,
        catalog_stats: Arc<catalog_stats::Client>,
    ) -> Self {
        Self {
            billing_provider,
            catalog_stats,
        }
    }
}

pub(crate) struct Tenant {
    pub tenant: String,
    pub billing_emails: Vec<String>,
    pub billing_name: Option<String>,
    pub billing_address: Option<serde_json::Value>,
    pub payment_provider: Option<PaymentProvider>,
    pub plan_kind: PlanKind,
    pub collection_method: CollectionMethod,
    pub trial_start: Option<chrono::NaiveDate>,
}

impl Tenant {
    /// Whether the tenant's invoices are charged to its default payment method,
    /// which it must then have. Tenants with an `external` payment provider
    /// don't need one either. Existing contracts are `external`, and keep the
    /// `self_serve` default of `plan_kind` until it's set.
    /// TODO(billing): remove the `external` exemption once no tenant uses it.
    pub fn needs_payment_method(&self) -> bool {
        self.plan_kind == PlanKind::SelfServe
            && self.collection_method == CollectionMethod::ChargeDefaultPaymentMethod
            && self.payment_provider != Some(PaymentProvider::External)
    }
}

async fn fetch_tenant_by_controller_task(
    pool: &sqlx::PgPool,
    task_id: models::Id,
) -> anyhow::Result<Option<Tenant>> {
    let row = sqlx::query_as!(
        Tenant,
        r#"
        SELECT tenant as "tenant!", billing_emails, billing_name, billing_address, payment_provider as "payment_provider: PaymentProvider", plan_kind as "plan_kind: PlanKind", collection_method as "collection_method: CollectionMethod", trial_start
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
        let Some(mut tenant) = fetch_tenant_by_controller_task(pool, task_id)
            .await
            .context("fetching tenant for controller task")?
        else {
            tracing::warn!(%task_id, "no tenant found for controller task, completing");
            return Ok(Action::Done);
        };

        // Wake messages request reconciliation of the current tenant state. Cancel
        // pending billing retry backoff so new state is reconciled now rather than
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

        // Billing contact and quotas run only when woken by a message or once a
        // retry of their own failure is due. They share one retrieval of the
        // tenant's Stripe customer with alerts, and other polls don't retrieve
        // it, so that daily polls don't fan out into a Stripe call per tenant.
        let now = chrono::Utc::now();
        let retry_due = |status: &TaskStatus| {
            status.failures != 0 && status.next_retry.is_none_or(|at| at <= now)
        };
        let customer = match &self.billing_provider {
            Some(provider)
                if woken_by_message
                    || retry_due(billing_status)
                    || retry_due(&quota_status.task_status) =>
            {
                Some(
                    billing::find_customer(pool, provider.as_ref(), &tenant.tenant)
                        .await
                        .context("looking up Stripe customer"),
                )
            }
            _ => None,
        };

        let mut result = Outcome::Idle;
        if woken_by_message || billing_status.failures != 0 {
            result = result.combine(
                billing_contact::reconcile(
                    billing_status,
                    &tenant,
                    &self.billing_provider,
                    customer.as_ref(),
                    now,
                )
                .await?,
            );
        }
        if woken_by_message || quota_status.task_status.failures != 0 {
            result = result.combine(
                quotas::update_quotas(quota_status, pool, &tenant, customer.as_ref(), now).await?,
            );
        }

        let free_trial_status = state.free_trial.get_or_insert_with(Default::default);
        result = result.combine(
            free_trial::check(
                free_trial_status,
                pool,
                &mut tenant,
                &self.catalog_stats,
                now,
            )
            .await?,
        );

        // Alerts run last, so that they see a trial started by this poll.
        let alerts_status = state.alerts.get_or_insert_with(Default::default);
        result = result
            .combine(alerts::evaluate(alerts_status, pool, &tenant, customer.as_ref(), now).await?);

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
    use sqlx::types::Json;

    const TENANT: &str = "tenantControllerPoll/";

    async fn poll_ready(server: &automations::Server, pool: &sqlx::PgPool) {
        let heartbeat_timeout = std::time::Duration::from_secs(5);
        let mut permits = Arc::new(tokio::sync::Semaphore::new(1))
            .acquire_many_owned(1)
            .await
            .unwrap();
        let mut ready = automations::server::dequeue_tasks(
            &mut permits,
            pool,
            server,
            &[task_types::TENANT_CONTROLLER.0],
            heartbeat_timeout,
        )
        .await
        .unwrap();
        assert_eq!(ready.len(), 1);
        automations::executors::poll_task(ready.pop().unwrap(), heartbeat_timeout)
            .await
            .unwrap();
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn only_messages_reconcile_billing(pool: sqlx::PgPool) {
        let provider = Arc::new(InMemoryBillingProvider::new());
        provider.add_customer(TENANT, "cus_test", Some("pm_test"));

        let client = Arc::new(catalog_stats::test_util::connect_emulator("estuary-local").await);
        catalog_stats::test_util::delete_rows_with_prefix(&client, TENANT)
            .await
            .unwrap();
        let server = automations::Server::new()
            .register(TenantController::new(Some(provider.clone()), client));

        sqlx::query("INSERT INTO tenants (tenant, billing_emails, tasks_quota, collections_quota, stripe_customer_id) VALUES ($1, '{billing@acme.co}', 2, 10, 'cus_test')")
            .bind(TENANT)
            .execute(&pool)
            .await
            .unwrap();

        poll_ready(&server, &pool).await;
        let (Json(state), scheduled, quotas): (Json<TenantControllerState>, bool, Vec<i32>) = sqlx::query_as(
            "SELECT t.inner_state, t.wake_at > NOW() AND t.wake_at <= NOW() + INTERVAL '1 day', ARRAY[te.tasks_quota, te.collections_quota] FROM tenants te JOIN internal.tasks t ON te.controller_task_id = t.task_id WHERE te.tenant = $1")
            .bind(TENANT)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(scheduled);
        assert_eq!(quotas, [2, 10]);
        assert_eq!(provider.retrieve_customer_call_count(), 0);
        assert_eq!(provider.update_billing_profile_call_count(), 0);
        let next_check = state.free_trial.unwrap().next_check;
        assert!(next_check.is_some());

        sqlx::query("SELECT internal.wake_tenant_controller($1)")
            .bind(TENANT)
            .execute(&pool)
            .await
            .unwrap();
        poll_ready(&server, &pool).await;
        let (Json(state), consumed, quotas): (Json<TenantControllerState>, bool, Vec<i32>) = sqlx::query_as(
            "SELECT t.inner_state, t.inbox IS NULL AND t.inbox_next IS NULL, ARRAY[te.tasks_quota, te.collections_quota] FROM tenants te JOIN internal.tasks t ON te.controller_task_id = t.task_id WHERE te.tenant = $1")
            .bind(TENANT)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(consumed);
        assert_eq!(quotas, [100, 10000]);
        assert_eq!(provider.retrieve_customer_call_count(), 1);
        assert_eq!(provider.update_billing_profile_call_count(), 1);
        assert_eq!(state.free_trial.as_ref().unwrap().next_check, next_check);

        // A poll before a pending retry is due doesn't call Stripe.
        let mut state = state;
        state.billing_contact = Some(TaskStatus {
            failures: 1,
            next_retry: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
            last_error: None,
        });
        sqlx::query("UPDATE internal.tasks SET inner_state = $2, wake_at = NOW() WHERE task_id = (SELECT controller_task_id FROM tenants WHERE tenant = $1)")
            .bind(TENANT)
            .bind(Json(state))
            .execute(&pool)
            .await
            .unwrap();
        poll_ready(&server, &pool).await;
        assert_eq!(provider.retrieve_customer_call_count(), 1);
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn resetting_trial_resumes_checks(pool: sqlx::PgPool) {
        let tenant = "tenantControllerReset/";
        let client = Arc::new(catalog_stats::test_util::connect_emulator("estuary-local").await);
        catalog_stats::test_util::delete_rows_with_prefix(&client, tenant)
            .await
            .unwrap();
        let server = automations::Server::new().register(TenantController::new(None, client));
        // A `send_invoice` tenant needs no payment method, so no daily alert
        // check follows its trial.
        sqlx::query("INSERT INTO tenants (tenant, trial_start, collection_method) VALUES ($1, CURRENT_DATE - 40, 'send_invoice')")
            .bind(tenant)
            .execute(&pool)
            .await
            .unwrap();
        poll_ready(&server, &pool).await;
        let suspended: bool = sqlx::query_scalar("SELECT t.wake_at IS NULL AND te.trial_start = CURRENT_DATE - 40 FROM tenants te JOIN internal.tasks t ON te.controller_task_id = t.task_id WHERE te.tenant = $1")
            .bind(tenant).fetch_one(&pool).await.unwrap();
        assert!(suspended);

        sqlx::query("UPDATE tenants SET trial_start = NULL WHERE tenant = $1")
            .bind(tenant)
            .execute(&pool)
            .await
            .unwrap();
        poll_ready(&server, &pool).await;
        let (Json(state), scheduled, untouched): (Json<TenantControllerState>, bool, bool) = sqlx::query_as(
            "SELECT t.inner_state, t.wake_at > NOW(), te.trial_start IS NULL FROM tenants te JOIN internal.tasks t ON te.controller_task_id = t.task_id WHERE te.tenant = $1")
            .bind(tenant).fetch_one(&pool).await.unwrap();
        assert!(scheduled && untouched);
        assert!(state.free_trial.unwrap().next_check.is_some());
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn failed_trial_check_recovers(pool: sqlx::PgPool) {
        let tenant = "tenantControllerRetry/";
        let bad_client =
            Arc::new(catalog_stats::test_util::connect_emulator("missing-instance").await);
        let bad_server =
            automations::Server::new().register(TenantController::new(None, bad_client));
        sqlx::query("INSERT INTO tenants (tenant) VALUES ($1)")
            .bind(tenant)
            .execute(&pool)
            .await
            .unwrap();
        poll_ready(&bad_server, &pool).await;
        let (Json(mut state), retrying, untouched): (Json<TenantControllerState>, bool, bool) = sqlx::query_as(
            "SELECT t.inner_state, t.wake_at > NOW() + INTERVAL '50 seconds' AND t.wake_at < NOW() + INTERVAL '70 seconds', te.trial_start IS NULL FROM tenants te JOIN internal.tasks t ON te.controller_task_id = t.task_id WHERE te.tenant = $1")
            .bind(tenant).fetch_one(&pool).await.unwrap();
        assert!(retrying && untouched);
        let status = state.free_trial.as_mut().unwrap();
        assert_eq!(status.task_status.failures, 1);
        assert!(
            status
                .task_status
                .last_error
                .as_deref()
                .unwrap()
                .contains("daily usage")
        );
        status.task_status.next_retry = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
        sqlx::query("UPDATE internal.tasks SET inner_state = $2, wake_at = NOW() WHERE task_id = (SELECT controller_task_id FROM tenants WHERE tenant = $1)")
            .bind(tenant).bind(Json(state)).execute(&pool).await.unwrap();

        let client = Arc::new(catalog_stats::test_util::connect_emulator("estuary-local").await);
        catalog_stats::test_util::delete_rows_with_prefix(&client, tenant)
            .await
            .unwrap();
        let server = automations::Server::new().register(TenantController::new(None, client));
        poll_ready(&server, &pool).await;
        let (Json(state), scheduled): (Json<TenantControllerState>, bool) = sqlx::query_as(
            "SELECT t.inner_state, t.wake_at > NOW() AND t.wake_at <= NOW() + INTERVAL '1 day' FROM tenants te JOIN internal.tasks t ON te.controller_task_id = t.task_id WHERE te.tenant = $1")
            .bind(tenant).fetch_one(&pool).await.unwrap();
        let status = state.free_trial.unwrap();
        assert!(scheduled && status.next_check.is_some());
        assert_eq!(status.task_status.failures, 0);
        assert!(status.task_status.next_retry.is_none() && status.task_status.last_error.is_none());
    }
}
