use crate::tenant_controller::{Outcome, PaymentProvider, TaskStatus, Tenant, retry_backoff};
use anyhow::Context as _;
use control_plane_api::billing::{self, BillingProvider};
use std::sync::Arc;

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct QuotaUpdateStatus {
    #[serde(default)]
    pub updated_quotas: bool,
    #[serde(default)]
    pub task_status: TaskStatus,
}

pub async fn update_quotas(
    status: &mut QuotaUpdateStatus,
    pool: &sqlx::PgPool,
    tenant: &Tenant,
    billing_provider: &Option<Arc<dyn BillingProvider>>,
) -> anyhow::Result<Outcome> {
    if let Some(next_retry) = status.task_status.next_retry {
        let now = chrono::Utc::now();
        if next_retry > now {
            let wait = (next_retry - now)
                .to_std()
                .unwrap_or(std::time::Duration::from_secs(60));
            return Ok(Outcome::WaitForRetry(wait));
        }
    }

    match do_update_quotas(status, pool, tenant, billing_provider).await {
        Ok(()) => {
            status.task_status.failures = 0;
            status.task_status.next_retry = None;
            status.task_status.last_error = None;
            Ok(Outcome::Idle)
        }
        Err(err) => {
            status.task_status.failures += 1;
            let backoff = retry_backoff(status.task_status.failures);
            status.task_status.next_retry =
                Some(chrono::Utc::now() + chrono::Duration::from_std(backoff)?);
            status.task_status.last_error = Some(format!("{err:#}"));
            tracing::warn!(
                tenant = %tenant.tenant,
                failures = status.task_status.failures,
                ?backoff,
                "failed while updating tenant quotas: {err:#}",
            );
            Ok(Outcome::WaitForRetry(backoff))
        }
    }
}

async fn do_update_quotas(
    status: &mut QuotaUpdateStatus,
    pool: &sqlx::PgPool,
    tenant: &Tenant,
    billing_provider: &Option<Arc<dyn BillingProvider>>,
) -> anyhow::Result<()> {
    if status.updated_quotas {
        return Ok(());
    }
    // Check this first because its possible for us to error out while communicating with
    // the billing provider, it's possible for the customer to not exist.
    if tenant.payment_provider == Some(PaymentProvider::External) {
        let did_update = update_tenant_quotas_by_name(pool, &tenant.tenant).await?;
        if did_update {
            tracing::info!(
                tenant = %tenant.tenant,
                "Ran quota update because payment provider is set to external",
            );
        } else {
            tracing::warn!(
                tenant = %tenant.tenant,
                "Ran quota update but didn't update any rows",
            );
        }
        status.updated_quotas = did_update;
        return Ok(());
    }

    let Some(provider) = billing_provider else {
        return Ok(());
    };

    let customer = provider
        .find_customer(&tenant.tenant)
        .await
        .context("looking up Stripe customer")?;

    let Some(customer) = customer else {
        return Ok(());
    };

    if billing::default_payment_method_id(&customer).is_some() {
        // Updating tenant quotas using name
        let did_update = update_tenant_quotas_by_name(pool, &tenant.tenant).await?;
        if did_update {
            tracing::info!(
                tenant = %tenant.tenant,
                "Ran quota update because default payment method was set within billing provider",
            );
        } else {
            tracing::warn!(
                tenant = %tenant.tenant,
                "Ran quota update but didn't update any rows",
            );
        }
        status.updated_quotas = did_update;
    }
    Ok(())
}

const PAID_TASKS_QUOTA_MIN: i32 = 100;
const COLLECTIONS_QUOTAS_MIN: i32 = 10000;

/// Update the quotas to the necessary amount of 100 tasks and 10000 collections.
async fn update_tenant_quotas_by_name(
    pool: &sqlx::PgPool,
    tenant_name: &str,
) -> anyhow::Result<bool> {
    let result = sqlx::query!(
        r#"
            UPDATE tenants
                SET
                    tasks_quota = GREATEST(tasks_quota, $2),
                    collections_quota = GREATEST(collections_quota, $3)
                WHERE tenants.tenant = $1
            "#,
        tenant_name,
        PAID_TASKS_QUOTA_MIN,
        COLLECTIONS_QUOTAS_MIN,
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

#[cfg(test)]
mod tests {

    use super::super::Tenant;
    use super::{Outcome, QuotaUpdateStatus, update_quotas};
    use crate::tenant_controller::PaymentProvider;
    use control_plane_api::billing::{BillingProvider, InMemoryBillingProvider};
    use std::sync::Arc;

    const TENANT_1: &str = "acmeCo/";
    const TENANT_2: &str = "acmeCo2GtQuotas/";
    const CUSTOMER_ID: &str = "cus_test";

    async fn any_customer(tenant_name: &str) -> Arc<InMemoryBillingProvider> {
        let provider = InMemoryBillingProvider::new();
        provider.add_customer(tenant_name, CUSTOMER_ID, None);
        provider
            .update_customer_default_payment_method(
                &CUSTOMER_ID.parse().unwrap(),
                Some("card_123456"),
            )
            .await
            .unwrap();
        provider
            .update_customer_billing_profile(&CUSTOMER_ID.parse().unwrap(), None, None, None)
            .await
            .unwrap();
        Arc::new(provider)
    }

    async fn run(
        provider: &Arc<InMemoryBillingProvider>,
        status: &mut QuotaUpdateStatus,
        tenant: &Tenant,
        pool: &sqlx::PgPool,
    ) -> Outcome {
        let provider: Option<Arc<dyn BillingProvider>> = Some(provider.clone());
        update_quotas(status, pool, tenant, &provider)
            .await
            .unwrap()
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn quota_update_no_customer(pool: sqlx::PgPool) {
        let tenant_name = "test_name";
        let provider = any_customer(tenant_name).await;
        let tenant = Tenant {
            tenant: "AnotherTenantName".to_string(),
            billing_email: None,
            billing_name: None,
            billing_address: None,
            payment_provider: None,
        };
        let mut status = QuotaUpdateStatus::default();
        let outcome = run(&provider, &mut status, &tenant, &pool).await;
        assert!(matches!(outcome, Outcome::Idle));
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn quota_update_no_tenant_in_db(pool: sqlx::PgPool) {
        let tenant_name = "test_name";
        let provider = any_customer(tenant_name).await;
        let tenant = Tenant {
            tenant: tenant_name.to_string(),
            billing_email: None,
            billing_name: None,
            billing_address: None,
            payment_provider: None,
        };
        let mut status = QuotaUpdateStatus::default();
        let outcome = run(&provider, &mut status, &tenant, &pool).await;
        assert!(matches!(outcome, Outcome::Idle));
        assert!(!status.updated_quotas);
        assert_eq!(status.task_status.failures, 0);
    }

    async fn get_tenants_quotas(pool: &sqlx::PgPool, tenant_name: &str) -> UpdatedQueryCheck {
        sqlx::query_as!(
            UpdatedQueryCheck,
            r#"
            SELECT tasks_quota, collections_quota FROM tenants WHERE tenant = $1
            "#,
            tenant_name,
        )
        .fetch_one(pool)
        .await
        .unwrap()
    }

    pub struct UpdatedQueryCheck {
        tasks_quota: i32,
        collections_quota: i32,
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "fixtures", scripts("quotas"))
    )]
    async fn quota_update_success_set_in_stripe(pool: sqlx::PgPool) {
        let provider = any_customer(TENANT_1).await;
        let tenant = Tenant {
            tenant: TENANT_1.to_string(),
            billing_email: None,
            billing_name: None,
            billing_address: None,
            payment_provider: None,
        };
        let mut status = QuotaUpdateStatus::default();
        let outcome = run(&provider, &mut status, &tenant, &pool).await;
        assert!(matches!(outcome, Outcome::Idle));
        assert!(status.updated_quotas);
        assert_eq!(status.task_status.failures, 0);
        let quotas = get_tenants_quotas(&pool, TENANT_1).await;
        assert_eq!(quotas.tasks_quota, 100);
        assert_eq!(quotas.collections_quota, 10000);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "fixtures", scripts("quotas"))
    )]
    async fn quota_update_success_set_in_db(pool: sqlx::PgPool) {
        // Isolate the `payment_provider = External` path: the customer has NO
        // default payment method configured but is configured inside of stripe.
        let provider = InMemoryBillingProvider::new();
        provider.add_customer(TENANT_1, CUSTOMER_ID, None);

        let provider = Arc::new(provider);
        let tenant = Tenant {
            tenant: TENANT_1.to_string(),
            billing_email: None,
            billing_name: None,
            billing_address: None,
            payment_provider: Some(PaymentProvider::External),
        };
        let mut status = QuotaUpdateStatus::default();
        let outcome = run(&provider, &mut status, &tenant, &pool).await;
        assert!(matches!(outcome, Outcome::Idle));
        assert!(status.updated_quotas);
        assert_eq!(status.task_status.failures, 0);
        let quotas = get_tenants_quotas(&pool, TENANT_1).await;
        assert_eq!(quotas.tasks_quota, 100);
        assert_eq!(quotas.collections_quota, 10000);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "fixtures", scripts("quotas"))
    )]
    async fn quota_update_success_pre_configured(pool: sqlx::PgPool) {
        let provider = any_customer(TENANT_2).await;
        let tenant = Tenant {
            tenant: TENANT_2.to_string(),
            billing_email: None,
            billing_name: None,
            billing_address: None,
            payment_provider: None,
        };

        let mut status = QuotaUpdateStatus::default();
        let outcome = run(&provider, &mut status, &tenant, &pool).await;
        assert!(matches!(outcome, Outcome::Idle));
        assert!(status.updated_quotas);
        assert_eq!(status.task_status.failures, 0);

        let quotas = get_tenants_quotas(&pool, TENANT_2).await;
        assert_eq!(quotas.tasks_quota, 200);
        assert_eq!(quotas.collections_quota, 20000);
    }
}
