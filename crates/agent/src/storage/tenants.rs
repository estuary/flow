use sqlx::PgPool;

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

pub struct Tenant {
    pub tenant: String,
    pub billing_email: Option<String>,
    pub billing_name: Option<String>,
    pub billing_address: Option<serde_json::Value>,
    pub payment_provider: Option<PaymentProvider>,
}

#[async_trait::async_trait]
pub trait TenantStore: Send + Sync {
    async fn get_tenant_by_controller_task(
        &self,
        task_id: models::Id,
    ) -> anyhow::Result<Option<Tenant>>;

    /// This updates the tenant quotas when transitioning from a free or trail account
    /// to a paid account.
    async fn update_tenant_quotas_by_name(&self, tenant_name: &str) -> anyhow::Result<()>;
}

/// This is the concrete implementation using postgres database pool
#[derive(Clone)]
pub struct PgTenantStore {
    pool: PgPool,
}

impl PgTenantStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl TenantStore for PgTenantStore {
    async fn get_tenant_by_controller_task(
        &self,
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
        .fetch_optional(&self.pool)
        .await?;

        Ok(row)
    }

    async fn update_tenant_quotas_by_name(&self, tenant_name: &str) -> anyhow::Result<()> {
        sqlx::query!(
            r#"
            UPDATE tenants
                SET
                    tasks_quota = GREATEST(tasks_quota, 100),
                    collections_quota = GREATEST(collections_quota, 10000)
                WHERE tenants.tenant = $1
                AND (tasks_quota <= 10 OR collections_quota <= 500)
            "#,
            tenant_name,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
