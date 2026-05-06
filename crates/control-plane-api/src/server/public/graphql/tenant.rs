use async_graphql::{Context, Result, SimpleObject};
use validator::Validate;

#[derive(Debug, Default)]
pub struct TenantQuery;

#[async_graphql::Object]
impl TenantQuery {
    async fn tenant(&self, ctx: &Context<'_>, name: String) -> Result<Option<Tenant>> {
        let env = ctx.data::<crate::Envelope>()?;
        let tenant = validate_tenant_name(&name)?;

        verify_tenant(env, tenant.as_str(), models::Capability::Read).await?;

        if !tenant_exists(&env.pg_pool, tenant.as_str()).await? {
            return Ok(None);
        }

        Ok(Some(Tenant {
            name: tenant.to_string(),
        }))
    }
}

#[derive(Debug, Clone, SimpleObject)]
#[graphql(complex)]
pub struct Tenant {
    pub name: String,
}

pub(super) async fn tenant_exists(pool: &sqlx::PgPool, tenant: &str) -> Result<bool> {
    let exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS(
            SELECT 1
            FROM tenants
            WHERE tenant = $1
        )
        "#,
    )
    .bind(tenant)
    .fetch_one(pool)
    .await?;

    Ok(exists)
}

pub(super) async fn verify_tenant(
    env: &crate::Envelope,
    tenant: &str,
    capability: models::Capability,
) -> Result<()> {
    let policy_result = crate::server::evaluate_names_authorization(
        env.snapshot(),
        env.claims()?,
        capability,
        [tenant],
    );
    let (_expiry, ()) = env.authorization_outcome(policy_result).await?;
    Ok(())
}

pub(super) fn validate_tenant_name(name: &str) -> Result<models::Prefix> {
    let prefix = models::Prefix::new(name);
    prefix
        .validate()
        .map_err(|err| async_graphql::Error::new(format!("invalid tenant name: {err}")))?;
    Ok(prefix)
}
