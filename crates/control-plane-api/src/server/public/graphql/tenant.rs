use async_graphql::{Context, Result, SimpleObject};
use validator::Validate;

#[derive(Debug, Default)]
pub struct TenantQuery;

#[async_graphql::Object]
impl TenantQuery {
    async fn tenant(&self, ctx: &Context<'_>, name: String) -> Result<Option<Tenant>> {
        let env = ctx.data::<crate::Envelope>()?;
        let tenant = validate_tenant_name(&name)?;

        super::verify_authorization(env, tenant.as_str(), models::Capability::Read).await?;

        let exists: bool = sqlx::query_scalar!(
            r#"SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant = $1) AS "exists!""#,
            tenant.as_str(),
        )
        .fetch_one(&env.pg_pool)
        .await?;
        if !exists {
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

pub(super) fn validate_tenant_name(name: &str) -> Result<models::Prefix> {
    let prefix = models::Prefix::new(name);
    prefix
        .validate()
        .map_err(|err| async_graphql::Error::new(format!("invalid tenant name: {err}")))?;
    Ok(prefix)
}
