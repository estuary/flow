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

        let Some(sensitive) = sqlx::query_scalar!(
            r#"SELECT sensitive FROM tenants WHERE tenant = $1"#,
            tenant.as_str(),
        )
        .fetch_optional(&env.pg_pool)
        .await?
        else {
            return Ok(None);
        };

        Ok(Some(Tenant {
            name: tenant.to_string(),
            sensitive,
        }))
    }
}

#[derive(Debug, Clone, SimpleObject)]
#[graphql(complex)]
pub struct Tenant {
    pub name: String,
    pub sensitive: bool,
}

pub(super) fn validate_tenant_name(name: &str) -> Result<models::Prefix> {
    let prefix = models::Prefix::new(name);
    prefix
        .validate()
        .map_err(|err| async_graphql::Error::new(format!("invalid tenant name: {err}")))?;
    Ok(prefix)
}

#[cfg(test)]
mod tests {
    use crate::test_server;
    use serde_json::json;

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("sso_tenant", "bob_co", "bob_co2"))
    )]
    async fn graphql_tenant_sensitive(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server =
            test_server::TestServer::start(pool.clone(), test_server::snapshot(pool, false).await)
                .await;
        let token = server.make_access_token(uuid::Uuid::from_bytes([0x22; 16]), None);

        let response: serde_json::Value = server
            .graphql(
                &json!({
                    "query": r#"
                        query {
                          bobCo: tenant(name: "bobCo/") { name sensitive }
                          bobCo2: tenant(name: "bobCo2/") { name sensitive }
                        }
                    "#
                }),
                Some(&token),
            )
            .await;

        insta::assert_json_snapshot!(response, @r#"
        {
          "data": {
            "bobCo": {
              "name": "bobCo/",
              "sensitive": false
            },
            "bobCo2": {
              "name": "bobCo2/",
              "sensitive": true
            }
          }
        }
        "#);
    }
}
