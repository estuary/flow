use async_graphql::{Context, Result, SimpleObject};
use validator::Validate;

#[derive(Debug, Default)]
pub struct TenantQuery;

#[async_graphql::Object]
impl TenantQuery {
    async fn tenant(&self, ctx: &Context<'_>, name: String) -> Result<Option<Tenant>> {
        let env = ctx.data::<crate::Envelope>()?;
        let tenant = validate_tenant_name(&name)?;

        env.verify_authorization(tenant.as_str(), models::Capability::Read)
            .await?;

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

#[cfg(test)]
mod tests {
    use crate::test_server;

    // `tenant` gates on Read through `verify_authorization`. A mask that
    // withholds part of Read is refused by naming the withheld bits, before
    // any grant is consulted; a mask enabling Read passes to the lookup.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_tenant_honors_capability_mask(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;
        let alice = uuid::Uuid::from_bytes([0x11; 16]);
        let query = serde_json::json!({
            "query": r#"query { tenant(name: "aliceCo/") { name } }"#
        });

        let journal_only =
            server.make_masked_access_token(alice, Some("alice@example.test"), &["JournalRead"]);
        let response: serde_json::Value = server.graphql(&query, Some(&journal_only)).await;
        insta::assert_json_snapshot!(response, @r#"
        {
          "data": null,
          "errors": [
            {
              "locations": [
                {
                  "column": 9,
                  "line": 1
                }
              ],
              "message": "PermissionDenied: token does not enable capabilities [CatalogRead, ViewDataPlanePrivateNetworking] required to access prefix or name 'aliceCo/'",
              "path": [
                "tenant"
              ]
            }
          ]
        }
        "#);

        // No `tenants` row exists for aliceCo/ in the fixture, so a passing
        // check resolves to null rather than an error.
        let viewer =
            server.make_masked_access_token(alice, Some("alice@example.test"), &["Viewer"]);
        let response: serde_json::Value = server.graphql(&query, Some(&viewer)).await;
        insta::assert_json_snapshot!(response, @r#"
        {
          "data": {
            "tenant": null
          }
        }
        "#);
    }
}
