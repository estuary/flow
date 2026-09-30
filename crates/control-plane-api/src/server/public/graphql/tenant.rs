use async_graphql::{Context, Result, SimpleObject};
use validator::Validate;

const TENANT_UNAVAILABLE_MESSAGE: &str = "The organization name is already in use, \
    please choose a different one or contact support@estuary.dev.";

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

#[derive(Debug, Default)]
pub struct TenantMutation;

#[derive(async_graphql::InputObject)]
pub struct TenantCreateInput {
    /// Organization name as a single catalog token, without a trailing slash.
    pub name: String,
    pub survey: Option<async_graphql::Json<serde_json::Value>>,
}

#[derive(async_graphql::SimpleObject)]
pub struct TenantCreatePayload {
    pub tenant: Tenant,
}

#[async_graphql::Object]
impl TenantMutation {
    /// Create a tenant for the authenticated user. Users with an existing direct
    /// tenant-admin grant cannot provision another tenant.
    async fn tenant_create(
        &self,
        ctx: &async_graphql::Context<'_>,
        input: TenantCreateInput,
    ) -> async_graphql::Result<TenantCreatePayload> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env
            .claims()
            .map_err(|_| async_graphql::Error::new("Authentication is required"))?;

        // Agents using restricted tokens cannot create tenants (for now)
        if claims.capability_mask.is_some() {
            return Err(async_graphql::Error::new(
                "Restricted tokens cannot create tenants",
            ));
        }
        let mut txn = env.pg_pool.begin().await?;
        let is_service_account = sqlx::query_scalar!(
            "SELECT EXISTS(SELECT 1 FROM internal.service_accounts WHERE user_id = $1) AS \"exists!\"",
            claims.sub,
        )
        .fetch_one(&mut *txn)
        .await?;
        if is_service_account {
            return Err(async_graphql::Error::new(
                "Service accounts cannot create tenants",
            ));
        }

        let tenant_name = create_tenant(
            claims.sub,
            &input.name,
            input.survey.map(|v| v.0).unwrap_or(serde_json::Value::Null),
            &mut txn,
        )
        .await?;
        txn.commit().await?;

        tracing::info!(user_id = %claims.sub, tenant = %tenant_name, "created tenant");
        Ok(TenantCreatePayload {
            tenant: Tenant {
                name: tenant_name,
                sensitive: false,
            },
        })
    }
}

async fn create_tenant(
    user_id: uuid::Uuid,
    name: &str,
    survey: serde_json::Value,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> async_graphql::Result<String> {
    models::Token::new(name)
        .validate()
        .map_err(|_| async_graphql::Error::new("Invalid organization name"))?;

    if !unicode_normalization::is_nfkc(name) {
        return Err(async_graphql::Error::new("Invalid organization name"));
    }

    // Lock this user to prevent concurrent tenant create mutations.
    sqlx::query!(
        "SELECT id FROM auth.users WHERE id = $1 FOR UPDATE",
        user_id
    )
    .fetch_one(&mut **txn)
    .await?;

    if crate::directives::beta_onboard::is_user_provisioned(user_id, txn).await? {
        return Err(async_graphql::Error::new(
            "Cannot provision a new tenant because the user has existing grants",
        ));
    }
    let tenant_name = format!("{}/", name);
    let banned = sqlx::query!(
        r#"
        select 1 as "exists" from internal.illegal_tenant_names
        where lower(name) = lower($1::catalog_tenant)
        "#,
        tenant_name.clone() as String,
    )
    .fetch_optional(&mut **txn)
    .await?;
    if banned.is_some() {
        return Err(async_graphql::Error::new(TENANT_UNAVAILABLE_MESSAGE));
    }

    crate::directives::beta_onboard::provision_tenant(
        // TODO: remove unused email param when retiring betaOnboard directive
        "",
        Some("created via tenantCreate".to_string()),
        name,
        user_id,
        txn,
    )
    .await
    .map_err(|err| {
        if let sqlx::Error::Database(db) = &err {
            if matches!(
                db.constraint(),
                Some("tenants_tenant_key" | "tenants_tenant_lower_key")
            ) {
                return async_graphql::Error::new(TENANT_UNAVAILABLE_MESSAGE);
            }
        }
        err.into()
    })?;
    let metadata = if survey.is_null() {
        serde_json::json!({})
    } else {
        serde_json::json!({ "onboardingSurvey": survey })
    };

    sqlx::query!(
        r#"UPDATE public.tenants
           SET created_by = $1,
               metadata = metadata || $2::jsonb
           WHERE tenant = $3"#,
        user_id,
        metadata,
        &tenant_name as &str,
    )
    .execute(&mut **txn)
    .await?;

    Ok(tenant_name)
}

#[cfg(test)]
mod test {
    use crate::test_server::TestServer;

    const ALICE: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);
    const BOB: uuid::Uuid = uuid::Uuid::from_bytes([0x22; 16]);

    async fn server(pool: &sqlx::PgPool) -> TestServer {
        sqlx::query("INSERT INTO auth.users (id, email) VALUES ($1, 'alice@example.test'), ($2, 'bob@example.test')")
            .bind(ALICE).bind(BOB).execute(pool).await.unwrap();
        TestServer::start(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
        )
        .await
    }

    fn request(tenant: &str) -> serde_json::Value {
        serde_json::json!({
            "query": "mutation($input: TenantCreateInput!) { tenantCreate(input: $input) { tenant { name } } }",
            "variables": { "input": {
                "name": tenant,
                "survey": { "origin": "search", "details": "testing" },
            }}
        })
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes"))
    )]
    async fn tenant_create_success(pool: sqlx::PgPool) {
        let server = server(&pool).await;
        let token = server.make_access_token(ALICE, Some("alice@example.test"));
        let req = request("acmeCo");
        let response: serde_json::Value = server.graphql(&req, Some(&token)).await;
        insta::assert_json_snapshot!(response, @r#"
        {
          "data": {
            "tenantCreate": {
              "tenant": {
                "name": "acmeCo/"
              }
            }
          }
        }
        "#);

        let state: serde_json::Value = sqlx::query_scalar(r#"SELECT jsonb_build_object(
            'creator', created_by,
            'metadata', metadata,
            'isAdmin', EXISTS(SELECT 1 FROM user_grants WHERE user_id = $1 AND object_role = tenant AND capability = 'admin')
        ) FROM tenants WHERE tenant = 'acmeCo/'"#)
            .bind(ALICE).fetch_one(&pool).await.unwrap();
        insta::assert_json_snapshot!(state, @r#"
        {
          "creator": "11111111-1111-1111-1111-111111111111",
          "isAdmin": true,
          "metadata": {
            "onboardingSurvey": {
              "details": "testing",
              "origin": "search"
            }
          }
        }
        "#);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes"))
    )]
    async fn tenant_create_validation_and_auth(pool: sqlx::PgPool) {
        let server = server(&pool).await;
        let token = server.make_access_token(ALICE, None);
        let req = request("acmeCo");
        let unauth: serde_json::Value = server.graphql(&req, None).await;
        assert_eq!(unauth["errors"][0]["message"], "Authentication is required");
        let masked = server.make_masked_access_token(ALICE, None, Some(vec![]));
        let denied: serde_json::Value = server.graphql(&req, Some(&masked)).await;
        assert_eq!(
            denied["errors"][0]["message"],
            "Restricted tokens cannot create tenants"
        );
        sqlx::query("INSERT INTO internal.service_accounts (user_id, catalog_name, created_by) VALUES ($1, 'acmeCo/bot', $2)")
            .bind(BOB).bind(ALICE).execute(&pool).await.unwrap();
        let bot = server.make_access_token(BOB, None);
        let denied: serde_json::Value = server.graphql(&req, Some(&bot)).await;
        assert_eq!(
            denied["errors"][0]["message"],
            "Service accounts cannot create tenants"
        );

        for name in ["invalid/name", "Ａcme"] {
            let response: serde_json::Value = server.graphql(&request(name), Some(&token)).await;
            assert_eq!(
                response["errors"][0]["message"], "Invalid organization name",
                "{response}"
            );
        }
        // Reserved names and existing names are both case insensitive.
        sqlx::query("INSERT INTO tenants (tenant) VALUES ('takenCo/')")
            .execute(&pool)
            .await
            .unwrap();
        for name in ["AdMiN", "TaKeNcO"] {
            let response: serde_json::Value = server.graphql(&request(name), Some(&token)).await;
            assert_eq!(
                response["errors"][0]["message"],
                super::TENANT_UNAVAILABLE_MESSAGE,
                "{response}"
            );
        }

        let mut without_survey = request("acmeCo");
        without_survey["variables"]["input"]
            .as_object_mut()
            .unwrap()
            .remove("survey");
        let response: serde_json::Value = server.graphql(&without_survey, Some(&token)).await;
        assert!(response.get("errors").is_none(), "{response}");

        let metadata: serde_json::Value =
            sqlx::query_scalar("SELECT metadata FROM tenants WHERE tenant = 'acmeCo/'")
                .fetch_one(&pool)
                .await
                .unwrap();
        insta::assert_json_snapshot!(metadata, @r#"{}"#);

        let response: serde_json::Value = server.graphql(&request("anotherCo"), Some(&token)).await;
        assert_eq!(
            response["errors"][0]["message"],
            "Cannot provision a new tenant because the user has existing grants",
            "{response}"
        );
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes"))
    )]
    async fn tenant_create_rejects_reserved_names(pool: sqlx::PgPool) {
        let server = server(&pool).await;
        let token = server.make_access_token(ALICE, None);

        for name in [
            "ops",
            "oPs",
            "recovery",
            "ReCoVeRy",
            "ops.us-central1.v1",
            "Ops.Us-Central1.V1",
        ] {
            let response: serde_json::Value = server.graphql(&request(name), Some(&token)).await;
            assert_eq!(
                response["errors"][0]["message"],
                super::TENANT_UNAVAILABLE_MESSAGE,
                "{response}"
            );
        }

        let count: i64 = sqlx::query_scalar(
            "SELECT (SELECT count(*) FROM tenants WHERE lower(tenant) IN ('ops/', 'recovery/', 'ops.us-central1.v1/'))
                  + (SELECT count(*) FROM user_grants WHERE user_id = $1)",
        )
        .bind(ALICE)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 0);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes"))
    )]
    async fn tenant_create_rollback(pool: sqlx::PgPool) {
        let server = server(&pool).await;
        let token = server.make_access_token(ALICE, None);
        // Fail the last write, after provisioning has populated every table.
        sqlx::raw_sql("CREATE FUNCTION internal.reject_signup() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test failure'; END $$; CREATE TRIGGER reject_signup BEFORE UPDATE OF metadata ON public.tenants FOR EACH ROW EXECUTE FUNCTION internal.reject_signup();")
            .execute(&pool).await.unwrap();
        let req = request("acmeCo");
        let response: serde_json::Value = server.graphql(&req, Some(&token)).await;
        assert!(response.get("errors").is_some(), "{response}");
        let count: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM tenants WHERE tenant = 'acmeCo/') + (SELECT count(*) FROM user_grants WHERE object_role = 'acmeCo/') + (SELECT count(*) FROM role_grants WHERE subject_role = 'acmeCo/' OR object_role = 'acmeCo/') + (SELECT count(*) FROM storage_mappings WHERE catalog_prefix IN ('acmeCo/', 'recovery/acmeCo/')) + (SELECT count(*) FROM alert_subscriptions WHERE catalog_prefix = 'acmeCo/')")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(count, 0);
        sqlx::query("DROP TRIGGER reject_signup ON public.tenants")
            .execute(&pool)
            .await
            .unwrap();
        let retry: serde_json::Value = server.graphql(&req, Some(&token)).await;
        assert!(retry.get("errors").is_none(), "{retry}");
    }
}
