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

/// Advertising platform that supplied a signup click identifier.
#[derive(async_graphql::Enum, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AdAttributionProvider {
    Reddit,
    Linkedin,
}

#[derive(async_graphql::InputObject, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdClickInput {
    pub provider: AdAttributionProvider,
    /// Opaque platform click identifier. Blank IDs and IDs over 256 bytes are ignored.
    pub click_id: String,
    /// When the ad click occurred, as recorded by the client. Not the signup time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clicked_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(async_graphql::InputObject, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SignupAttributionInput {
    /// At most the first 16 entries are considered; the first valid ID per provider is kept.
    #[graphql(default)]
    pub ad_clicks: Vec<AdClickInput>,
    /// Client-recorded UTM parameters from the signup journey, independent of ad clicks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub utm: Option<UtmAttributionInput>,
}

/// UTM values are case-sensitive. Blank values and values over 256 bytes are ignored.
#[derive(async_graphql::InputObject, serde::Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UtmAttributionInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub medium: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub campaign: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub term: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

impl UtmAttributionInput {
    fn normalize(mut self) -> Option<Self> {
        let mut has_value = false;
        for field in [
            &mut self.source,
            &mut self.medium,
            &mut self.campaign,
            &mut self.term,
            &mut self.content,
            &mut self.id,
        ] {
            *field = field
                .take()
                .filter(|value| !value.trim().is_empty() && value.len() <= 256);
            has_value |= field.is_some();
        }
        has_value.then_some(self)
    }
}

impl SignupAttributionInput {
    fn normalize(self) -> Option<Self> {
        // Advertising data must not prevent provisioning. Bound stored data while
        // keeping IDs opaque, and allow each platform to attribute independently.
        let mut ad_clicks: Vec<AdClickInput> = Vec::new();
        for click in self.ad_clicks.into_iter().take(16) {
            if click.click_id.trim().is_empty()
                || click.click_id.len() > 256
                || ad_clicks.iter().any(|kept| kept.provider == click.provider)
            {
                continue;
            }
            ad_clicks.push(click);
        }
        let utm = self.utm.and_then(UtmAttributionInput::normalize);
        (!ad_clicks.is_empty() || utm.is_some()).then_some(Self { ad_clicks, utm })
    }
}

#[derive(Debug, Default)]
pub struct TenantMutation;

#[async_graphql::Object]
impl TenantMutation {
    /// Create a tenant for the authenticated user. Users with an existing direct
    /// tenant-admin grant cannot provision another tenant.
    async fn tenant_create(
        &self,
        ctx: &async_graphql::Context<'_>,
        #[graphql(desc = "Organization name as a single catalog token, without a trailing slash.")]
        name: String,
        #[graphql(desc = "ID of the latest MSA terms the submitting user has read and accepts.")]
        submitting_user_agrees_to_terms_id: models::Id,
        survey: Option<async_graphql::Json<serde_json::Value>>,
        attribution: Option<SignupAttributionInput>,
    ) -> async_graphql::Result<bool> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env
            .claims()
            .map_err(|_| async_graphql::Error::new("Authentication is required"))?;

        // Agents using restricted tokens cannot create tenants (for now)
        if claims.capability_mask.is_some() || claims.prefix_scope.is_some() {
            return Err(async_graphql::Error::new(
                "Restricted tokens cannot create tenants",
            ));
        }
        let terms_id = submitting_user_agrees_to_terms_id;
        let mut txn = env.pg_pool.begin().await?;
        let latest_msa_id: Option<models::Id> = sqlx::query_scalar(
            "SELECT id FROM internal.legal_terms WHERE type = 'msa'
             ORDER BY version DESC LIMIT 1",
        )
        .fetch_optional(&mut *txn)
        .await?;
        if latest_msa_id != Some(terms_id) {
            return Err(async_graphql::Error::new(
                "You must accept the latest MSA terms",
            ));
        }
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
            &name,
            survey.map(|v| v.0).unwrap_or(serde_json::Value::Null),
            attribution.and_then(SignupAttributionInput::normalize),
            &mut txn,
        )
        .await?;
        // Record consent atomically with provisioning, using the stored user identity.
        sqlx::query(
            "INSERT INTO internal.tenant_consent
                (user_id, user_email, terms_id, tenant_name, tenant_id)
             VALUES ($1, (SELECT email FROM auth.users WHERE id = $1), $2::flowid,
                     $3::catalog_tenant, (SELECT id FROM public.tenants WHERE tenant = $3))",
        )
        .bind(claims.sub)
        .bind(terms_id)
        .bind(&tenant_name)
        .execute(&mut *txn)
        .await?;
        txn.commit().await?;

        tracing::info!(user_id = %claims.sub, tenant = %tenant_name, "created tenant");
        // Nested Tenant resolvers could retry authorization with the pre-creation
        // snapshot, replaying this mutation after it has already committed.
        Ok(true)
    }
}

async fn create_tenant(
    user_id: uuid::Uuid,
    name: &str,
    survey: serde_json::Value,
    attribution: Option<SignupAttributionInput>,
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
    let mut metadata = if survey.is_null() {
        serde_json::json!({})
    } else {
        serde_json::json!({ "onboardingSurvey": survey })
    };

    if let Some(attribution) = attribution {
        metadata["signupAttribution"] = serde_json::to_value(attribution)?;
    }

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

    const TERMS_ID: &str = "00:00:00:00:00:00:00:01";
    const STALE_ID: &str = "00:00:00:00:00:00:00:04";
    const PRIVACY_ID: &str = "00:00:00:00:00:00:00:02";

    const ALICE: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);
    const BOB: uuid::Uuid = uuid::Uuid::from_bytes([0x22; 16]);

    async fn server(pool: &sqlx::PgPool) -> TestServer {
        sqlx::query("INSERT INTO auth.users (id, email) VALUES ($1, 'alice@example.test'), ($2, 'bob@example.test')")
            .bind(ALICE).bind(BOB).execute(pool).await.unwrap();
        sqlx::query("INSERT INTO internal.legal_terms (id, type, version, text) VALUES ($1::flowid, 'msa', 99, 'Test terms'), ($2::flowid, 'privacy_policy', 99, 'Test privacy policy'), ($3::flowid, 'msa', 98, 'Old test MSA terms')")
            .bind(TERMS_ID).bind(PRIVACY_ID).bind(STALE_ID).execute(pool).await.unwrap();
        TestServer::start(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
        )
        .await
    }

    fn request(tenant: &str) -> serde_json::Value {
        serde_json::json!({
            "query": "mutation($name: String!, $submittingUserAgreesToTermsId: Id!, $survey: JSON) { tenantCreate(name: $name, submittingUserAgreesToTermsId: $submittingUserAgreesToTermsId, survey: $survey) }",
            "variables": {
                "name": tenant,
                "submittingUserAgreesToTermsId": TERMS_ID,
                "survey": { "origin": "search", "details": "testing" },
            }
        })
    }

    fn attributed_request(tenant: &str, attribution: serde_json::Value) -> serde_json::Value {
        let mut req = request(tenant);
        req["query"] = serde_json::json!(
            "mutation($name: String!, $submittingUserAgreesToTermsId: Id!, $survey: JSON, $attribution: SignupAttributionInput) { tenantCreate(name: $name, submittingUserAgreesToTermsId: $submittingUserAgreesToTermsId, survey: $survey, attribution: $attribution) }"
        );
        req["variables"]["attribution"] = attribution;
        req
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes"))
    )]
    async fn tenant_create_attribution(pool: sqlx::PgPool) {
        let server = server(&pool).await;
        let token = server.make_access_token(ALICE, Some("alice@example.test"));
        let req = attributed_request(
            "acmeCo",
            serde_json::json!({"adClicks": [
                {"provider": "REDDIT", "clickId": " "},
                {"provider": "LINKEDIN", "clickId": "x".repeat(257)},
                {"provider": "REDDIT", "clickId": "reddit-click", "clickedAt": "2026-09-30T14:30:00-04:00"},
                {"provider": "REDDIT", "clickId": "duplicate-click"},
                {"provider": "LINKEDIN", "clickId": "linkedin-click"}
            ], "utm": {"source": "LinkedIn", "medium": "paid_social", "campaign": "fall_launch",
                "term": "data pipelines", "content": "banner", "id": "campaign-123"}}),
        );
        let response: serde_json::Value = server.graphql(&req, Some(&token)).await;
        assert_eq!(
            response,
            serde_json::json!({"data": {"tenantCreate": true}})
        );
        let metadata: serde_json::Value =
            sqlx::query_scalar("SELECT metadata FROM tenants WHERE tenant = 'acmeCo/'")
                .fetch_one(&pool)
                .await
                .unwrap();
        insta::assert_json_snapshot!(metadata, @r#"
        {
          "onboardingSurvey": {
            "details": "testing",
            "origin": "search"
          },
          "signupAttribution": {
            "adClicks": [
              {
                "clickId": "reddit-click",
                "clickedAt": "2026-09-30T18:30:00Z",
                "provider": "REDDIT"
              },
              {
                "clickId": "linkedin-click",
                "provider": "LINKEDIN"
              }
            ],
            "utm": {
              "campaign": "fall_launch",
              "content": "banner",
              "id": "campaign-123",
              "medium": "paid_social",
              "source": "LinkedIn",
              "term": "data pipelines"
            }
          }
        }
        "#);
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
            "tenantCreate": true
          }
        }
        "#);

        let consent: Vec<serde_json::Value> = sqlx::query_scalar(
            "SELECT jsonb_build_object(
                'userId', c.user_id, 'userEmail', c.user_email,
                'termsId', c.terms_id, 'tenantName', c.tenant_name,
                'tenantMatches', c.tenant_id = t.id)
             FROM internal.tenant_consent c JOIN tenants t ON t.tenant = c.tenant_name ORDER BY c.terms_id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        insta::assert_json_snapshot!(consent, @r#"
        [
          {
            "tenantMatches": true,
            "tenantName": "acmeCo/",
            "termsId": "00:00:00:00:00:00:00:01",
            "userEmail": "alice@example.test",
            "userId": "11111111-1111-1111-1111-111111111111"
          }
        ]
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
    async fn tenant_create_requires_latest_terms(pool: sqlx::PgPool) {
        let server = server(&pool).await;
        let token = server.make_access_token(ALICE, None);
        let mut req = request("acmeCo");
        for invalid_id in ["00:00:00:00:00:00:00:03", STALE_ID, PRIVACY_ID] {
            req["variables"]["submittingUserAgreesToTermsId"] = serde_json::json!(invalid_id);
            let response: serde_json::Value = server.graphql(&req, Some(&token)).await;
            insta::allow_duplicates! {
                insta::assert_json_snapshot!(response["errors"][0]["message"], @r#""You must accept the latest MSA terms""#);
            }
        }
        let counts: (i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM tenants WHERE tenant = 'acmeCo/'),
                    (SELECT count(*) FROM internal.tenant_consent WHERE user_id = $1)",
        )
        .bind(ALICE)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(counts, (0, 0));
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes"))
    )]
    async fn tenant_create_rolls_back_when_consent_fails(pool: sqlx::PgPool) {
        let server = server(&pool).await;
        sqlx::query("UPDATE auth.users SET email = NULL WHERE id = $1")
            .bind(ALICE)
            .execute(&pool)
            .await
            .unwrap();
        let token = server.make_access_token(ALICE, Some("untrusted@example.test"));
        let req = attributed_request(
            "acmeCo",
            serde_json::json!({"adClicks": [
                {"provider": "REDDIT", "clickId": "rollback-click"}
            ]}),
        );
        let response: serde_json::Value = server.graphql(&req, Some(&token)).await;
        assert!(
            response["errors"]
                .as_array()
                .is_some_and(|errors| !errors.is_empty())
        );
        let counts: (i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM tenants WHERE tenant = 'acmeCo/'),
                    (SELECT count(*) FROM internal.tenant_consent WHERE user_id = $1),
                    (SELECT count(*) FROM user_grants WHERE user_id = $1)",
        )
        .bind(ALICE)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(counts, (0, 0, 0));
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
        for (mask, scope) in [
            (Some(vec![]), None),
            (Some(vec!["admin".to_string()]), None),
            (None, Some("acmeCo/".to_string())),
            (None, Some(String::new())),
            (Some(vec!["admin".to_string()]), Some("acmeCo/".to_string())),
        ] {
            let restricted = server.make_restricted_access_token(ALICE, None, mask, scope);
            let denied: serde_json::Value = server.graphql(&req, Some(&restricted)).await;
            assert_eq!(
                denied["errors"][0]["message"], "Restricted tokens cannot create tenants",
                "{denied}"
            );
        }
        let created: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant = 'acmeCo/')")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(!created, "restricted callers must not create a tenant");
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
        without_survey["variables"]
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
        // Fail the metadata write after provisioning has populated every table.
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
