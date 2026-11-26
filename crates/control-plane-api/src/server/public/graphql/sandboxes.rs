//! GraphQL operations over [`crate::sandboxes`].

use async_graphql::Context;
use std::sync::Arc;

/// A Linux machine that runs the user's shell commands.
#[derive(Debug, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct Sandbox {
    #[graphql(skip)]
    pub id: models::Id,
    pub catalog_name: models::Name,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub state: SandboxState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, async_graphql::Enum)]
pub enum SandboxState {
    /// The sandbox's machine is being created.
    Provisioning,
    /// The sandbox's machine can run commands.
    Ready,
    /// The sandbox's machine no longer exists, and never will again. Delete
    /// the sandbox and create another.
    Gone,
}

impl From<crate::sandboxes::Sandbox> for Sandbox {
    fn from(sandbox: crate::sandboxes::Sandbox) -> Self {
        let state = match sandbox.state() {
            crate::sandboxes::State::Provisioning => SandboxState::Provisioning,
            crate::sandboxes::State::Ready => SandboxState::Ready,
            crate::sandboxes::State::Gone => SandboxState::Gone,
        };
        Self {
            id: sandbox.id,
            catalog_name: models::Name::new(sandbox.catalog_name),
            created_at: sandbox.created_at,
            state,
        }
    }
}

#[async_graphql::ComplexObject]
impl Sandbox {
    /// Unexpired credential approvals of the sandbox's workspaces.
    async fn credentials(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Vec<SandboxCredential>> {
        let env = ctx.data::<crate::Envelope>()?;

        let credentials = crate::sandboxes::credentials::fetch_unexpired(&env.pg_pool, self.id)
            .await
            .map_err(|err| {
                tracing::error!(?err, sandbox_id = %self.id, "failed to fetch sandbox credentials");
                async_graphql::Error::new("failed to fetch sandbox credentials")
            })?;

        Ok(credentials
            .into_iter()
            .map(SandboxCredential::from)
            .collect())
    }
}

/// A user's approval of a credential for the commands of a sandbox workspace.
/// A command which names the credential receives, as `FLOW_AUTH_TOKEN`, a
/// token minted from the caller's own authority and restricted to the
/// approval's masks. Nothing secret is stored.
#[derive(Debug, async_graphql::SimpleObject)]
pub struct SandboxCredential {
    pub workspace: String,
    pub name: String,
    /// Capabilities to which minted tokens are restricted.
    pub capability_mask: Vec<String>,
    /// Catalog prefixes to which minted tokens are restricted.
    pub prefix_mask: Vec<String>,
    /// No token is minted from the approval after this time.
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

impl From<crate::sandboxes::credentials::Credential> for SandboxCredential {
    fn from(credential: crate::sandboxes::credentials::Credential) -> Self {
        let crate::sandboxes::credentials::Credential {
            workspace,
            name,
            capability_mask,
            prefix_mask,
            approved_by: _,
            expires_at,
        } = credential;

        Self {
            workspace,
            name,
            capability_mask,
            prefix_mask,
            expires_at,
        }
    }
}

#[derive(Debug, Default)]
pub struct SandboxesQuery;

#[async_graphql::Object]
impl SandboxesQuery {
    async fn sandbox(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
    ) -> async_graphql::Result<Option<Sandbox>> {
        let env = ctx.data::<crate::Envelope>()?;

        // fetch_sandbox will return nothing if the user doesn't own the sandbox at catalog_name
        Ok(fetch_sandbox(env, catalog_name.as_str())
            .await?
            .map(Sandbox::from))
    }

    async fn sandboxes(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<Sandbox>> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        // A sandbox is readable only by the user who created it.
        // TODO: Authorize by prefix once a sandbox read capability exists,
        // passing `authorized_prefixes` for that capability to `fetch`.
        let names = sqlx::query_scalar!(
            "select catalog_name from internal.sandboxes where user_id = $1",
            claims.sub,
        )
        .fetch_all(&env.pg_pool)
        .await
        .map_err(|err| {
            tracing::error!(?err, %claims.sub, "failed to list sandbox names");
            async_graphql::Error::new("failed to list sandboxes")
        })?;

        let sandboxes = crate::sandboxes::fetch(&env.pg_pool, &names, &[])
            .await
            .map_err(|err| {
                tracing::error!(?err, %claims.sub, "failed to list sandboxes");
                async_graphql::Error::new("failed to list sandboxes")
            })?;

        Ok(sandboxes.into_iter().map(Sandbox::from).collect())
    }
}

#[derive(Debug, Default)]
pub struct SandboxesMutation;

#[async_graphql::Object]
impl SandboxesMutation {
    /// Returns once the sandbox's machine is ready.
    async fn sandbox_create(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
    ) -> async_graphql::Result<Sandbox> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;
        super::verify_authorization(
            env,
            catalog_name.as_str(),
            models::authz::CapabilityBit::CreateSandbox,
        )
        .await?;

        let tenant = models::tenant_from(catalog_name.as_str());
        let accepted = super::legal_terms::check_consent(
            &env.pg_pool,
            tenant,
            super::legal_terms::LegalTermsType::Sandbox,
            super::legal_terms::ConsentVersion::Gte(1),
        )
        .await
        .map_err(|error| {
            async_graphql::Error::new_with_source(
                anyhow::Error::new(error).context("checking sandbox agreement acceptance"),
            )
        })?;
        if !accepted {
            return Err(async_graphql::Error::new(format!(
                "The tenant must accept a sandbox agreement before creating sandboxes. Accept at {}",
                consent_url(tenant)
            )));
        }
        let providers = &ctx.data::<Arc<crate::App>>()?.sandbox_providers;

        let sandbox =
            crate::sandboxes::create(providers, &env.pg_pool, claims.sub, catalog_name.as_str())
                .await
                .map_err(|err| match err {
                    crate::sandboxes::CreateError::Other(err) => {
                        tracing::error!(?err, %claims.sub, "failed to create sandbox");
                        async_graphql::Error::new(format!("failed to create sandbox: {err:#}"))
                    }
                    refused => async_graphql::Error::new(refused.to_string()),
                })?;

        tracing::info!(%claims.sub, %sandbox.id, "created sandbox");
        Ok(sandbox.into())
    }

    /// Approves a credential for the commands of a sandbox workspace, which
    /// replaces any approval of the same workspace and name.
    ///
    /// The masks narrow the caller's own authority, as do those of
    /// `POST /api/v1/auth/token`. `capabilityMask` names `CapabilityBundle`
    /// values in any case. Each tenant the masks reach must have accepted a
    /// sandbox agreement. Empty masks are allowed, and make a credential which
    /// identifies the user without granting access.
    async fn sandbox_credential_approve(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
        workspace: String,
        name: String,
        capability_mask: Vec<String>,
        prefix_mask: Vec<String>,
        duration_minutes: i32,
    ) -> async_graphql::Result<SandboxCredential> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        crate::sandboxes::credentials::validate(&workspace, &name, duration_minutes)
            .map_err(async_graphql::Error::new)?;
        let capability_mask = crate::sandboxes::credentials::capability_mask(capability_mask)
            .map_err(async_graphql::Error::new)?;
        let tenants = crate::sandboxes::credentials::consent_tenants(&prefix_mask)
            .map_err(async_graphql::Error::new)?;

        // resolve_sandbox will error if the user doesn't own catalog_name
        let sandbox = resolve_sandbox(env, catalog_name.as_str()).await?;
        crate::server::public::token_exchange::verify_may_mint(claims, &env.pg_pool).await?;

        for tenant in &tenants {
            let accepted = super::legal_terms::check_authorized_consent(
                env,
                tenant,
                super::legal_terms::LegalTermsType::Sandbox,
                super::legal_terms::ConsentVersion::Gte(1),
            )
            .await?;

            if !accepted {
                return Err(async_graphql::Error::new(format!(
                    "Tenant {tenant} must accept a sandbox agreement before sandbox credentials may access it. Accept at {}",
                    consent_url(tenant)
                )));
            }
        }

        let credential = crate::sandboxes::credentials::approve(
            &env.pg_pool,
            sandbox.id,
            &workspace,
            &name,
            &capability_mask,
            &prefix_mask,
            claims.sub,
            duration_minutes,
        )
        .await
        .map_err(|err| {
            tracing::error!(?err, %sandbox.id, "failed to approve sandbox credential");
            async_graphql::Error::new("failed to approve sandbox credential")
        })?;

        tracing::info!(
            %claims.sub,
            %sandbox.id,
            %workspace,
            %name,
            ?capability_mask,
            ?prefix_mask,
            %credential.expires_at,
            "approved sandbox credential"
        );
        Ok(credential.into())
    }

    async fn sandbox_delete(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
    ) -> async_graphql::Result<bool> {
        let env = ctx.data::<crate::Envelope>()?;
        let providers = &ctx.data::<Arc<crate::App>>()?.sandbox_providers;

        // resolve_sandbox will error if the user doesn't own catalog_name
        let sandbox = resolve_sandbox(env, catalog_name.as_str()).await?;

        crate::sandboxes::delete(providers, &env.pg_pool, &sandbox)
            .await
            .map_err(|err| {
                tracing::error!(?err, %sandbox.id, "failed to delete sandbox");
                async_graphql::Error::new(format!("failed to delete sandbox: {err:#}"))
            })?;

        Ok(true)
    }
}

async fn fetch_sandbox(
    env: &crate::Envelope,
    catalog_name: &str,
) -> async_graphql::Result<Option<crate::sandboxes::Sandbox>> {
    let claims = env.claims()?;

    crate::sandboxes::fetch_owned(&env.pg_pool, claims, catalog_name)
        .await
        .map_err(|err| {
            tracing::error!(?err, %catalog_name, %claims.sub, "failed to look up sandbox");
            async_graphql::Error::new("failed to look up sandbox")
        })
}

/// Where a tenant's admin accepts the sandbox agreement.
fn consent_url(tenant: &str) -> url::Url {
    let mut url = url::Url::parse("https://dashboard.estuary.dev/legal-consent").unwrap();
    url.query_pairs_mut()
        .append_pair("type", "sandbox")
        .append_pair("tenant", tenant.trim_end_matches('/'));
    url
}

/// Errors if sandbox not found
async fn resolve_sandbox(
    env: &crate::Envelope,
    catalog_name: &str,
) -> async_graphql::Result<crate::sandboxes::Sandbox> {
    fetch_sandbox(env, catalog_name)
        .await?
        .ok_or_else(|| async_graphql::Error::new("sandbox not found"))
}

#[cfg(test)]
mod test {
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn sandbox_create_requires_agreement(pool: sqlx::PgPool) {
        let user = uuid::Uuid::from_bytes([0x11; 16]);
        sqlx::query("INSERT INTO auth.users (id, email) VALUES ($1, 'alice@example.test')")
            .bind(user)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenants (tenant) VALUES ('acmeCo/'), ('acmeCoOther/')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO user_grants (user_id, object_role, capability, bundles)
             VALUES ($1, 'acmeCo/nested/', 'read', ARRAY['create_sandbox']::capability_bundle[])",
        )
        .bind(user)
        .execute(&pool)
        .await
        .unwrap();
        let server = crate::test_server::TestServer::start(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let token = server.make_access_token(user, None);
        let query = serde_json::json!({"query": r#"mutation {
            sandboxCreate(catalogName: "acmeCo/nested/sandbox") { catalogName }
        }"#});
        let mut outcomes = Vec::new();
        let response: serde_json::Value = server.graphql(&query, Some(&token)).await;
        outcomes.push(response["errors"][0]["message"].clone());

        // MSA consent and another tenant cannot cover this name.
        sqlx::query(
            "INSERT INTO internal.tenant_consent
                 (user_id, user_email, terms_id, tenant_name, tenant_id)
             SELECT $1, 'alice@example.test', l.id, t.tenant, t.id
             FROM internal.legal_terms l CROSS JOIN tenants t
             WHERE l.type = 'msa' OR t.tenant = 'acmeCoOther/'",
        )
        .bind(user)
        .execute(&pool)
        .await
        .unwrap();
        let response: serde_json::Value = server.graphql(&query, Some(&token)).await;
        outcomes.push(response["errors"][0]["message"].clone());

        sqlx::query(
            "INSERT INTO internal.tenant_consent
                 (user_id, user_email, terms_id, tenant_name, tenant_id)
             SELECT $1, 'alice@example.test', l.id, t.tenant, t.id
             FROM internal.legal_terms l CROSS JOIN tenants t
             WHERE l.type = 'sandbox' AND l.version = 1 AND t.tenant = 'acmeCo/'",
        )
        .bind(user)
        .execute(&pool)
        .await
        .unwrap();
        // The test server has no sandbox provider; reaching it proves consent passed.
        let response: serde_json::Value = server.graphql(&query, Some(&token)).await;
        outcomes.push(response["errors"][0]["message"].clone());
        insta::assert_json_snapshot!(outcomes, @r#"
        [
          "The tenant must accept a sandbox agreement before creating sandboxes. Accept at https://dashboard.estuary.dev/legal-consent?type=sandbox&tenant=acmeCo",
          "The tenant must accept a sandbox agreement before creating sandboxes. Accept at https://dashboard.estuary.dev/legal-consent?type=sandbox&tenant=acmeCo",
          "Sandboxes are not configured"
        ]
        "#);
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn sandbox_lifecycle(pool: sqlx::PgPool) {
        let user = uuid::Uuid::from_bytes([0x11; 16]);
        sqlx::query("INSERT INTO auth.users (id, email) VALUES ($1, 'alice@example.test')")
            .bind(user)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenants (tenant) VALUES ('acmeCo/')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO user_grants (user_id, object_role, capability, bundles)
             VALUES ($1, 'acmeCo/', 'read', ARRAY['create_sandbox']::capability_bundle[])",
        )
        .bind(user)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO internal.tenant_consent
                 (user_id, user_email, terms_id, tenant_name, tenant_id)
             SELECT $1, 'alice@example.test', l.id, t.tenant, t.id
             FROM internal.legal_terms l CROSS JOIN tenants t
             WHERE l.type = 'sandbox' AND l.version = 1",
        )
        .bind(user)
        .execute(&pool)
        .await
        .unwrap();

        let fake = std::sync::Arc::new(crate::sandboxes::fake::Fake::new("fake"));
        let providers = crate::sandboxes::Providers::new(
            vec![fake.clone() as std::sync::Arc<dyn crate::sandboxes::provider::Provider>],
            Some("fake"),
        )
        .unwrap();
        let server = crate::test_server::TestServer::start_with_sandbox_providers(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
            providers,
        )
        .await;
        let token = server.make_access_token(user, None);

        let mut outcomes = Vec::new();
        let mut run = async |step: &str, query: &str| {
            let response: serde_json::Value = server
                .graphql(&serde_json::json!({ "query": query }), Some(&token))
                .await;
            outcomes.push(serde_json::json!({ step: response }));
        };

        run(
            "create",
            r#"mutation { sandboxCreate(catalogName: "acmeCo/sandbox") { catalogName state } }"#,
        )
        .await;
        run(
            "createAgain",
            r#"mutation { sandboxCreate(catalogName: "acmeCo/sandbox") { catalogName } }"#,
        )
        .await;
        run("list", r#"query { sandboxes { catalogName state } }"#).await;

        let sandbox = crate::sandboxes::fetch(&pool, &["acmeCo/sandbox".to_string()], &[])
            .await
            .unwrap()
            .pop()
            .unwrap();
        crate::sandboxes::mark_gone(&pool, &sandbox).await.unwrap();
        run(
            "gone",
            r#"query { sandbox(catalogName: "acmeCo/sandbox") { catalogName state } }"#,
        )
        .await;

        run(
            "delete",
            r#"mutation { sandboxDelete(catalogName: "acmeCo/sandbox") }"#,
        )
        .await;
        run(
            "deleteAgain",
            r#"mutation { sandboxDelete(catalogName: "acmeCo/sandbox") }"#,
        )
        .await;
        run(
            "afterDelete",
            r#"query { sandbox(catalogName: "acmeCo/sandbox") { catalogName } }"#,
        )
        .await;

        let ops: Vec<_> = fake.calls().into_iter().map(|(op, _handle)| op).collect();
        insta::assert_json_snapshot!(serde_json::json!({"outcomes": outcomes, "calls": ops}));
    }
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn sandbox_credential_approvals(pool: sqlx::PgPool) {
        let alice = uuid::Uuid::from_bytes([0x11; 16]);
        let bob = uuid::Uuid::from_bytes([0x22; 16]);
        sqlx::query(
            "INSERT INTO auth.users (id, email)
             VALUES ($1, 'alice@example.test'), ($2, 'bob@example.test')",
        )
        .bind(alice)
        .bind(bob)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO tenants (tenant) VALUES ('acmeCo/'), ('otherCo/'), ('thirdCo/')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO user_grants (user_id, object_role, capability, bundles) VALUES
             ($1, 'acmeCo/', 'read', ARRAY['create_sandbox']::capability_bundle[]),
             ($1, 'otherCo/', 'read', ARRAY[]::capability_bundle[]),
             ($2, 'acmeCo/', 'read', ARRAY[]::capability_bundle[])",
        )
        .bind(alice)
        .bind(bob)
        .execute(&pool)
        .await
        .unwrap();
        // Only acmeCo/ has accepted the sandbox agreement.
        sqlx::query(
            "INSERT INTO internal.tenant_consent
                 (user_id, user_email, terms_id, tenant_name, tenant_id)
             SELECT $1, 'alice@example.test', l.id, t.tenant, t.id
             FROM internal.legal_terms l CROSS JOIN tenants t
             WHERE l.type = 'sandbox' AND l.version = 1 AND t.tenant = 'acmeCo/'",
        )
        .bind(alice)
        .execute(&pool)
        .await
        .unwrap();

        let fake = std::sync::Arc::new(crate::sandboxes::fake::Fake::new("fake"));
        let providers = crate::sandboxes::Providers::new(
            vec![fake.clone() as std::sync::Arc<dyn crate::sandboxes::provider::Provider>],
            Some("fake"),
        )
        .unwrap();
        let server = crate::test_server::TestServer::start_with_sandbox_providers(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
            providers,
        )
        .await;
        let alice_token = server.make_access_token(alice, Some("alice@example.test"));
        let bob_token = server.make_access_token(bob, Some("bob@example.test"));
        let restricted_token = server.make_restricted_access_token(
            alice,
            Some("alice@example.test"),
            Some(Vec::new()),
            None,
        );

        let mut outcomes = Vec::new();
        let mut run = async |step: &str, token: &str, query: &str| {
            let response: serde_json::Value = server
                .graphql(&serde_json::json!({ "query": query }), Some(token))
                .await;
            outcomes.push(serde_json::json!({ step: response }));
        };
        let approve = |workspace: &str,
                       name: &str,
                       capability_mask: &str,
                       prefixes: &str,
                       minutes: i32| {
            format!(
                r#"mutation {{ sandboxCredentialApprove(
                    catalogName: "acmeCo/sandbox", workspace: "{workspace}", name: "{name}",
                    capabilityMask: {capability_mask}, prefixMask: {prefixes}, durationMinutes: {minutes}
                ) {{ workspace name capabilityMask prefixMask }} }}"#
            )
        };

        run(
            "create",
            &alice_token,
            r#"mutation { sandboxCreate(catalogName: "acmeCo/sandbox") { catalogName } }"#,
        )
        .await;

        // An empty-scope credential needs no consent.
        run(
            "approveEmpty",
            &alice_token,
            &approve("ws", "ident", "[]", "[]", 60),
        )
        .await;
        run(
            "approveScoped",
            &alice_token,
            &approve(
                "ws",
                "reader",
                r#"["Viewer", "viewer"]"#,
                r#"["acmeCo/", "acmeCo/nested/", "ops/dp/public/", "ops/dp/private/acmeCo/"]"#,
                480,
            ),
        )
        .await;

        for (step, token, query) in [
            (
                "invalidWorkspace",
                &alice_token,
                approve("WS", "x", "[]", "[]", 60),
            ),
            (
                "invalidName",
                &alice_token,
                approve("ws", "-x", "[]", "[]", 60),
            ),
            (
                "zeroDuration",
                &alice_token,
                approve("ws", "x", "[]", "[]", 0),
            ),
            (
                "longDuration",
                &alice_token,
                approve("ws", "x", "[]", "[]", 481),
            ),
            (
                "unknownCapability",
                &alice_token,
                approve("ws", "x", r#"["viewer", "read"]"#, "[]", 60),
            ),
            (
                "invalidPrefix",
                &alice_token,
                approve("ws", "x", "[]", r#"["acmeCo"]"#, 60),
            ),
            (
                "unsupportedPrefix",
                &alice_token,
                approve("ws", "x", "[]", r#"["ops/tasks/"]"#, 60),
            ),
            (
                "consentMissing",
                &alice_token,
                approve("ws", "x", "[]", r#"["acmeCo/", "otherCo/"]"#, 60),
            ),
            (
                "privateDataPlaneConsentMissing",
                &alice_token,
                approve("ws", "x", "[]", r#"["ops/dp/private/otherCo/"]"#, 60),
            ),
            (
                "tenantUnauthorized",
                &alice_token,
                approve("ws", "x", "[]", r#"["thirdCo/"]"#, 60),
            ),
            ("notOwner", &bob_token, approve("ws", "x", "[]", "[]", 60)),
            (
                "restrictedCaller",
                &restricted_token,
                approve("ws", "x", "[]", "[]", 60),
            ),
        ] {
            run(step, token, &query).await;
        }

        // Approving again replaces the approval.
        run(
            "replace",
            &alice_token,
            &approve("ws", "reader", r#"["admin"]"#, r#"["acmeCo/nested/"]"#, 30),
        )
        .await;
        run(
            "otherWorkspace",
            &alice_token,
            &approve("other-ws", "reader", "[]", "[]", 1),
        )
        .await;

        sqlx::query(
            "UPDATE internal.sandbox_credentials SET expires_at = now() - interval '1 minute'
             WHERE name = 'ident'",
        )
        .execute(&pool)
        .await
        .unwrap();

        run(
            "list",
            &alice_token,
            r#"query { sandbox(catalogName: "acmeCo/sandbox") {
                credentials { workspace name capabilityMask prefixMask }
            } }"#,
        )
        .await;

        // Each row's duration and approver. The expired row remains.
        let rows: Vec<(String, String, i32, bool)> = sqlx::query_as(
            "SELECT workspace, name,
                    extract(epoch from expires_at - created_at)::int4,
                    approved_by = $1
             FROM internal.sandbox_credentials ORDER BY workspace, name",
        )
        .bind(alice)
        .fetch_all(&pool)
        .await
        .unwrap();

        insta::assert_json_snapshot!(serde_json::json!({"outcomes": outcomes, "rows": rows}));
    }
}
