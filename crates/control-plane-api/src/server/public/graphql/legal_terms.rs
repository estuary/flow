#[derive(Debug, Default)]
pub struct LegalTermsQuery;

#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    async_graphql::Enum,
    sqlx::Type,
)]
#[sqlx(type_name = "internal.legal_terms_type", rename_all = "snake_case")]
pub enum LegalTermsType {
    /// Master Services Agreement.
    Msa,
}

#[derive(async_graphql::SimpleObject, sqlx::FromRow)]
pub struct LegalTerms {
    pub id: models::Id,
    pub text: String,
    pub r#type: LegalTermsType,
    pub version: i32,
}

#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct LegalConsentVersion {
    pub eq: Option<i32>,
    pub gte: Option<i32>,
}

fn validate_tenant(tenant: &String) -> Result<(), async_graphql::InputValueError<String>> {
    async_graphql::validators::regex(tenant, "^[^/]+/$").map_err(|_| {
        "Tenant must be a top-level name followed by '/', for example 'acmeCo/'".into()
    })
}

#[async_graphql::Object]
impl LegalTermsQuery {
    /// Returns the latest legal terms of the given type.
    async fn legal_terms(
        &self,
        ctx: &async_graphql::Context<'_>,
        r#type: LegalTermsType,
    ) -> async_graphql::Result<Option<LegalTerms>> {
        let env = ctx.data::<crate::Envelope>()?;
        let row = sqlx::query_as::<_, LegalTerms>(
            "SELECT id, text, type, version FROM internal.legal_terms
             WHERE type = $1 ORDER BY version DESC LIMIT 1",
        )
        .bind(r#type)
        .fetch_optional(&env.pg_pool)
        .await?;

        Ok(row)
    }

    /// Whether the tenant has accepted matching legal terms. Requires CatalogRead anywhere within the tenant.
    async fn legal_check(
        &self,
        ctx: &async_graphql::Context<'_>,
        #[graphql(validator(custom = "validate_tenant"))] tenant: String,
        r#type: LegalTermsType,
        version: LegalConsentVersion,
    ) -> async_graphql::Result<bool> {
        if version.eq.is_some() == version.gte.is_some() {
            return Err(async_graphql::Error::new(
                "Provide exactly one of version.eq or version.gte",
            ));
        }

        let env = ctx.data::<crate::Envelope>()?;
        let capability = models::authz::Capability::CatalogRead;
        let prefixes = super::authorized_prefixes::authorized_prefixes(
            &env.snapshot().role_grants,
            &env.snapshot().user_grants,
            &env.claims()?.subject(),
            capability,
            Some(&tenant),
        );
        if prefixes.is_empty() {
            // Refresh stale permissions before reporting a terminal denial.
            super::verify_authorization(env, &tenant, capability).await?;
        }

        // Audit records survive tenant deletion; consent must match the current tenant ID.
        let accepted: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM internal.tenant_consent c
                 JOIN public.tenants t ON t.id = c.tenant_id
                 JOIN internal.legal_terms l ON l.id = c.terms_id
                 WHERE t.tenant = $1 AND l.type = $2
                   AND ($3::integer IS NULL OR l.version = $3)
                   AND ($4::integer IS NULL OR l.version >= $4)
             )",
        )
        .bind(&tenant)
        .bind(r#type)
        .bind(version.eq)
        .bind(version.gte)
        .fetch_one(&env.pg_pool)
        .await?;
        Ok(accepted)
    }
}

#[derive(Debug, Default)]
pub struct LegalTermsMutation;

#[async_graphql::Object]
impl LegalTermsMutation {
    /// Accept the latest legal terms of their type. Requires a direct ManageLegal grant and an unrestricted user token.
    async fn legal_accept(
        &self,
        ctx: &async_graphql::Context<'_>,
        #[graphql(validator(custom = "validate_tenant"))] tenant: String,
        terms_id: models::Id,
    ) -> async_graphql::Result<LegalTerms> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;
        if claims.is_restricted() {
            return Err(async_graphql::Error::new(
                "Restricted tokens cannot accept legal terms",
            ));
        }
        // Legal consent must come from the user's direct grants, not inherited admin roles.
        // TODO: Use standard authorization checks once the legacy admin bundle is split up.
        let capability = models::authz::Capability::ManageLegal;
        let authorized = tables::UserGrant::is_authorized(
            &[],
            &env.snapshot().user_grants,
            &claims.subject(),
            tenant.as_str(),
            capability,
        );
        let outcome = if authorized {
            Ok((None, ()))
        } else {
            Err(tonic::Status::permission_denied(
                "Accepting legal terms requires a direct user grant on the tenant",
            ))
        };
        if let Err(error) = env.authorization_outcome(outcome).await {
            if matches!(&error, crate::ApiError::Status(status) if status.code() == tonic::Code::PermissionDenied)
            {
                return Err(async_graphql::Error::new_with_source(
                    super::authorization::AuthorizationDenied {
                        capability,
                        message: async_graphql::Error::from(error).message,
                    },
                ));
            }
            return Err(error.into());
        }
        if crate::server::is_service_account(&env.pg_pool, claims.sub).await? {
            return Err(async_graphql::Error::new(
                "Service accounts cannot accept legal terms",
            ));
        }

        // Return eligible terms even when another request records consent first.
        let terms = sqlx::query_as::<_, LegalTerms>(
            "WITH terms AS (
                 SELECT l.id, l.text, l.type, l.version,
                        u.id AS user_id, u.email, t.tenant, t.id AS tenant_id
                 FROM auth.users u CROSS JOIN public.tenants t CROSS JOIN internal.legal_terms l
                 WHERE u.id = $1 AND t.tenant = $2 AND l.id = $3
                   AND NOT EXISTS (
                       SELECT 1 FROM internal.legal_terms newer
                       WHERE newer.type = l.type AND newer.version > l.version
                   )
             ), consent AS (
                 INSERT INTO internal.tenant_consent
                     (user_id, user_email, terms_id, tenant_name, tenant_id)
                 SELECT user_id, email, id, tenant, tenant_id FROM terms
                 ON CONFLICT (tenant_id, terms_id) DO NOTHING
             )
             SELECT id, text, type, version FROM terms",
        )
        .bind(claims.sub)
        .bind(tenant.as_str())
        .bind(terms_id)
        .fetch_optional(&env.pg_pool)
        .await
        .map_err(|error| {
            async_graphql::Error::new_with_source(
                anyhow::Error::new(error).context("recording acceptance of legal terms"),
            )
        })?;
        terms.ok_or_else(|| {
            async_graphql::Error::new(
                "User or tenant not found, or legal terms are not the latest version",
            )
        })
    }
}

#[cfg(test)]
mod test {
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn legal_terms_requires_type(pool: sqlx::PgPool) {
        let server = crate::test_server::TestServer::start(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let query = serde_json::json!({ "query": "{ legalTerms { id text } }" });

        let response: serde_json::Value = server.graphql(&query, None).await;
        insta::assert_json_snapshot!(response["errors"][0]["message"], @r#""Field \"legalTerms\" argument \"type\" of type \"QueryRoot\" is required but not provided""#);
        assert!(response["data"].is_null());
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn legal_terms_unauthenticated(pool: sqlx::PgPool) {
        let server = crate::test_server::TestServer::start(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let msa = serde_json::json!({ "query": "{ legalTerms(type: MSA) { id text } }" });

        let seeded: serde_json::Value = server.graphql(&msa, None).await;
        assert!(seeded.get("errors").is_none(), "{seeded}");
        let terms = &seeded["data"]["legalTerms"];
        let expected_id: models::Id = sqlx::query_scalar(
            "SELECT id FROM internal.legal_terms WHERE type = 'msa' AND version = 4",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(terms["id"], expected_id.to_string());
        let text = terms["text"].as_str().unwrap();
        assert!(text.starts_with("# MASTER SERVICES AGREEMENT\n"));
    }

    async fn seed_accepted_terms(pool: &sqlx::PgPool) -> uuid::Uuid {
        let user = uuid::Uuid::from_bytes([0x11; 16]);
        sqlx::query("INSERT INTO auth.users (id, email) VALUES ($1, 'alice@example.test')")
            .bind(user)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenants (tenant) VALUES ('acmeCo/')")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO user_grants (user_id, object_role, capability) VALUES ($1, 'acmeCo/', 'admin')")
            .bind(user).execute(pool).await.unwrap();
        sqlx::query(
            "INSERT INTO internal.tenant_consent (user_id, user_email, terms_id, tenant_name, tenant_id)
             SELECT $1, 'alice@example.test', l.id, t.tenant, t.id
             FROM internal.legal_terms l CROSS JOIN tenants t WHERE t.tenant = 'acmeCo/'",
        ).bind(user).execute(pool).await.unwrap();
        user
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn accept_legal_terms(pool: sqlx::PgPool) {
        let user = seed_accepted_terms(&pool).await;
        let other_user = uuid::Uuid::from_bytes([0x22; 16]);
        sqlx::query("INSERT INTO auth.users (id, email) VALUES ($1, 'bob@example.test')")
            .bind(other_user)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO user_grants (user_id, object_role, capability) VALUES ($1, 'acmeCo/', 'admin')")
            .bind(other_user).execute(&pool).await.unwrap();
        let server = crate::test_server::TestServer::start(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let token = server.make_access_token(user, Some("untrusted@example.test"));
        let terms_id = "00:00:00:00:00:00:00:0f";
        sqlx::query("INSERT INTO internal.legal_terms (id, type, version, text) VALUES ($1::flowid, 'msa', 99, 'Test agreement')")
            .bind(terms_id).execute(&pool).await.unwrap();
        let query = serde_json::json!({
            "query": "mutation($id: Id!) { legalAccept(tenant: \"acmeCo/\", termsId: $id) { type version text } }",
            "variables": {"id": terms_id}
        });
        let response: serde_json::Value = server.graphql(&query, Some(&token)).await;
        insta::assert_json_snapshot!(response, @r#"
        {
          "data": {
            "legalAccept": {
              "text": "Test agreement",
              "type": "MSA",
              "version": 99
            }
          }
        }
        "#);
        let audit: serde_json::Value = sqlx::query_scalar(
            "SELECT json_build_object('userMatches', c.user_id = $1, 'email', c.user_email,
                                      'tenant', c.tenant_name, 'currentTenant', c.tenant_id = t.id)
             FROM internal.tenant_consent c JOIN tenants t ON t.tenant = c.tenant_name
             WHERE c.terms_id = $2::flowid",
        )
        .bind(user)
        .bind(terms_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        insta::assert_json_snapshot!(audit, @r#"
        {
          "currentTenant": true,
          "email": "alice@example.test",
          "tenant": "acmeCo/",
          "userMatches": true
        }
        "#);
        let original: serde_json::Value = sqlx::query_scalar(
            "SELECT to_jsonb(c) FROM internal.tenant_consent c WHERE terms_id = $1::flowid",
        )
        .bind(terms_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        let other_token = server.make_access_token(other_user, Some("bob@example.test"));
        let (retry, other_acceptance) = tokio::join!(
            server.graphql::<_, serde_json::Value>(&query, Some(&token)),
            server.graphql::<_, serde_json::Value>(&query, Some(&other_token)),
        );
        assert_eq!(retry, response);
        assert_eq!(other_acceptance, response);
        let rows: Vec<serde_json::Value> = sqlx::query_scalar(
            "SELECT to_jsonb(c) FROM internal.tenant_consent c WHERE terms_id = $1::flowid",
        )
        .bind(terms_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(rows, vec![original]);
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn accept_legal_terms_requires_direct_grant(pool: sqlx::PgPool) {
        let user = seed_accepted_terms(&pool).await;
        sqlx::query("INSERT INTO tenants (tenant) VALUES ('otherCo/')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO role_grants (subject_role, object_role, capability) VALUES ('acmeCo/', 'otherCo/', 'admin')")
            .execute(&pool).await.unwrap();
        // Another user's direct grant must not authorize the requesting user.
        sqlx::query("INSERT INTO auth.users (id, email) VALUES ($1, 'bob@example.test')")
            .bind(uuid::Uuid::from_bytes([0x22; 16]))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO user_grants (user_id, object_role, capability) VALUES ($1, 'otherCo/', 'admin')")
            .bind(uuid::Uuid::from_bytes([0x22; 16]))
            .execute(&pool).await.unwrap();
        let snapshot = crate::test_server::snapshot(pool.clone(), false).await;
        let refresh = snapshot.token();
        let grants = refresh.result().unwrap();
        assert!(tables::UserGrant::is_authorized(
            &grants.role_grants,
            &grants.user_grants,
            &models::authz::Subject::unrestricted(user),
            "otherCo/",
            models::authz::Capability::ManageLegal,
        ));
        let server = crate::test_server::TestServer::start(pool.clone(), snapshot).await;
        let token = server.make_access_token(user, None);
        let terms_id: models::Id = sqlx::query_scalar(
            "SELECT id FROM internal.legal_terms WHERE type = 'msa' AND version = 4",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let response: serde_json::Value = server.graphql(&serde_json::json!({
            "query": "mutation($id: Id!) { legalAccept(tenant: \"otherCo/\", termsId: $id) { id } }",
            "variables": {"id": terms_id.to_string()},
        }), Some(&token)).await;
        insta::assert_json_snapshot!(response["errors"][0]["message"], @r#""PermissionDenied: Accepting legal terms requires a direct user grant on the tenant. Requires capabilities: admin""#);
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM internal.tenant_consent WHERE tenant_name = 'otherCo/'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 0);
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn accept_legal_terms_rejections(pool: sqlx::PgPool) {
        let user = seed_accepted_terms(&pool).await;
        let server = crate::test_server::TestServer::start(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let token = server.make_access_token(user, None);
        let terms_id: models::Id = sqlx::query_scalar(
            "SELECT id FROM internal.legal_terms WHERE type = 'msa' AND version = 4",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let query = serde_json::json!({
            "query": "mutation($tenant: String!, $id: Id!) { legalAccept(tenant: $tenant, termsId: $id) { id } }",
            "variables": {"tenant": "acmeCo/", "id": terms_id.to_string()}
        });
        let before: i64 = sqlx::query_scalar("SELECT count(*) FROM internal.tenant_consent")
            .fetch_one(&pool)
            .await
            .unwrap();
        let admin_mask =
            server.make_restricted_access_token(user, None, Some(vec!["admin".to_string()]), None);
        let prefix_mask = server.make_restricted_access_token(
            user,
            None,
            None,
            Some(vec!["acmeCo/".to_string()]),
        );
        for restricted in [admin_mask, prefix_mask] {
            let response: serde_json::Value = server.graphql(&query, Some(&restricted)).await;
            insta::allow_duplicates! {
                insta::assert_json_snapshot!(response["errors"][0]["message"], @r#""Restricted tokens cannot accept legal terms""#);
            }
        }
        let old_terms_id: models::Id = sqlx::query_scalar(
            "SELECT id FROM internal.legal_terms WHERE type = 'msa' AND version = 3",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let mut outdated = query.clone();
        outdated["variables"]["id"] = serde_json::json!(old_terms_id.to_string());
        let outdated: serde_json::Value = server.graphql(&outdated, Some(&token)).await;
        insta::assert_json_snapshot!(outdated["errors"][0]["message"], @r#""User or tenant not found, or legal terms are not the latest version""#);
        let mut invalid = query.clone();
        invalid["variables"]["tenant"] = serde_json::json!("acmeCo/nested/");
        let invalid: serde_json::Value = server.graphql(&invalid, Some(&token)).await;
        assert!(invalid.get("errors").is_some(), "{invalid}");
        sqlx::query("INSERT INTO internal.service_accounts (user_id, catalog_name, created_by) VALUES ($1, 'acmeCo/bot', $1)")
            .bind(user).execute(&pool).await.unwrap();
        let bot: serde_json::Value = server.graphql(&query, Some(&token)).await;
        insta::assert_json_snapshot!(bot["errors"][0]["message"], @r#""Service accounts cannot accept legal terms""#);
        let after: i64 = sqlx::query_scalar("SELECT count(*) FROM internal.tenant_consent")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(before, after);
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn legal_check(pool: sqlx::PgPool) {
        let user = seed_accepted_terms(&pool).await;
        sqlx::query("UPDATE user_grants SET capability = 'read' WHERE user_id = $1")
            .bind(user)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenants (tenant) VALUES ('otherCo/'), ('nestedCo/')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO user_grants (user_id, object_role, capability) VALUES ($1, 'otherCo/', 'read'), ($1, 'nestedCo/data/', 'read')")
            .bind(user).execute(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO internal.tenant_consent (user_id, user_email, terms_id, tenant_name, tenant_id)
             SELECT $1, 'alice@example.test', id, 'otherCo/', '00:00:00:00:00:00:00:01'::flowid
             FROM internal.legal_terms WHERE type = 'msa' AND version = 4",
        ).bind(user).execute(&pool).await.unwrap();
        let server = crate::test_server::TestServer::start(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let token = server.make_access_token(user, None);
        let query = serde_json::json!({"query": r#"{
            exact: legalCheck(tenant: "acmeCo/", type: MSA, version: { eq: 3 })
            minimum: legalCheck(tenant: "acmeCo/", type: MSA, version: { gte: 4 })
            missing: legalCheck(tenant: "acmeCo/", type: MSA, version: { eq: 5 })
            newer: legalCheck(tenant: "acmeCo/", type: MSA, version: { gte: 5 })
            recreated: legalCheck(tenant: "otherCo/", type: MSA, version: { eq: 4 })
            nested: legalCheck(tenant: "nestedCo/", type: MSA, version: { eq: 4 })
        }"#});
        let response: serde_json::Value = server.graphql(&query, Some(&token)).await;
        insta::assert_json_snapshot!(response, @r#"
        {
          "data": {
            "exact": true,
            "minimum": true,
            "missing": false,
            "nested": false,
            "newer": false,
            "recreated": false
          }
        }
        "#);

        let query = serde_json::json!({
            "query": "query($tenant: String!, $version: LegalConsentVersion!) { legalCheck(tenant: $tenant, type: MSA, version: $version) }",
            "variables": {"tenant": "acmeCo/", "version": {"eq": 4}},
        });
        let mut errors = Vec::new();
        for version in [
            serde_json::json!({}),
            serde_json::json!({"eq": 4, "gte": 4}),
        ] {
            let mut invalid = query.clone();
            invalid["variables"]["version"] = version;
            let response: serde_json::Value = server.graphql(&invalid, Some(&token)).await;
            errors.push(response["errors"][0]["message"].clone());
        }
        insta::assert_json_snapshot!(errors, @r#"
        [
          "Provide exactly one of version.eq or version.gte",
          "Provide exactly one of version.eq or version.gte"
        ]
        "#);
        let mut nested = query.clone();
        nested["variables"]["tenant"] = serde_json::json!("nestedCo/");
        let mut authorized = Vec::new();
        for (capability_mask, prefix_mask) in [
            (None, Some(vec!["nestedCo/data/sub/".to_string()])),
            (None, Some(vec!["nestedCo/other/".to_string()])),
            (None, Some(vec!["otherCo/".to_string()])),
            (Some(vec!["billing".to_string()]), None),
        ] {
            let token =
                server.make_restricted_access_token(user, None, capability_mask, prefix_mask);
            let response: serde_json::Value = server.graphql(&nested, Some(&token)).await;
            authorized.push(response.get("errors").is_none());
        }
        insta::assert_json_snapshot!(authorized, @r#"
        [
          true,
          false,
          false,
          false
        ]
        "#);
        nested["variables"]["tenant"] = serde_json::json!("nestedCoOther/");
        let denied: serde_json::Value = server.graphql(&nested, Some(&token)).await;
        assert!(denied["errors"].is_array(), "{denied}");
    }
}
