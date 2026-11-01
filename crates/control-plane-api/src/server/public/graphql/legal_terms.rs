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

    /// Whether the tenant has accepted matching legal terms. Requires CatalogRead on the tenant.
    async fn legal_consent_check(
        &self,
        ctx: &async_graphql::Context<'_>,
        #[graphql(validator(min_length = 2, regex = "^[^/]+/$"))] tenant: String,
        r#type: LegalTermsType,
        version: super::filters::IntFilter,
    ) -> async_graphql::Result<bool> {
        let env = ctx.data::<crate::Envelope>()?;
        super::verify_authorization(env, &tenant, models::authz::Capability::CatalogRead).await?;
        if version.eq.is_some() == version.gte.is_some() {
            return Err(async_graphql::Error::new(
                "Provide exactly one of version.eq or version.gte",
            ));
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
    async fn legal_terms_accept(
        &self,
        ctx: &async_graphql::Context<'_>,
        #[graphql(validator(min_length = 2, regex = "^[^/]+/$"))] tenant: String,
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

        // Capture the stored identity and current tenant ID in the immutable audit record.
        let terms = sqlx::query_as::<_, LegalTerms>(
            "WITH consent AS (
                 INSERT INTO internal.tenant_consent
                     (user_id, user_email, terms_id, tenant_name, tenant_id)
                 SELECT u.id, u.email, l.id, t.tenant, t.id
                 FROM auth.users u CROSS JOIN public.tenants t CROSS JOIN internal.legal_terms l
                 WHERE u.id = $1 AND t.tenant = $2 AND l.id = $3
                   AND NOT EXISTS (
                       SELECT 1 FROM internal.legal_terms newer
                       WHERE newer.type = l.type AND newer.version > l.version
                   )
                 RETURNING terms_id
             )
             SELECT l.id, l.text, l.type, l.version FROM internal.legal_terms l
             JOIN consent c ON c.terms_id = l.id",
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

    async fn accepted_terms_server(
        pool: &sqlx::PgPool,
    ) -> (crate::test_server::TestServer, String) {
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
        let server = crate::test_server::TestServer::start(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let token = server.make_access_token(user, None);
        (server, token)
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn accept_legal_terms(pool: sqlx::PgPool) {
        let (server, _) = accepted_terms_server(&pool).await;
        let user = uuid::Uuid::from_bytes([0x11; 16]);
        let token = server.make_access_token(user, Some("untrusted@example.test"));
        let terms_id = "00:00:00:00:00:00:00:0f";
        sqlx::query("INSERT INTO internal.legal_terms (id, type, version, text) VALUES ($1::flowid, 'msa', 99, 'Test agreement')")
            .bind(terms_id).execute(&pool).await.unwrap();
        let query = serde_json::json!({
            "query": "mutation($id: Id!) { legalTermsAccept(tenant: \"acmeCo/\", termsId: $id) { type version text } }",
            "variables": {"id": terms_id}
        });
        let response: serde_json::Value = server.graphql(&query, Some(&token)).await;
        insta::assert_json_snapshot!(response, @r#"
        {
          "data": {
            "legalTermsAccept": {
              "type": "MSA",
              "version": 99,
              "text": "Test agreement"
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
        let query = serde_json::json!({"query": "{ legalConsentCheck(tenant: \"acmeCo/\", type: MSA, version: { eq: 99 }) }"});
        let accepted: serde_json::Value = server.graphql(&query, Some(&token)).await;
        insta::assert_json_snapshot!(accepted, @r#"
        {
          "data": {
            "legalConsentCheck": true
          }
        }
        "#);
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn accept_legal_terms_requires_direct_grant(pool: sqlx::PgPool) {
        let (_server, _) = accepted_terms_server(&pool).await;
        let user = uuid::Uuid::from_bytes([0x11; 16]);
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
            "query": "mutation($id: Id!) { legalTermsAccept(tenant: \"otherCo/\", termsId: $id) { id } }",
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
        let (server, token) = accepted_terms_server(&pool).await;
        let user = uuid::Uuid::from_bytes([0x11; 16]);
        let terms_id: models::Id = sqlx::query_scalar(
            "SELECT id FROM internal.legal_terms WHERE type = 'msa' AND version = 4",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let query = serde_json::json!({
            "query": "mutation($tenant: String!, $id: Id!) { legalTermsAccept(tenant: $tenant, termsId: $id) { id } }",
            "variables": {"tenant": "acmeCo/", "id": terms_id.to_string()}
        });
        let before: i64 = sqlx::query_scalar("SELECT count(*) FROM internal.tenant_consent")
            .fetch_one(&pool)
            .await
            .unwrap();
        let anonymous: serde_json::Value = server.graphql(&query, None).await;
        assert!(anonymous.get("errors").is_some(), "{anonymous}");
        let admin_mask =
            server.make_restricted_access_token(user, None, Some(vec!["admin".to_string()]), None);
        let tenant_scope = server.make_restricted_access_token(
            user,
            None,
            None,
            Some(vec!["acmeCo/".to_string()]),
        );
        for restricted in [admin_mask, tenant_scope] {
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
        let mut missing = query.clone();
        missing["variables"]["id"] = serde_json::json!("00:00:00:00:00:00:00:0f");
        let missing: serde_json::Value = server.graphql(&missing, Some(&token)).await;
        insta::assert_json_snapshot!(missing["errors"][0]["message"], @r#""User or tenant not found, or legal terms are not the latest version""#);
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
    async fn legal_consent_check(pool: sqlx::PgPool) {
        let (_server, _) = accepted_terms_server(&pool).await;
        let user = uuid::Uuid::from_bytes([0x11; 16]);
        sqlx::query("UPDATE user_grants SET capability = 'read' WHERE user_id = $1")
            .bind(user)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenants (tenant) VALUES ('otherCo/'), ('nestedCo/')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO user_grants (user_id, object_role, capability) VALUES ($1, 'otherCo/', 'read'), ($1, 'nestedCo/data/', 'admin')")
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
            exact: legalConsentCheck(tenant: "acmeCo/", type: MSA, version: { eq: 3 })
            minimum: legalConsentCheck(tenant: "acmeCo/", type: MSA, version: { gte: 4 })
            missing: legalConsentCheck(tenant: "acmeCo/", type: MSA, version: { eq: 5 })
            newer: legalConsentCheck(tenant: "acmeCo/", type: MSA, version: { gte: 5 })
            recreated: legalConsentCheck(tenant: "otherCo/", type: MSA, version: { eq: 4 })
        }"#});
        let response: serde_json::Value = server.graphql(&query, Some(&token)).await;
        insta::assert_json_snapshot!(response, @r#"
        {
          "data": {
            "exact": true,
            "minimum": true,
            "missing": false,
            "newer": false,
            "recreated": false
          }
        }
        "#);

        let query = serde_json::json!({
            "query": "query($tenant: String!, $version: IntFilter!) { legalConsentCheck(tenant: $tenant, type: MSA, version: $version) }",
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
        let denied: serde_json::Value = server.graphql(&nested, Some(&token)).await;
        assert!(denied["errors"].is_array(), "{denied}");
        let anonymous: serde_json::Value = server.graphql(&query, None).await;
        assert!(anonymous["errors"].is_array(), "{anonymous}");
    }
}
