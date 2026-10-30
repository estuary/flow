use async_graphql::types::connection;

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
    SandboxConsent,
}

#[derive(async_graphql::SimpleObject, sqlx::FromRow)]
pub struct LegalTerms {
    pub id: models::Id,
    pub text: String,
    pub r#type: LegalTermsType,
    pub version: i32,
}

type LegalTermsCursor = connection::OpaqueCursor<(LegalTermsType, i32)>;

type PaginatedLegalTerms = connection::Connection<
    LegalTermsCursor,
    LegalTerms,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

const DEFAULT_PAGE_SIZE: usize = 20;

#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct LegalTermsTypeFilter {
    pub eq: Option<LegalTermsType>,
}

#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct LegalTermsFilter {
    /// Only terms accepted by this tenant. Requires CatalogRead on the tenant.
    pub tenant: Option<super::filters::StringFilter>,
    pub r#type: Option<LegalTermsTypeFilter>,
    pub version: Option<super::filters::IntFilter>,
}

#[async_graphql::Object]
impl LegalTermsQuery {
    /// Returns the highest version of the given terms type accepted by the tenant, or null.
    async fn tenant_legal_terms(
        &self,
        ctx: &async_graphql::Context<'_>,
        r#type: LegalTermsType,
        tenant: String,
    ) -> async_graphql::Result<Option<LegalTerms>> {
        let env = ctx.data::<crate::Envelope>()?;
        let tenant = super::tenant::validate_tenant_name(&tenant)?;
        super::verify_authorization(env, tenant.as_str(), models::authz::Capability::CatalogRead)
            .await?;

        // Audit records survive tenant deletion; only the current tenant ID grants consent.
        let row = sqlx::query_as::<_, LegalTerms>(
            "SELECT l.id, l.text, l.type, l.version FROM internal.legal_terms l
             WHERE l.type = $1 AND EXISTS (
                 SELECT 1 FROM internal.tenant_consent c
                 JOIN public.tenants t ON t.id = c.tenant_id
                 WHERE t.tenant = $2 AND c.terms_id = l.id
             )
             ORDER BY l.version DESC LIMIT 1",
        )
        .bind(r#type)
        .bind(tenant.as_str())
        .fetch_optional(&env.pg_pool)
        .await?;

        Ok(row)
    }

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

    /// Lists legal terms accepted by tenants the caller can read, with optional filters and pagination.
    async fn legal_terms_accepted(
        &self,
        ctx: &async_graphql::Context<'_>,
        filter: Option<LegalTermsFilter>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> async_graphql::Result<PaginatedLegalTerms> {
        let env = ctx.data::<crate::Envelope>()?;
        let LegalTermsFilter {
            tenant,
            r#type,
            version,
        } = filter.unwrap_or_default();
        let version = version.unwrap_or_default();
        let tenant = tenant.and_then(|f| f.eq);
        let tenant = tenant
            .as_deref()
            .map(super::tenant::validate_tenant_name)
            .transpose()?;
        let snapshot = env.snapshot();
        let prefixes = super::authorized_prefixes::authorized_prefixes(
            &snapshot.role_grants,
            &snapshot.user_grants,
            &env.claims()?.subject(),
            models::authz::Capability::CatalogRead,
            tenant.as_ref().map(|tenant| tenant.as_str()),
        );

        connection::query_with::<LegalTermsCursor, _, _, _, async_graphql::Error>(
            after, before, first, last,
            |after, before, first, last| async move {
                let limit = first.or(last).unwrap_or(DEFAULT_PAGE_SIZE);
                if limit == 0 || prefixes.is_empty() {
                    return Ok(PaginatedLegalTerms::new(false, false));
                }
                let reverse = last.is_some();
                let after = after.map(|cursor| cursor.0);
                let before = before.map(|cursor| cursor.0);
                if after.iter().chain(before.iter()).any(|(_, version)| *version <= 0) {
                    return Err(async_graphql::Error::new("Invalid legal terms cursor version"));
                }
                // Audit records survive tenant deletion; consent must match the current tenant ID.
                let mut rows = sqlx::query_as::<_, LegalTerms>(
                    "SELECT l.id, l.text, l.type, l.version FROM internal.legal_terms l
                     WHERE EXISTS (
                         SELECT 1 FROM internal.tenant_consent c
                         JOIN public.tenants t ON t.id = c.tenant_id
                         WHERE c.terms_id = l.id
                           AND t.tenant::text ^@ ANY($14::text[])
                           AND ($1::text IS NULL OR t.tenant = $1)
                     )
                     AND ($2::internal.legal_terms_type IS NULL OR l.type = $2)
                     AND ($3::integer IS NULL OR l.version = $3)
                     AND ($4::integer IS NULL OR l.version > $4)
                     AND ($5::integer IS NULL OR l.version < $5)
                     AND ($6::integer IS NULL OR l.version >= $6)
                     AND ($7::integer IS NULL OR l.version <= $7)
                     AND ($8::internal.legal_terms_type IS NULL OR (l.type, -l.version) > ($8, -$9::integer))
                     AND ($10::internal.legal_terms_type IS NULL OR (l.type, -l.version) < ($10, -$11::integer))
                     ORDER BY
                         CASE WHEN NOT $12 THEN l.type END ASC,
                         CASE WHEN NOT $12 THEN l.version END DESC,
                         CASE WHEN $12 THEN l.type END DESC,
                         CASE WHEN $12 THEN l.version END ASC
                     LIMIT $13",
                )
                .bind(tenant.as_ref().map(|tenant| tenant.as_str()))
                .bind(r#type.and_then(|f| f.eq))
                .bind(version.eq)
                .bind(version.gt)
                .bind(version.lt)
                .bind(version.gte)
                .bind(version.lte)
                .bind(after.map(|cursor| cursor.0))
                .bind(after.map(|cursor| cursor.1))
                .bind(before.map(|cursor| cursor.0))
                .bind(before.map(|cursor| cursor.1))
                .bind(reverse)
                .bind(limit as i64 + 1)
                .bind(&prefixes)
                .fetch_all(&env.pg_pool)
                .await?;

                let has_more = rows.len() > limit;
                rows.truncate(limit);
                let (has_previous, has_next) = if reverse {
                    rows.reverse();
                    (has_more, before.is_some())
                } else {
                    (after.is_some(), has_more)
                };
                let mut connection = PaginatedLegalTerms::new(has_previous, has_next);
                connection.edges.extend(rows.into_iter().map(|row| {
                    connection::Edge::new(connection::OpaqueCursor((row.r#type, row.version)), row)
                }));
                Ok(connection)
            },
        ).await
    }
}

#[derive(Debug, Default)]
pub struct LegalTermsMutation;

#[async_graphql::Object]
impl LegalTermsMutation {
    /// Accept the latest legal terms of their type. Requires ManageLegal and an unrestricted user token.
    async fn legal_terms_accept(
        &self,
        ctx: &async_graphql::Context<'_>,
        tenant: String,
        terms_id: models::Id,
    ) -> async_graphql::Result<LegalTerms> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;
        if claims.capability_mask.is_some() || claims.prefix_scope.is_some() {
            return Err(async_graphql::Error::new(
                "Restricted tokens cannot accept legal terms",
            ));
        }
        let tenant = super::tenant::validate_tenant_name(&tenant)?;
        super::verify_authorization(env, tenant.as_str(), models::authz::Capability::ManageLegal)
            .await?;
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
        sqlx::query("INSERT INTO internal.legal_terms (id, type, version, text) VALUES ($1::flowid, 'sandbox_consent', 99, 'Test agreement'), (DEFAULT, 'msa', 100, 'Test MSA agreement')")
            .bind(terms_id).execute(&pool).await.unwrap();
        let query = serde_json::json!({
            "query": "mutation($id: Id!) { legalTermsAccept(tenant: \"acmeCo\", termsId: $id) { type version text } }",
            "variables": {"id": terms_id}
        });
        let response: serde_json::Value = server.graphql(&query, Some(&token)).await;
        insta::assert_json_snapshot!(response, @r#"
        {
          "data": {
            "legalTermsAccept": {
              "type": "SANDBOX_CONSENT",
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
        let query = serde_json::json!({"query": "{ legalTermsAccepted(filter: { type: { eq: SANDBOX_CONSENT } }, first: 1) { edges { node { version text } } } }"});
        let accepted: serde_json::Value = server.graphql(&query, Some(&token)).await;
        insta::assert_json_snapshot!(accepted, @r#"
        {
          "data": {
            "legalTermsAccepted": {
              "edges": [
                {
                  "node": {
                    "version": 99,
                    "text": "Test agreement"
                  }
                }
              ]
            }
          }
        }
        "#);
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
            "variables": {"tenant": "acmeCo", "id": terms_id.to_string()}
        });
        let before: i64 = sqlx::query_scalar("SELECT count(*) FROM internal.tenant_consent")
            .fetch_one(&pool)
            .await
            .unwrap();
        let reader =
            server.make_restricted_access_token(user, None, Some(vec!["viewer".to_string()]), None);
        let billing = server.make_restricted_access_token(
            user,
            None,
            Some(vec!["billing".to_string()]),
            None,
        );
        let scoped =
            server.make_restricted_access_token(user, None, None, Some("acmeCo/data/".to_string()));
        for token in [None, Some(&reader), Some(&billing), Some(&scoped)] {
            let response: serde_json::Value =
                server.graphql(&query, token.map(String::as_str)).await;
            assert!(response.get("errors").is_some(), "{response}");
        }
        let admin_mask =
            server.make_restricted_access_token(user, None, Some(vec!["admin".to_string()]), None);
        let tenant_scope =
            server.make_restricted_access_token(user, None, None, Some("acmeCo/".to_string()));
        for restricted in [admin_mask, tenant_scope] {
            let response: serde_json::Value = server.graphql(&query, Some(&restricted)).await;
            insta::assert_json_snapshot!(response["errors"][0]["message"], @r#""Restricted tokens cannot accept legal terms""#);
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
    async fn legal_terms_pagination(pool: sqlx::PgPool) {
        let (server, token) = accepted_terms_server(&pool).await;
        let mut pages = Vec::new();
        for reverse in [false, true] {
            let mut cursor = serde_json::Value::Null;
            for _ in 0..4 {
                let query = serde_json::json!({
                    "query": "query($after: String, $before: String, $first: Int, $last: Int) { legalTermsAccepted(after: $after, before: $before, first: $first, last: $last) { edges { node { type version } } pageInfo { startCursor endCursor hasPreviousPage hasNextPage } } }",
                    "variables": if reverse {
                        serde_json::json!({"before": cursor, "last": 1})
                    } else {
                        serde_json::json!({"after": cursor, "first": 1})
                    }
                });
                let response: serde_json::Value = server.graphql(&query, Some(&token)).await;
                assert!(response.get("errors").is_none(), "{response}");
                let mut page = response["data"]["legalTermsAccepted"].clone();
                cursor =
                    page["pageInfo"][if reverse { "startCursor" } else { "endCursor" }].clone();
                let info = page["pageInfo"].as_object_mut().unwrap();
                info.remove("startCursor");
                info.remove("endCursor");
                pages.push(page);
            }
        }
        insta::assert_json_snapshot!(pages, @r#"
        [
          {
            "edges": [
              {
                "node": {
                  "type": "MSA",
                  "version": 4
                }
              }
            ],
            "pageInfo": {
              "hasPreviousPage": false,
              "hasNextPage": true
            }
          },
          {
            "edges": [
              {
                "node": {
                  "type": "MSA",
                  "version": 3
                }
              }
            ],
            "pageInfo": {
              "hasPreviousPage": true,
              "hasNextPage": true
            }
          },
          {
            "edges": [
              {
                "node": {
                  "type": "SANDBOX_CONSENT",
                  "version": 1
                }
              }
            ],
            "pageInfo": {
              "hasPreviousPage": true,
              "hasNextPage": false
            }
          },
          {
            "edges": [],
            "pageInfo": {
              "hasPreviousPage": true,
              "hasNextPage": false
            }
          },
          {
            "edges": [
              {
                "node": {
                  "type": "SANDBOX_CONSENT",
                  "version": 1
                }
              }
            ],
            "pageInfo": {
              "hasPreviousPage": true,
              "hasNextPage": false
            }
          },
          {
            "edges": [
              {
                "node": {
                  "type": "MSA",
                  "version": 3
                }
              }
            ],
            "pageInfo": {
              "hasPreviousPage": true,
              "hasNextPage": true
            }
          },
          {
            "edges": [
              {
                "node": {
                  "type": "MSA",
                  "version": 4
                }
              }
            ],
            "pageInfo": {
              "hasPreviousPage": false,
              "hasNextPage": true
            }
          },
          {
            "edges": [],
            "pageInfo": {
              "hasPreviousPage": false,
              "hasNextPage": true
            }
          }
        ]
        "#);
        let query = serde_json::json!({"query": "{ filtered: legalTermsAccepted(filter: { type: { eq: MSA }, version: { gte: 3 } }, first: 1) { edges { node { version } } pageInfo { hasNextPage } } zero: legalTermsAccepted(first: 0) { edges { cursor } pageInfo { hasNextPage hasPreviousPage } } }"});
        let response: serde_json::Value = server.graphql(&query, Some(&token)).await;
        insta::assert_json_snapshot!(response, @r#"
        {
          "data": {
            "filtered": {
              "edges": [
                {
                  "node": {
                    "version": 4
                  }
                }
              ],
              "pageInfo": {
                "hasNextPage": true
              }
            },
            "zero": {
              "edges": [],
              "pageInfo": {
                "hasNextPage": false,
                "hasPreviousPage": false
              }
            }
          }
        }
        "#);
        for args in ["first: -1", "first: 1, last: 1", "after: \"invalid\""] {
            let query = serde_json::json!({"query": format!("{{ legalTermsAccepted({args}) {{ edges {{ cursor }} }} }}")});
            let response: serde_json::Value = server.graphql(&query, Some(&token)).await;
            assert!(response.get("errors").is_some(), "{response}");
        }
    }
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn legal_terms_filters(pool: sqlx::PgPool) {
        let (server, token) = accepted_terms_server(&pool).await;
        let query = serde_json::json!({ "query": "{ all: legalTermsAccepted { edges { node { type version } } } msa: legalTermsAccepted(filter: { type: { eq: MSA } }) { edges { node { type version } } } version: legalTermsAccepted(filter: { version: { eq: 3 } }) { edges { node { type version } } } exact: legalTermsAccepted(filter: { type: { eq: MSA }, version: { eq: 4 } }) { edges { node { type version } } } missing: legalTermsAccepted(filter: { version: { eq: 999 } }) { edges { node { type version } } } }" });
        let response: serde_json::Value = server.graphql(&query, Some(&token)).await;
        insta::assert_json_snapshot!(response, @r#"
        {
          "data": {
            "all": {
              "edges": [
                {
                  "node": {
                    "type": "MSA",
                    "version": 4
                  }
                },
                {
                  "node": {
                    "type": "MSA",
                    "version": 3
                  }
                },
                {
                  "node": {
                    "type": "SANDBOX_CONSENT",
                    "version": 1
                  }
                }
              ]
            },
            "msa": {
              "edges": [
                {
                  "node": {
                    "type": "MSA",
                    "version": 4
                  }
                },
                {
                  "node": {
                    "type": "MSA",
                    "version": 3
                  }
                }
              ]
            },
            "version": {
              "edges": [
                {
                  "node": {
                    "type": "MSA",
                    "version": 3
                  }
                }
              ]
            },
            "exact": {
              "edges": [
                {
                  "node": {
                    "type": "MSA",
                    "version": 4
                  }
                }
              ]
            },
            "missing": {
              "edges": []
            }
          }
        }
        "#);
    }
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn legal_terms_version_operators(pool: sqlx::PgPool) {
        let (server, token) = accepted_terms_server(&pool).await;
        let query = serde_json::json!({ "query": r#"{
  gt: legalTermsAccepted(filter: { version: { gt: 3 } }) { edges { node { type version } } }
  lt: legalTermsAccepted(filter: { version: { lt: 3 } }) { edges { node { type version } } }
  gte: legalTermsAccepted(filter: { version: { gte: 3 } }) { edges { node { type version } } }
  lte: legalTermsAccepted(filter: { version: { lte: 3 } }) { edges { node { type version } } }
  inclusive: legalTermsAccepted(filter: { version: { gte: 3, lte: 3 } }) { edges { node { type version } } }
  range: legalTermsAccepted(filter: { version: { gt: 1, lt: 4 } }) { edges { node { type version } } }
  combined: legalTermsAccepted(filter: { version: { eq: 3, gt: 3 } }) { edges { node { type version } } }
  empty: legalTermsAccepted(filter: { tenant: {}, type: {}, version: {} }) { edges { node { type version } } }
}"# });
        let response: serde_json::Value = server.graphql(&query, Some(&token)).await;
        insta::assert_json_snapshot!(response, @r#"
        {
          "data": {
            "gt": {
              "edges": [
                {
                  "node": {
                    "type": "MSA",
                    "version": 4
                  }
                }
              ]
            },
            "lt": {
              "edges": [
                {
                  "node": {
                    "type": "SANDBOX_CONSENT",
                    "version": 1
                  }
                }
              ]
            },
            "gte": {
              "edges": [
                {
                  "node": {
                    "type": "MSA",
                    "version": 4
                  }
                },
                {
                  "node": {
                    "type": "MSA",
                    "version": 3
                  }
                }
              ]
            },
            "lte": {
              "edges": [
                {
                  "node": {
                    "type": "MSA",
                    "version": 3
                  }
                },
                {
                  "node": {
                    "type": "SANDBOX_CONSENT",
                    "version": 1
                  }
                }
              ]
            },
            "inclusive": {
              "edges": [
                {
                  "node": {
                    "type": "MSA",
                    "version": 3
                  }
                }
              ]
            },
            "range": {
              "edges": [
                {
                  "node": {
                    "type": "MSA",
                    "version": 3
                  }
                }
              ]
            },
            "combined": {
              "edges": []
            },
            "empty": {
              "edges": [
                {
                  "node": {
                    "type": "MSA",
                    "version": 4
                  }
                },
                {
                  "node": {
                    "type": "MSA",
                    "version": 3
                  }
                },
                {
                  "node": {
                    "type": "SANDBOX_CONSENT",
                    "version": 1
                  }
                }
              ]
            }
          }
        }
        "#);
    }
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn legal_terms_tenant_filter(pool: sqlx::PgPool) {
        let user = uuid::Uuid::from_bytes([0x11; 16]);
        sqlx::query("INSERT INTO auth.users (id, email) VALUES ($1, 'alice@example.test')")
            .bind(user)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenants (tenant) VALUES ('acmeCo/'), ('otherCo/'), ('hiddenCo/'), ('nestedCo/')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO user_grants (user_id, object_role, capability) VALUES ($1, 'acmeCo/', 'admin'), ($1, 'nestedCo/data/', 'read')")
            .bind(user).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO role_grants (subject_role, object_role, capability) VALUES ('acmeCo/', 'otherCo/', 'read')")
            .execute(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO internal.tenant_consent (user_id, user_email, terms_id, tenant_name, tenant_id)
             SELECT $1, 'alice@example.test', l.id, t.tenant, t.id
             FROM internal.legal_terms l CROSS JOIN tenants t
             WHERE (t.tenant = 'otherCo/' AND (l.version = 3 OR l.type = 'sandbox_consent'))
                OR (t.tenant IN ('hiddenCo/', 'nestedCo/') AND l.type = 'msa' AND l.version = 4)",
        ).bind(user).execute(&pool).await.unwrap();
        // Duplicate acceptances must not duplicate terms. Deleted tenant IDs must not match.
        sqlx::query(
            "INSERT INTO internal.tenant_consent (user_id, user_email, terms_id, tenant_name, tenant_id)
             SELECT $1, 'alice@example.test', l.id, t.tenant, t.id
             FROM internal.legal_terms l CROSS JOIN tenants t CROSS JOIN generate_series(1, 2)
             WHERE l.type = 'msa' AND l.version = 3 AND t.tenant = 'acmeCo/'",
        ).bind(user).execute(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO internal.tenant_consent (user_id, user_email, terms_id, tenant_name, tenant_id)
             SELECT $1, 'alice@example.test', id, 'acmeCo/', '00:00:00:00:00:00:00:01'::flowid
             FROM internal.legal_terms WHERE type = 'msa' AND version = 4",
        ).bind(user).execute(&pool).await.unwrap();
        let server = crate::test_server::TestServer::start(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let token = server.make_access_token(user, None);
        let query = serde_json::json!({ "query": "{ terms: legalTermsAccepted(filter: { tenant: { eq: \"acmeCo\" } }) { edges { node { type version } } } exact: legalTermsAccepted(filter: { tenant: { eq: \"acmeCo/\" }, type: { eq: MSA }, version: { eq: 3 } }) { edges { node { type version } } } missing: legalTermsAccepted(filter: { tenant: { eq: \"acmeCo\" }, version: { eq: 4 } }) { edges { node { type version } } } }" });
        let response: serde_json::Value = server.graphql(&query, Some(&token)).await;
        insta::assert_json_snapshot!(response, @r#"
        {
          "data": {
            "terms": {
              "edges": [
                {
                  "node": {
                    "type": "MSA",
                    "version": 3
                  }
                }
              ]
            },
            "exact": {
              "edges": [
                {
                  "node": {
                    "type": "MSA",
                    "version": 3
                  }
                }
              ]
            },
            "missing": {
              "edges": []
            }
          }
        }
        "#);
        let query = serde_json::json!({ "query": "{ legalTermsAccepted(filter: { tenant: { eq: \"hiddenCo\" } }) { edges { node { id } } } }" });
        let denied: serde_json::Value = server.graphql(&query, Some(&token)).await;
        insta::assert_json_snapshot!(denied, @r#"
        {
          "data": {
            "legalTermsAccepted": {
              "edges": []
            }
          }
        }
        "#);
        let query = serde_json::json!({"query": "{ legalTermsAccepted { edges { node { type version } } } }"});
        let all: serde_json::Value = server.graphql(&query, Some(&token)).await;
        insta::assert_json_snapshot!(all, @r#"
        {
          "data": {
            "legalTermsAccepted": {
              "edges": [
                {
                  "node": {
                    "type": "MSA",
                    "version": 3
                  }
                },
                {
                  "node": {
                    "type": "SANDBOX_CONSENT",
                    "version": 1
                  }
                }
              ]
            }
          }
        }
        "#);
        let scoped =
            server.make_restricted_access_token(user, None, None, Some("acmeCo/".to_string()));
        let scoped: serde_json::Value = server.graphql(&query, Some(&scoped)).await;
        insta::assert_json_snapshot!(scoped, @r#"
        {
          "data": {
            "legalTermsAccepted": {
              "edges": [
                {
                  "node": {
                    "type": "MSA",
                    "version": 3
                  }
                }
              ]
            }
          }
        }
        "#);
        let nested =
            server.make_restricted_access_token(user, None, None, Some("acmeCo/data/".to_string()));
        let masked = server.make_restricted_access_token(user, None, Some(Vec::new()), None);
        let ungranted = server.make_access_token(uuid::Uuid::from_bytes([0x22; 16]), None);
        for token in [nested, masked, ungranted] {
            let empty: serde_json::Value = server.graphql(&query, Some(&token)).await;
            insta::assert_json_snapshot!(empty, @r#"
        {
          "data": {
            "legalTermsAccepted": {
              "edges": []
            }
          }
        }
        "#);
        }
        let anonymous: serde_json::Value = server.graphql(&query, None).await;
        assert!(anonymous.get("errors").is_some(), "{anonymous}");
    }
}
