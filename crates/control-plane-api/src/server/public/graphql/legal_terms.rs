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

    /// Lists paginated legal terms, optionally restricted to terms accepted by an authorized tenant.
    async fn legal_terms(
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
        if let Some(tenant) = &tenant {
            super::verify_authorization(
                env,
                tenant.as_str(),
                models::authz::Capability::CatalogRead,
            )
            .await?;
        }

        connection::query_with::<LegalTermsCursor, _, _, _, async_graphql::Error>(
            after, before, first, last,
            |after, before, first, last| async move {
                let limit = first.or(last).unwrap_or(DEFAULT_PAGE_SIZE);
                if limit == 0 {
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
                     WHERE ($1::text IS NULL OR EXISTS (
                         SELECT 1 FROM internal.tenant_consent c
                         JOIN public.tenants t ON t.id = c.tenant_id
                         WHERE t.tenant = $1 AND c.terms_id = l.id
                     ))
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

#[cfg(test)]
mod test {
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn legal_terms_pagination(pool: sqlx::PgPool) {
        let server = crate::test_server::TestServer::start(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let mut pages = Vec::new();
        for reverse in [false, true] {
            let mut cursor = serde_json::Value::Null;
            for _ in 0..4 {
                let query = serde_json::json!({
                    "query": "query($after: String, $before: String, $first: Int, $last: Int) { legalTerms(after: $after, before: $before, first: $first, last: $last) { edges { node { type version } } pageInfo { startCursor endCursor hasPreviousPage hasNextPage } } }",
                    "variables": if reverse {
                        serde_json::json!({"before": cursor, "last": 1})
                    } else {
                        serde_json::json!({"after": cursor, "first": 1})
                    }
                });
                let response: serde_json::Value = server.graphql(&query, None).await;
                assert!(response.get("errors").is_none(), "{response}");
                let mut page = response["data"]["legalTerms"].clone();
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
        let query = serde_json::json!({"query": "{ filtered: legalTerms(filter: { type: { eq: MSA }, version: { gte: 3 } }, first: 1) { edges { node { version } } pageInfo { hasNextPage } } zero: legalTerms(first: 0) { edges { cursor } pageInfo { hasNextPage hasPreviousPage } } }"});
        let response: serde_json::Value = server.graphql(&query, None).await;
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
            let query = serde_json::json!({"query": format!("{{ legalTerms({args}) {{ edges {{ cursor }} }} }}")});
            let response: serde_json::Value = server.graphql(&query, None).await;
            assert!(response.get("errors").is_some(), "{response}");
        }
    }
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn legal_terms_filters(pool: sqlx::PgPool) {
        let server = crate::test_server::TestServer::start(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let query = serde_json::json!({ "query": "{ all: legalTerms { edges { node { type version } } } msa: legalTerms(filter: { type: { eq: MSA } }) { edges { node { type version } } } version: legalTerms(filter: { version: { eq: 3 } }) { edges { node { type version } } } exact: legalTerms(filter: { type: { eq: MSA }, version: { eq: 4 } }) { edges { node { type version } } } missing: legalTerms(filter: { version: { eq: 999 } }) { edges { node { type version } } } }" });
        let response: serde_json::Value = server.graphql(&query, None).await;
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
        let server = crate::test_server::TestServer::start(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let query = serde_json::json!({ "query": r#"{
  gt: legalTerms(filter: { version: { gt: 3 } }) { edges { node { type version } } }
  lt: legalTerms(filter: { version: { lt: 3 } }) { edges { node { type version } } }
  gte: legalTerms(filter: { version: { gte: 3 } }) { edges { node { type version } } }
  lte: legalTerms(filter: { version: { lte: 3 } }) { edges { node { type version } } }
  inclusive: legalTerms(filter: { version: { gte: 3, lte: 3 } }) { edges { node { type version } } }
  range: legalTerms(filter: { version: { gt: 1, lt: 4 } }) { edges { node { type version } } }
  combined: legalTerms(filter: { version: { eq: 3, gt: 3 } }) { edges { node { type version } } }
  empty: legalTerms(filter: { tenant: {}, type: {}, version: {} }) { edges { node { type version } } }
}"# });
        let response: serde_json::Value = server.graphql(&query, None).await;
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
        sqlx::query("INSERT INTO tenants (tenant) VALUES ('acmeCo/'), ('otherCo/')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO user_grants (user_id, object_role, capability) VALUES ($1, 'acmeCo/', 'read')")
            .bind(user).execute(&pool).await.unwrap();
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
        let query = serde_json::json!({ "query": "{ terms: legalTerms(filter: { tenant: { eq: \"acmeCo\" } }) { edges { node { type version } } } exact: legalTerms(filter: { tenant: { eq: \"acmeCo/\" }, type: { eq: MSA }, version: { eq: 3 } }) { edges { node { type version } } } missing: legalTerms(filter: { tenant: { eq: \"acmeCo\" }, version: { eq: 4 } }) { edges { node { type version } } } }" });
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
        let query = serde_json::json!({ "query": "{ legalTerms(filter: { tenant: { eq: \"otherCo\" } }) { edges { node { id } } } }" });
        let denied: serde_json::Value = server.graphql(&query, Some(&token)).await;
        assert!(denied.get("errors").is_some(), "{denied}");
        let anonymous: serde_json::Value = server.graphql(&query, None).await;
        assert!(anonymous.get("errors").is_some(), "{anonymous}");
    }
}
