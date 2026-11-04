use super::filters;
use async_graphql::{Context, types::connection};

/// A prefix to which the user is authorized.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct PrefixRef {
    /// The prefix to which the user is authorized.
    pub prefix: models::Prefix,
    /// The literal legacy `capability` column value of the grant(s) that
    /// emitted this prefix (max'd if multiple grants land at the same
    /// prefix). Reports `none` for prefixes whose authorization comes
    /// entirely from the `bundles` column rather than the legacy column.
    ///
    /// Exists solely so the dashboard's read/write/admin prefix-bucket
    /// store keeps working until it migrates to consuming `capabilities`
    /// directly. Once that migration lands, this field and its derivation
    /// can be deleted.
    pub user_capability: models::Capability,
    /// Capability bundles fully covered by the user's effective permissions at this prefix.
    pub capabilities: Vec<models::authz::CapabilityBundle>,
}

#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct PrefixesBy {
    /// Filter returned prefixes by user capability.
    pub min_capability: models::Capability,
}

/// Optional filter for the `prefixes` query. A filter only narrows the
/// prefixes the caller can reach.
#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct PrefixesFilter {
    /// Filter on the returned prefix.
    pub prefix: Option<filters::PrefixFilter>,
}

pub type PaginatedPrefixes = connection::Connection<
    String,
    PrefixRef,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

#[derive(Debug, Default)]
pub struct PrefixesQuery;

#[async_graphql::Object]
impl PrefixesQuery {
    pub async fn prefixes(
        &self,
        ctx: &Context<'_>,
        by: PrefixesBy,
        filter: Option<PrefixesFilter>,
        after: Option<String>,
        first: Option<i32>,
    ) -> async_graphql::Result<PaginatedPrefixes> {
        let env = ctx.data::<crate::Envelope>()?;

        let (starts_with, r#in) = match filter.and_then(|f| f.prefix) {
            Some(prefix) => prefix.into_parts("filter.prefix")?,
            None => (None, None),
        };

        connection::query(after, None, first, None, |after, _, first, _| async move {
            let snapshot = env.snapshot();
            let subject = env.claims()?.subject();

            let min_bits: models::authz::CapabilitySet = by.min_capability.into();
            let under = starts_with.as_deref().unwrap_or_default();

            let reachable = tables::UserGrant::reachable_prefixes(
                &snapshot.role_grants,
                &snapshot.user_grants,
                &subject,
            );
            // Cursor pagination: BTreeMap::range jumps directly to the
            // first key strictly greater than the previous page's last
            // prefix (or to `startsWith`, if later), rather than iterating
            // from the start and filtering past it. Keys under a prefix
            // are contiguous, so iteration ends with the subtree.
            let start = match after.as_deref() {
                Some(after) if after >= under => std::ops::Bound::Excluded(after),
                _ => std::ops::Bound::Included(under),
            };
            let all_roles: Vec<PrefixRef> = reachable
                .range::<str, _>((start, std::ops::Bound::Unbounded))
                .take_while(|(prefix, _)| prefix.starts_with(under))
                .filter(|(prefix, _)| {
                    r#in.as_ref()
                        .is_none_or(|exact| exact.iter().any(|e| e == *prefix))
                })
                .filter(|(_, (bits, _))| bits.is_superset(min_bits))
                .map(|(prefix, (bits, legacy))| PrefixRef {
                    prefix: models::Prefix::new(*prefix),
                    user_capability: *legacy,
                    capabilities:
                        <models::authz::CapabilityBundle as strum::VariantArray>::VARIANTS
                            .iter()
                            .copied()
                            .filter(|bundle| bits.is_superset(bundle.capabilities()))
                            .collect(),
                })
                .collect();

            let take = first.unwrap_or(all_roles.len());
            let has_next = first.is_some_and(|limit| all_roles.len() > limit);

            let edges = all_roles
                .into_iter()
                .take(take)
                .map(|prefix| {
                    let cursor = prefix.prefix.to_string();
                    connection::Edge::new(cursor, prefix)
                })
                .collect();

            let mut conn = connection::Connection::new(false, has_next);
            conn.edges = edges;
            async_graphql::Result::<PaginatedPrefixes>::Ok(conn)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::test_server;

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_graphql_prefixes(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let server = test_server::TestServer::start(
            pool.clone(),
            // Use an immediate Snapshot. Prefixes doesn't use Envelope::authorization_outcome
            // and won't trigger an authorization retry.
            test_server::snapshot(pool, false).await,
        )
        .await;

        let token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        prefixes(by: { minCapability: read }) {
                            edges {
                                node {
                                    prefix
                                    userCapability
                                    capabilities
                                }
                            }
                        }
                    }
                "#
                }),
                Some(&token),
            )
            .await;

        insta::assert_json_snapshot!(response,
          @r#"
        {
          "data": {
            "prefixes": {
              "edges": [
                {
                  "node": {
                    "capabilities": [
                      "viewer",
                      "writer",
                      "editor",
                      "admin",
                      "billing",
                      "team_admin",
                      "manage_service_accounts",
                      "manage_data_plane",
                      "create_sandbox",
                      "delegate"
                    ],
                    "prefix": "aliceCo/",
                    "userCapability": "admin"
                  }
                },
                {
                  "node": {
                    "capabilities": [
                      "viewer",
                      "writer"
                    ],
                    "prefix": "aliceCo/data/",
                    "userCapability": "write"
                  }
                },
                {
                  "node": {
                    "capabilities": [
                      "viewer"
                    ],
                    "prefix": "ops/dp/public/",
                    "userCapability": "read"
                  }
                }
              ]
            }
          }
        }
        "#);

        // Editor covers only part of Viewer, so the restricted result must not
        // advertise Viewer or the unrestricted Admin grant.
        let restricted_token = server.make_restricted_access_token(
            uuid::Uuid::from_bytes([0x11; 16]),
            None,
            Some(vec!["editor".to_string()]),
            None,
        );
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        prefixes(by: { minCapability: none }, filter: { prefix: { in: ["aliceCo/"] } }) {
                            edges { node { prefix capabilities userCapability } }
                        }
                    }
                    "#
                }),
                Some(&restricted_token),
            )
            .await;

        insta::assert_json_snapshot!(response, @r#"
        {
          "data": {
            "prefixes": {
              "edges": [
                {
                  "node": {
                    "capabilities": [
                      "editor",
                      "delegate"
                    ],
                    "prefix": "aliceCo/",
                    "userCapability": "admin"
                  }
                }
              ]
            }
          }
        }
        "#);

        // `exact` returns only the listed prefixes the user can reach.
        // `paged` resumes after a cursor and stops at the end of the
        // `aliceCo/` subtree, before `ops/dp/public/`. `subtree` skips the
        // `aliceCo/` rows that sort before its subtree and includes the
        // prefix itself.
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        exact: prefixes(by: { minCapability: read }, filter: { prefix: { in: ["aliceCo/data/", "bobCo/"] } }) {
                            edges { node { prefix } }
                        }
                        paged: prefixes(by: { minCapability: read }, filter: { prefix: { startsWith: "aliceCo/" } }, after: "aliceCo/") {
                            edges { node { prefix } }
                        }
                        subtree: prefixes(by: { minCapability: read }, filter: { prefix: { startsWith: "ops/dp/public/" } }) {
                            edges { node { prefix } }
                        }
                    }
                "#
                }),
                Some(&token),
            )
            .await;

        insta::assert_json_snapshot!(response, @r#"
        {
          "data": {
            "exact": {
              "edges": [
                {
                  "node": {
                    "prefix": "aliceCo/data/"
                  }
                }
              ]
            },
            "paged": {
              "edges": [
                {
                  "node": {
                    "prefix": "aliceCo/data/"
                  }
                }
              ]
            },
            "subtree": {
              "edges": [
                {
                  "node": {
                    "prefix": "ops/dp/public/"
                  }
                }
              ]
            }
          }
        }
        "#);

        // Again, but omit the authorization token with this request.
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        prefixes(by: { minCapability: read }) {
                            edges {
                                node {
                                    prefix
                                }
                            }
                        }
                    }
                "#
                }),
                None,
            )
            .await;

        insta::assert_json_snapshot!(response,
          @r#"
        {
          "data": null,
          "errors": [
            {
              "locations": [
                {
                  "column": 25,
                  "line": 3
                }
              ],
              "message": "code: 'The request does not have valid authentication credentials', message: \"This is an authenticated API but the request is missing a required Authorization: Bearer token\"",
              "path": [
                "prefixes"
              ]
            }
          ]
        }
        "#);
    }
}
