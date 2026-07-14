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
    /// store keeps working until it migrates to consuming `capabilityBits`
    /// directly. Once that migration lands, this field and its derivation
    /// can be deleted.
    #[graphql(
        deprecation = "Reports only the legacy read/write/admin grant level; use `capabilityBits` instead."
    )]
    pub user_capability: models::Capability,
    /// Fine-grained capabilities the user has at this prefix.
    /// Identical to `capabilityBits`, retained under this name until
    /// clients migrate.
    #[graphql(deprecation = "Renamed to `capabilityBits`.")]
    pub capabilities: Vec<models::authz::Capability>,
    /// Capability bundles the user effectively holds at this prefix:
    /// every bundle whose full capability set is covered by
    /// `capabilityBits`, regardless of which bundles were explicitly
    /// granted.
    pub capability_bundles: Vec<models::authz::CapabilityBundle>,
    /// Fine-grained capabilities the user has at this prefix.
    pub capability_bits: Vec<models::authz::Capability>,
}

#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct PrefixesBy {
    /// Filter to prefixes where the user's capability is at least this legacy
    /// level (an ordered read/write/admin threshold).
    ///
    /// Deprecated: a "minimum" has no meaning in the orthogonal capability
    /// model. Use `withCapabilities` to filter by specific capabilities instead.
    /// At most one of the two may be set; omitting both applies no
    /// capability filter.
    #[graphql(
        deprecation = "a minimum capability has no meaning in the orthogonal capability model; use withCapabilities instead."
    )]
    pub min_capability: Option<models::Capability>,
    /// Filter to prefixes where the user holds all of these fine-grained
    /// capabilities.
    pub with_capabilities: Option<Vec<models::authz::Capability>>,
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
        after: Option<String>,
        first: Option<i32>,
    ) -> async_graphql::Result<PaginatedPrefixes> {
        let env = ctx.data::<crate::Envelope>()?;

        // Legacy `minCapability` (a threshold on the ordered read/write/admin
        // scale) and `withCapabilities` (fine-grained capability bits) both
        // reduce to a required `CapabilitySet`; a prefix matches when the
        // user's capabilities there are a superset of it. With neither set the
        // required set is empty, so every reachable prefix matches (no filter).
        let required_bits: models::authz::CapabilitySet =
            match (by.min_capability, by.with_capabilities) {
                (Some(legacy), None) => legacy.into(),
                (None, Some(bits)) => bits.into_iter().collect(),
                (None, None) => models::authz::CapabilitySet::empty(),
                (Some(_), Some(_)) => {
                    return Err(async_graphql::Error::new(
                        "provide at most one of `minCapability` (deprecated) or `withCapabilities`",
                    ));
                }
            };

        connection::query(after, None, first, None, |after, _, first, _| async move {
            let snapshot = env.snapshot();
            let user_id = env.claims()?.sub;

            let reachable = tables::UserGrant::reachable_prefixes(
                &snapshot.role_grants,
                &snapshot.user_grants,
                user_id,
            );
            // Cursor pagination: BTreeMap::range jumps directly to the
            // first key strictly greater than the previous page's last
            // prefix, rather than iterating from the start and filtering
            // past it.
            let start = after
                .as_deref()
                .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
            let all_roles: Vec<PrefixRef> = reachable
                .range::<str, _>((start, std::ops::Bound::Unbounded))
                .filter(|(_, (bits, _))| bits.is_superset(required_bits))
                .map(|(prefix, (bits, legacy))| PrefixRef {
                    prefix: models::Prefix::new(*prefix),
                    user_capability: *legacy,
                    capabilities: bits.iter().collect(),
                    capability_bundles: models::authz::CapabilityBundle::covered_by(*bits),
                    capability_bits: bits.iter().collect(),
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
                                    capabilityBundles
                                    capabilityBits
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
                      "CatalogRead",
                      "JournalRead",
                      "JournalAppend",
                      "SpecEdit",
                      "CreateGrant",
                      "DeleteGrant",
                      "CreateInviteLink",
                      "ViewDataPlanePrivateNetworking",
                      "ModifyDataPlanePrivateNetworking",
                      "ViewBilling",
                      "EditBilling",
                      "Delegate"
                    ],
                    "capabilityBits": [
                      "CatalogRead",
                      "JournalRead",
                      "JournalAppend",
                      "SpecEdit",
                      "CreateGrant",
                      "DeleteGrant",
                      "CreateInviteLink",
                      "ViewDataPlanePrivateNetworking",
                      "ModifyDataPlanePrivateNetworking",
                      "ViewBilling",
                      "EditBilling",
                      "Delegate"
                    ],
                    "capabilityBundles": [
                      "View",
                      "Write",
                      "Edit",
                      "Admin",
                      "ManageBilling",
                      "ManageUsers",
                      "ManageDataPlanes",
                      "Delegate"
                    ],
                    "prefix": "aliceCo/",
                    "userCapability": "admin"
                  }
                },
                {
                  "node": {
                    "capabilities": [
                      "CatalogRead",
                      "JournalRead",
                      "JournalAppend",
                      "ViewDataPlanePrivateNetworking"
                    ],
                    "capabilityBits": [
                      "CatalogRead",
                      "JournalRead",
                      "JournalAppend",
                      "ViewDataPlanePrivateNetworking"
                    ],
                    "capabilityBundles": [
                      "View",
                      "Write"
                    ],
                    "prefix": "aliceCo/data/",
                    "userCapability": "write"
                  }
                },
                {
                  "node": {
                    "capabilities": [
                      "CatalogRead",
                      "JournalRead",
                      "ViewDataPlanePrivateNetworking"
                    ],
                    "capabilityBits": [
                      "CatalogRead",
                      "JournalRead",
                      "ViewDataPlanePrivateNetworking"
                    ],
                    "capabilityBundles": [
                      "View"
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
              "message": "status: 'The request does not have valid authentication credentials', self: \"This is an authenticated API but the request is missing a required Authorization: Bearer token\"",
              "path": [
                "prefixes"
              ]
            }
          ]
        }
        "#);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_graphql_prefixes_input_migration(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let server =
            test_server::TestServer::start(pool.clone(), test_server::snapshot(pool, false).await)
                .await;

        let token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        // The `withCapabilities` filter is accepted and returns the prefixes
        // where the user holds all of the listed fine-grained capabilities.
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        prefixes(by: { withCapabilities: [SpecEdit] }) {
                            edges {
                                node {
                                    prefix
                                    userCapability
                                }
                            }
                        }
                    }
                "#
                }),
                Some(&token),
            )
            .await;

        assert!(
            response.get("errors").is_none(),
            "withCapabilities query returned errors: {response}"
        );
        let edges = response["data"]["prefixes"]["edges"]
            .as_array()
            .expect("edges array");
        assert!(
            !edges.is_empty(),
            "withCapabilities: [SpecEdit] should return prefixes: {response}"
        );

        // Listing multiple capabilities requires the user to hold all of
        // them, so the result can only narrow: every prefix returned for
        // [SpecEdit, CreateGrant] must also appear in the [SpecEdit] result.
        let narrowed: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        prefixes(by: { withCapabilities: [SpecEdit, CreateGrant] }) {
                            edges { node { prefix } }
                        }
                    }
                "#
                }),
                Some(&token),
            )
            .await;
        assert!(
            narrowed.get("errors").is_none(),
            "multi-capability withCapabilities query returned errors: {narrowed}"
        );
        let edit_prefixes: Vec<&str> = edges
            .iter()
            .map(|e| e["node"]["prefix"].as_str().expect("prefix string"))
            .collect();
        for edge in narrowed["data"]["prefixes"]["edges"]
            .as_array()
            .expect("edges array")
        {
            let prefix = edge["node"]["prefix"].as_str().expect("prefix string");
            assert!(
                edit_prefixes.contains(&prefix),
                "prefix {prefix} returned for [SpecEdit, CreateGrant] but not for [SpecEdit]: {narrowed}"
            );
        }

        // Supplying both the deprecated and the new input is rejected.
        let both: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        prefixes(by: { minCapability: read, withCapabilities: [SpecEdit] }) {
                            edges { node { prefix } }
                        }
                    }
                "#
                }),
                Some(&token),
            )
            .await;
        assert!(
            both["errors"][0]["message"]
                .as_str()
                .is_some_and(|m| m.contains("at most one")),
            "expected at-most-one error, got: {both}"
        );

        // Supplying neither input applies no capability filter and returns
        // every prefix the caller can reach.
        let neither: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        prefixes(by: {}) {
                            edges { node { prefix } }
                        }
                    }
                "#
                }),
                Some(&token),
            )
            .await;
        assert!(
            neither.get("errors").is_none(),
            "unfiltered prefixes query returned errors: {neither}"
        );
        let neither_edges = neither["data"]["prefixes"]["edges"]
            .as_array()
            .expect("edges array");
        assert!(
            !neither_edges.is_empty(),
            "unfiltered prefixes should return all reachable prefixes: {neither}"
        );
    }
}
