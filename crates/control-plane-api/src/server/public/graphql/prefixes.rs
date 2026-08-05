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
    /// Fine-grained capabilities the user has at this prefix.
    pub capabilities: Vec<models::authz::Capability>,
}

#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct PrefixesBy {
    /// Filter returned prefixes by user capability.
    pub min_capability: models::Capability,
}

/// Composable filter for the `prefixes` query. Every field is optional and only
/// narrows the result set; the caller's reach is resolved independently, so a
/// filter can never widen what they see.
#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct PrefixesFilter {
    /// Keep only prefixes where the caller holds *every* listed capability.
    /// Bits are conjunctive, so `[CatalogRead, SpecEdit]` answers "where may I
    /// both read and publish", not "where may I do either". Omit the field to
    /// list every reachable prefix whatever the caller holds there; an empty
    /// list is rejected during input validation rather than silently matching
    /// nothing.
    ///
    /// This replaces the deprecated `by.minCapability`, which selects a point on
    /// the legacy read/write/admin ladder rather than naming bits. The two are
    /// alternative spellings of one constraint and are mutually exclusive;
    /// `by` does compose with this filter's other fields, which scope by
    /// namespace rather than capability.
    ///
    /// These capabilities also arm `tenant`: when both are given, a prefix must
    /// be one the caller holds them all at *and* one the tenant reaches with
    /// them all.
    #[graphql(validator(min_items = 1))]
    pub with_capabilities: Option<Vec<models::authz::Capability>>,
    /// Keep only prefixes that this tenant reaches through the role-grant
    /// graph — the tenant's own namespace, plus any namespace a qualifying
    /// chain of role grants projects it into. A chain qualifies when it carries
    /// every required capability, so with no capability filter at all any
    /// delegatable chain qualifies.
    ///
    /// The walk starts from the caller's own footholds within the tenant, and
    /// the reachable set is intersected with the caller's authorized prefixes.
    /// The filter therefore narrows a listing to one organization's namespace,
    /// never surfaces a prefix the caller could not already see, and shows only
    /// reach flowing from namespace the caller occupies. Naming a tenant
    /// requires at least one foothold within (or covering) its namespace,
    /// holding the required capabilities there.
    pub tenant: Option<models::Prefix>,
    /// Narrow to a subtree or an exact set of prefixes. The match is against
    /// the returned prefix itself, so `startsWith: "acmeCo/"` keeps `acmeCo/`
    /// and its descendants, and `in` keeps only exact members of the set.
    pub prefix: Option<super::filters::PrefixFilter>,
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
    /// Every prefix the caller reaches through the grant graph, with the
    /// capability bits they hold at each.
    ///
    /// Unfiltered, this is the caller's whole access surface: a prefix is listed
    /// whatever the caller holds there, and each entry's `capabilities` carry
    /// the bits a client gates features on. `filter.withCapabilities` inverts
    /// that read, answering "which prefixes may I do X at" instead.
    ///
    /// Ordered lexically, which walks the prefix tree depth-first: a parent
    /// immediately precedes its own descendants. The cursor is the prefix
    /// itself.
    pub async fn prefixes(
        &self,
        ctx: &Context<'_>,
        #[graphql(
            deprecation = "Prefer `filter: { withCapabilities }`, which names capability bits \
                                 directly instead of a point on the legacy read/write/admin \
                                 ladder. `by` is retained only for existing clients and is \
                                 mutually exclusive with it."
        )]
        by: Option<PrefixesBy>,
        filter: Option<PrefixesFilter>,
        after: Option<String>,
        first: Option<i32>,
    ) -> async_graphql::Result<PaginatedPrefixes> {
        let env = ctx.data::<crate::Envelope>()?;

        let filter = filter.unwrap_or_default();
        // `filter` is the going-forward replacement for `by`. Both name the same
        // capability constraint — `by.minCapability` as a point on the legacy
        // read/write/admin ladder, `filter.withCapabilities` as bits — so they
        // are alternative spellings and mutually exclusive. `by` composes freely
        // with the filter's other fields, which scope by namespace rather than
        // capability.
        //
        // With neither supplied the required set is empty, which every
        // capability set is a superset of, so the same `is_superset` test that
        // narrows a filtered query admits everything for an unfiltered one.
        let required: models::authz::CapabilitySet = match (by, filter.with_capabilities) {
            (Some(_), Some(_)) => {
                return Err(
                    "provide either `by` or `filter.withCapabilities`, not both; `by` is deprecated"
                        .into(),
                );
            }
            (Some(by), None) => by.min_capability.into(),
            (None, Some(bits)) => bits.into_iter().collect(),
            (None, None) => models::authz::CapabilitySet::empty(),
        };

        let tenant = match filter.tenant {
            Some(tenant) => Some(super::tenant::validate_tenant_name(tenant.as_str())?),
            None => None,
        };
        let (starts_with, r#in) = match filter.prefix {
            Some(prefix) => prefix.into_parts("filter.prefix")?,
            None => (None, None),
        };

        connection::query(after, None, first, None, |after, _, first, _| async move {
            let snapshot = env.snapshot();
            let user_id = env.claims()?.sub;

            // The single grant-graph walk this request performs. Both the
            // listing and the tenant scope are derived from it, and it is pure
            // in-memory work over the authorization Snapshot — this resolver
            // never touches the database.
            let reachable = tables::UserGrant::reachable_prefixes(
                &snapshot.role_grants,
                &snapshot.user_grants,
                user_id,
            );

            let tenant_scope = match &tenant {
                Some(tenant) => Some(tenant_scope(
                    &snapshot.role_grants,
                    &reachable,
                    tenant.as_str(),
                    required,
                )?),
                None => None,
            };

            // Cursor pagination: BTreeMap::range jumps directly to the
            // first key strictly greater than the previous page's last
            // prefix, rather than iterating from the start and filtering
            // past it.
            let start = after
                .as_deref()
                .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
            let all_roles: Vec<PrefixRef> = reachable
                .range::<str, _>((start, std::ops::Bound::Unbounded))
                .filter(|(prefix, (bits, _))| {
                    bits.is_superset(required)
                        && tenant_scope.as_ref().is_none_or(|scope| {
                            scope.iter().any(|s| prefix.starts_with(s.as_str()))
                        })
                        && starts_with
                            .as_deref()
                            .is_none_or(|sw| prefix.starts_with(sw))
                        && r#in
                            .as_deref()
                            .is_none_or(|exact| exact.iter().any(|e| e.as_str() == **prefix))
                })
                .map(|(prefix, (bits, legacy))| PrefixRef {
                    prefix: models::Prefix::new(*prefix),
                    user_capability: *legacy,
                    capabilities: bits.iter().collect(),
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

/// Resolves the prefix scope that `filter.tenant` narrows to: the prefixes the
/// tenant reaches with `required`, intersected with the caller's own authorized
/// prefixes so the filter can only ever remove entries. An empty `required`
/// admits any chain the graph can delegate along.
///
/// The walk starts from the caller's footholds within the tenant — their
/// authorized prefixes clamped into its subtree — so the caller witnesses only
/// reach flowing from namespace they occupy. Naming a tenant requires at least
/// one foothold. A tenant's reachable set is derived from `role_grants`, which
/// are not otherwise readable here: without that gate, filtering by a tenant the
/// caller knows nothing about and observing whether prefixes come back in some
/// *other* namespace would reveal that a role grant connects the two. The check
/// is a function of the caller's own grants and the tenant string alone, so the
/// denial itself reveals nothing about the tenant — including whether it exists.
fn tenant_scope(
    role_grants: &tables::RoleGrants,
    reachable: &super::authorized_prefixes::ReachablePrefixMap<'_>,
    tenant: &str,
    required: models::authz::CapabilitySet,
) -> async_graphql::Result<Vec<String>> {
    let caller = super::authorized_prefixes::authorized_from_reachable(reachable, required);

    let seeds = super::authorized_prefixes::intersect_prefixes(&caller, &[tenant.to_string()]);
    if seeds.is_empty() {
        return Err(async_graphql::Error::new(format!(
            "not authorized to filter by tenant '{tenant}'"
        )));
    }

    // The walk starts from the footholds rather than the tenant root, so the
    // caller witnesses only reach flowing from namespace they occupy: edges
    // granted to sibling branches of their footholds contribute nothing.
    let reached =
        super::authorized_prefixes::tenant_reachable_prefixes(role_grants, &seeds, required);

    Ok(super::authorized_prefixes::intersect_prefixes(
        &caller, &reached,
    ))
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
                    "prefix": "aliceCo/",
                    "userCapability": "admin"
                  }
                },
                {
                  "node": {
                    "prefix": "aliceCo/data/",
                    "userCapability": "write"
                  }
                },
                {
                  "node": {
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

    /// Runs `query` as alice and returns the JSON response.
    async fn run(server: &test_server::TestServer, query: &str) -> serde_json::Value {
        let token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);
        server
            .graphql(&serde_json::json!({ "query": query }), Some(&token))
            .await
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_graphql_prefixes_filters(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        // The alice fixture grants admin on aliceCo/, plus role grants reaching
        // aliceCo/data/ and ops/dp/public/. zebraCo/ adds a second root, which
        // sorts last under the lexical ordering this query preserves.
        sqlx::query(
            "INSERT INTO public.user_grants (user_id, object_role, capability)
             VALUES ($1, 'zebraCo/', 'admin')",
        )
        .bind(uuid::Uuid::from_bytes([0x11; 16]))
        .execute(&pool)
        .await
        .unwrap();

        let server =
            test_server::TestServer::start(pool.clone(), test_server::snapshot(pool, false).await)
                .await;

        // `by` is now optional. Omitting every argument lists the caller's whole
        // access surface, in lexical order, whatever they hold at each prefix.
        let response = run(
            &server,
            r#"
            query {
                prefixes {
                    edges { cursor node { prefix userCapability capabilities } }
                }
            }
            "#,
        )
        .await;
        insta::assert_json_snapshot!(response);

        // `withCapabilities` is conjunctive. Alice admins aliceCo/ and zebraCo/,
        // so she holds both bits only there; aliceCo/data/ reaches her with
        // write (no SpecEdit) and ops/dp/public/ with read.
        let response = run(
            &server,
            r#"
            query {
                prefixes(filter: { withCapabilities: [CatalogRead, SpecEdit] }) {
                    edges { node { prefix } }
                }
            }
            "#,
        )
        .await;
        insta::assert_json_snapshot!(response);

        // `by` and `withCapabilities` are alternative spellings of one
        // constraint, so supplying both is rejected.
        let response = run(
            &server,
            r#"
            query {
                prefixes(
                    by: { minCapability: read }
                    filter: { withCapabilities: [SpecEdit] }
                ) {
                    edges { node { prefix } }
                }
            }
            "#,
        )
        .await;
        insta::assert_json_snapshot!(response);

        // The exclusion is narrow: `by` composes with the filter's other fields,
        // which scope by namespace rather than capability. `admin` alone admits
        // the two prefixes she admins, and the aliceCo/ tenant scope excludes
        // zebraCo/, so a result of just aliceCo/ proves both were applied.
        let response = run(
            &server,
            r#"
            query {
                prefixes(by: { minCapability: admin }, filter: { tenant: "aliceCo/" }) {
                    edges { node { prefix } }
                }
            }
            "#,
        )
        .await;
        insta::assert_json_snapshot!(response);

        // A bit no grant carries anywhere yields an empty listing rather than
        // falling back to the unfiltered set.
        let response = run(
            &server,
            r#"
            query {
                prefixes(filter: { withCapabilities: [Assume] }) {
                    edges { node { prefix } }
                }
            }
            "#,
        )
        .await;
        insta::assert_json_snapshot!(response);

        // `startsWith` drills into one subtree, matching the returned prefix
        // itself, so the ops/dp/public/ and zebraCo/ roots drop out.
        let response = run(
            &server,
            r#"
            query {
                prefixes(filter: { prefix: { startsWith: "aliceCo/" } }) {
                    edges { node { prefix } }
                }
            }
            "#,
        )
        .await;
        insta::assert_json_snapshot!(response);

        // `in` matches exactly, selecting aliceCo/data/ without its parent.
        let response = run(
            &server,
            r#"
            query {
                prefixes(filter: { prefix: { in: ["aliceCo/data/", "ghostCo/"] } }) {
                    edges { node { prefix } }
                }
            }
            "#,
        )
        .await;
        insta::assert_json_snapshot!(response);

        // `tenant` keeps what aliceCo/ reaches with the required capabilities —
        // its own namespace plus ops/dp/public/ through the role grant — and
        // drops zebraCo/, which alice sees but aliceCo/ does not reach.
        let response = run(
            &server,
            r#"
            query {
                prefixes(filter: { tenant: "aliceCo/" }) {
                    edges { node { prefix } }
                }
            }
            "#,
        )
        .await;
        insta::assert_json_snapshot!(response);

        // The capabilities also arm the tenant walk: the aliceCo/ ->
        // ops/dp/public/ edge carries only read, so requiring SpecEdit drops it
        // where the unfiltered tenant query keeps it.
        let response = run(
            &server,
            r#"
            query {
                prefixes(filter: { tenant: "aliceCo/", withCapabilities: [SpecEdit] }) {
                    edges { node { prefix } }
                }
            }
            "#,
        )
        .await;
        insta::assert_json_snapshot!(response);

        // Naming a tenant alice has no foothold in is denied. The denial turns
        // only on her own grants, so it reveals nothing about the tenant —
        // including whether it exists.
        let response = run(
            &server,
            r#"
            query {
                prefixes(filter: { tenant: "ghostCo/" }) {
                    edges { node { prefix } }
                }
            }
            "#,
        )
        .await;
        insta::assert_json_snapshot!(response);

        // `startsWith` and `in` are mutually exclusive.
        let response = run(
            &server,
            r#"
            query {
                prefixes(filter: { prefix: { startsWith: "aliceCo/", in: ["aliceCo/"] } }) {
                    edges { node { prefix } }
                }
            }
            "#,
        )
        .await;
        insta::assert_json_snapshot!(response);

        // An empty capability list is rejected during input validation.
        let response = run(
            &server,
            r#"
            query {
                prefixes(filter: { withCapabilities: [] }) {
                    edges { node { prefix } }
                }
            }
            "#,
        )
        .await;
        insta::assert_json_snapshot!(response);
    }
}
