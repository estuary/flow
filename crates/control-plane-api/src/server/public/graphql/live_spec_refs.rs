use crate::{
    alerts::Alert,
    server::public::graphql::{
        PgDataLoader, alerts, filters, live_specs, publication_history, status,
    },
};
use async_graphql::{
    ComplexObject, Context, SimpleObject, dataloader,
    types::connection::{self, Connection},
};

const DEFAULT_PAGE_SIZE: usize = 50;
const MAX_PREFIXES: usize = 20;

/// Input type for querying live specs.
#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct LiveSpecsBy {
    /// Fetch live specs by name. Required if `prefix` is empty
    pub names: Option<Vec<models::Name>>,
    /// Fetch live specs by prefix. Required if `names` is empty
    pub prefix: Option<models::Prefix>,
    /// Optionally filter by catalogType
    pub catalog_type: Option<models::CatalogType>,
    /// Optionally filter by dataPlane name
    pub data_plane_name: Option<models::Name>,
}

/// Composable filter for the `liveSpecs` query. Every field is optional and
/// only narrows the result set; the caller's catalog-read scope is enforced
/// independently, so a filter can never widen what a caller may see.
#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct LiveSpecsFilter {
    /// Narrow by catalog name. `startsWith` matches a whole subtree — specs
    /// under `acmeCo/`, `acmeCo/team/`, etc. — like the deprecated
    /// `by: { prefix }`. `in` matches an exact set of names, like
    /// `by: { names }`. The two are alternative query modes and are mutually
    /// exclusive. Either way, results compose with (never widen past) the
    /// caller's authorized read prefixes.
    pub catalog_name: Option<filters::PrefixFilter>,
}

/// Represents a reference from one live spec to another.
#[derive(Debug, Clone, SimpleObject)]
#[graphql(complex)]
pub struct LiveSpecRef {
    /// The catalog_name of the referent.
    pub catalog_name: models::Name,
    /// The current user's capability to the referent. Null indicates no access.
    /// A query can obtain a reference to a catalog spec that the user has no
    /// access to, which happens in scenarios where a LiveSpec that the user
    /// does have access to references a spec in a different catalog namespace
    /// that the user cannot access. It can also happen simply by listing by
    /// name, and passing a name that the user cannot access. In either case,
    /// the result would be `userCapability: null`, and all other fields on the
    /// LiveSpecRef would also be null.
    pub user_capability: Option<models::Capability>,
}

#[ComplexObject]
impl LiveSpecRef {
    /// Returns the live spec that the reference points to, if the user has access to it.
    async fn live_spec(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Option<live_specs::LiveSpec>> {
        if self.user_capability.is_none() {
            return Ok(None);
        }

        let lookahead = ctx.look_ahead();
        let with_model = lookahead.field("model").exists();
        let with_built = lookahead.field("builtSpec").exists();
        let loader = ctx.data::<dataloader::DataLoader<PgDataLoader>>()?;
        let key = live_specs::LiveSpecKey {
            catalog_name: self.catalog_name.to_string(),
            with_built,
            with_model,
        };
        let live_spec = loader.load_one(key).await?;
        Ok(live_spec)
    }

    /// Returns all alerts that are currently firing for this live spec.
    async fn active_alerts(&self, ctx: &Context<'_>) -> async_graphql::Result<Option<Vec<Alert>>> {
        if self.user_capability.is_none() {
            return Ok(None);
        }
        let loader = ctx.data::<async_graphql::dataloader::DataLoader<PgDataLoader>>()?;
        let alerts = loader
            .load_one(alerts::ActiveAlerts(self.catalog_name.to_string()))
            .await?;
        // The result should be `Some(vec![])` if the user has access, but there are no active alerts.
        Ok(Some(alerts.unwrap_or_default()))
    }

    /// Returns the history of resolved alerts for this live spec. Alerts are
    /// returned in reverse chronological order based on the `firedAt`
    /// timestamp, and are paginated.
    async fn alert_history(
        &self,
        ctx: &Context<'_>,
        before: Option<String>,
        last: i32,
    ) -> async_graphql::Result<Option<alerts::PaginatedAlerts>> {
        if self.user_capability.is_none() {
            return Ok(None);
        }
        alerts::live_spec_alert_history_no_authz(ctx, &self.catalog_name, before, last)
            .await
            .map(|c| Some(c))
    }

    /// Returns the status of the live spec.
    async fn status(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Option<status::LiveSpecStatus>> {
        if self.user_capability.is_none() {
            return Ok(None);
        }
        let loader = ctx.data::<async_graphql::dataloader::DataLoader<PgDataLoader>>()?;
        let status = loader
            .load_one(status::StatusKey(self.catalog_name.to_string()))
            .await?;
        Ok(status)
    }

    /// Information about the most recent publication of the spec
    async fn last_publication(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Option<publication_history::SpecPublicationHistoryItem>> {
        if self.user_capability.is_none() {
            return Ok(None);
        }

        let include_model = ctx.look_ahead().field("model").exists();
        let key = publication_history::LastPublicationInfoKey {
            catalog_name: self.catalog_name.clone(),
            include_model,
        };

        let loader = ctx.data::<async_graphql::dataloader::DataLoader<PgDataLoader>>()?;
        let pub_info = loader.load_one(key).await?;
        Ok(pub_info)
    }

    /// The complete history of publications of this spec
    async fn publication_history(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        first: Option<i32>,
        before: Option<String>,
        last: Option<i32>,
    ) -> async_graphql::Result<Option<publication_history::SpecHistoryConnection>> {
        if self.user_capability.is_none() {
            return Ok(None);
        }
        let include_model = ctx
            .look_ahead()
            .field("edges")
            .field("node")
            .field("model")
            .exists();
        let history = publication_history::fetch_spec_history_no_authz(
            ctx,
            self.catalog_name.clone(),
            include_model,
            after,
            first,
            before,
            last,
        )
        .await?;
        Ok(Some(history))
    }
}

/// Applies the given pagination parameters to `all_names` and returns a
/// `Connection` suitable for a graphql response. `all_names` is expected to
/// contain the complete list of **sorted** live specs names. Note that the sort
/// order, both of `all_names` and the query results, must always be ascending,
/// regardless of whether forward or reverse pagination is being used. Source:
/// https://relay.dev/graphql/connections.htm#sec-Edge-order
/// If `require_min_capability` is `Some`, then `all_specs` will be filtered to
/// only include those specs for which the user has the required minimum
/// capability.
pub async fn paginate_live_specs_refs(
    ctx: &Context<'_>,
    require_min_capability: Option<models::Capability>,
    all_names: Vec<String>,
    after: Option<String>,
    before: Option<String>,
    first: Option<i32>,
    last: Option<i32>,
) -> async_graphql::Result<PaginatedLiveSpecsRefs> {
    let env = ctx.data::<crate::Envelope>()?;

    if all_names.is_empty() {
        return Ok(connection::Connection::new(false, false));
    }
    let all_refs = crate::server::attach_user_capabilities(
        env.snapshot(),
        env.claims()?,
        all_names,
        |name, maybe_capability| {
            if require_min_capability.is_some_and(|min_cap| maybe_capability < Some(min_cap)) {
                return None;
            }
            Some(LiveSpecRef {
                catalog_name: models::Name::new(name),
                user_capability: maybe_capability,
            })
        },
    );
    apply_pagination(all_refs, after, before, first, last).await
}

async fn apply_pagination(
    mut all_refs: Vec<LiveSpecRef>,
    after: Option<String>,
    before: Option<String>,
    first: Option<i32>,
    last: Option<i32>,
) -> async_graphql::Result<PaginatedLiveSpecsRefs> {
    connection::query_with::<String, _, _, _, String>(
        after,
        before,
        first,
        last,
        |after, before, first, last| async move {
            // Which direction to paginate in? Default to forward, if no parameters were given.
            let (start_index, end_index) = if before.is_some() || last.is_some() {
                let end = if let Some(before_name) = &before {
                    all_refs.partition_point(|r| r.catalog_name.as_str() < before_name.as_str())
                } else {
                    all_refs.len()
                };
                let start = end.saturating_sub(last.unwrap_or(all_refs.len()));
                (start, end)
            } else {
                let start = if let Some(after_name) = &after {
                    all_refs.partition_point(|r| r.catalog_name.as_str() <= after_name.as_str())
                } else {
                    0
                };
                (start, first.unwrap_or(usize::MAX).min(all_refs.len()))
            };
            let has_prev = start_index > 0;
            let has_next = end_index < all_refs.len().saturating_sub(1);
            let edges = all_refs
                .drain(start_index..end_index)
                .map(|r| connection::Edge::new(r.catalog_name.to_string(), r))
                .collect();
            let mut conn = PaginatedLiveSpecsRefs::new(has_prev, has_next);
            conn.edges = edges;

            async_graphql::Result::Ok(conn)
        },
    )
    .await
}

pub type PaginatedLiveSpecsRefs = Connection<
    String,
    LiveSpecRef,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

#[derive(Debug, Default)]
pub struct LiveSpecsQuery;

#[async_graphql::Object]
impl LiveSpecsQuery {
    /// Returns a paginated list of live specs accessible to the current user.
    ///
    /// Omitting both `by` and `filter` returns every live spec under every
    /// prefix where the caller has catalog-read capability, and the optional
    /// `filter` narrows those authorized results. The deprecated `by` instead
    /// requires an explicit `names` or `prefix` selection, and fails the
    /// entire request if any part of that selection is unauthorized.
    ///
    /// Note that the `user_capability` that's returned as part of the reference
    /// represents the user's capability to the whole prefix, and it is possible
    /// that there are more specific grants for a broader capability. In other
    /// words, this capability represents the _minimum_ capability that the user
    /// has for the given spec.
    pub async fn live_specs(
        &self,
        ctx: &Context<'_>,
        #[graphql(
            deprecation = "Prefer `filter: { catalogName }`: `startsWith` replaces `prefix` and \
                                 `in` replaces `names`. `by` is retained only for existing clients."
        )]
        by: Option<LiveSpecsBy>,
        filter: Option<LiveSpecsFilter>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> async_graphql::Result<PaginatedLiveSpecsRefs> {
        let env = ctx.data::<crate::Envelope>()?;

        // `filter` is the going-forward replacement for `by`, and the two are
        // mutually exclusive. They also authorize differently: `by` fails the
        // entire request on an unauthorized name or prefix, while `filter`
        // only narrows the caller's authorized prefixes and can never widen
        // them. Both resolve into the parameters of one shared SQL query.
        let (names, prefix, catalog_type, data_plane, exact, read_prefixes) = match (
            by,
            filter.and_then(|f| f.catalog_name),
        ) {
            (Some(by), filter_catalog_name) => {
                if filter_catalog_name
                    .is_some_and(|cn| cn.starts_with.is_some() || cn.r#in.is_some())
                {
                    return Err(
                        "provide either `by` or `filter`, not both; `by` is deprecated".into(),
                    );
                }
                let LiveSpecsBy {
                    names,
                    prefix,
                    catalog_type,
                    data_plane_name: data_plane,
                } = by;
                let names = names.unwrap_or_default();

                // Fail the entire request if it passed a name or prefix that the user is unauthorized to.
                let policy_result = crate::server::evaluate_names_authorization(
                    env.snapshot(),
                    env.claims()?,
                    models::Capability::Read,
                    names
                        .iter()
                        .map(models::Name::as_str)
                        .chain(prefix.as_ref().map(models::Prefix::as_str).into_iter()),
                );
                let (_expiry, ()) = env.authorization_outcome(policy_result).await?;

                if names.is_empty() && prefix.is_none() {
                    return Err(
                        "must provide at least one of `names` or `prefix`, or omit `by` entirely"
                            .into(),
                    );
                }
                let prefix = prefix.map(|p| p.to_string());
                (
                    names,
                    prefix,
                    catalog_type,
                    data_plane,
                    Vec::new(),
                    Vec::new(),
                )
            }
            (None, filter_catalog_name) => {
                let snapshot = env.snapshot();
                let (read_prefixes, starts_with, exact) =
                    super::authorized_prefixes::filtered_authorized_prefixes(
                        &snapshot.role_grants,
                        &snapshot.user_grants,
                        env.claims()?.sub,
                        models::authz::Capability::CatalogRead,
                        filter_catalog_name,
                        "filter.catalogName",
                    )
                    .map(|(prefixes, starts_with, r#in)| {
                        (prefixes, starts_with, r#in.unwrap_or_default())
                    })?;

                if read_prefixes.is_empty() {
                    return Ok(PaginatedLiveSpecsRefs::new(false, false));
                }
                if read_prefixes.len() > MAX_PREFIXES {
                    return Err(async_graphql::Error::new(
                        "Too many accessible prefixes; narrow results with a filter",
                    ));
                }
                (Vec::new(), starts_with, None, None, exact, read_prefixes)
            }
        };

        let (names, has_prev, has_next) =
            connection::query_with::<String, _, _, _, async_graphql::Error>(
                after,
                before,
                first,
                last,
                |after, before, first, last| async move {
                    let limit = first.or(last).unwrap_or(DEFAULT_PAGE_SIZE);
                    if limit == 0 {
                        return Ok((Vec::new(), false, false));
                    }

                    let result = if before.is_some() || last.is_some() {
                        let names = fetch_live_specs_names_before(
                            &env.pg_pool,
                            names,
                            prefix.as_deref(),
                            catalog_type,
                            data_plane.as_deref(),
                            &exact,
                            &read_prefixes,
                            before.as_deref(),
                            limit as i64,
                        )
                        .await
                        .map_err(async_graphql::Error::from)?;
                        // There is a previous page if there were enough names to fill this page.
                        let has_prev = names.len() == limit;
                        // There is implicitly a next page if this request provided a before cursor.
                        (names, has_prev, before.is_some())
                    } else {
                        // Default to forward pagination unless before or last is specified
                        let names = fetch_live_specs_names_after(
                            &env.pg_pool,
                            names,
                            prefix.as_deref(),
                            catalog_type,
                            data_plane.as_deref(),
                            &exact,
                            &read_prefixes,
                            after.as_deref(),
                            limit as i64,
                        )
                        .await
                        .map_err(async_graphql::Error::from)?;
                        // There is implicitly a previous page if this request provided an after cursor.
                        // There is a next page if there were enough names to fill this page.
                        let has_next = names.len() == limit;
                        (names, after.is_some(), has_next)
                    };

                    async_graphql::Result::Ok(result)
                },
            )
            .await?;

        // We already know that the user at least has read capability to the prefix,
        // but it's possible that they may have a greater capability to specific
        // sub-prefixes, so resolve those here.
        let edges = crate::server::attach_user_capabilities(
            env.snapshot(),
            env.claims()?,
            names,
            |name, user_capability| {
                Some(connection::Edge::new(
                    name.clone(),
                    LiveSpecRef {
                        catalog_name: models::Name::new(name),
                        user_capability,
                    },
                ))
            },
        );

        let mut conn = PaginatedLiveSpecsRefs::new(has_prev, has_next);
        conn.edges = edges;
        async_graphql::Result::<PaginatedLiveSpecsRefs>::Ok(conn)
    }
}

async fn fetch_live_specs_names_after(
    db: &sqlx::PgPool,
    names: Vec<models::Name>,
    prefix: Option<&str>,
    catalog_type: Option<models::CatalogType>,
    data_plane: Option<&str>,
    exact: &[String],
    read_prefixes: &[String],
    after: Option<&str>,
    limit: i64,
) -> anyhow::Result<Vec<String>> {
    assert!(
        !names.is_empty() || prefix.is_some() || !read_prefixes.is_empty(),
        "must have a name, prefix, or read-prefixes predicate when querying live specs"
    );
    // `exact` and `read_prefixes` are caller- and grant-derived strings, so
    // they bind as text[] rather than the catalog_name domain array: sqlx does
    // not run domain-constraint validation on bind.
    let names = sqlx::query_scalar!(
        r#"select ls.catalog_name as "name!: String"
        from live_specs ls
        left outer join data_planes dp on ls.data_plane_id = dp.id
        where (coalesce(array_length($1::catalog_name[], 1), 0) = 0 or ls.catalog_name = any($1::catalog_name[]))
        and ($2::text is null or ls.catalog_name::text ^@ $2::text)
        and ($3::catalog_spec_type is null or ls.spec_type = $3::catalog_spec_type)
        and ($4::text is null or $4::text = dp.data_plane_name)
        and (coalesce(array_length($5::text[], 1), 0) = 0 or ls.catalog_name::text = any($5::text[]))
        and (coalesce(array_length($6::text[], 1), 0) = 0 or ls.catalog_name::text ^@ any($6::text[]))
        and ($7::catalog_name is null or ls.catalog_name > $7::catalog_name)
        order by ls.catalog_name asc
        limit $8"#,
        names as Vec<models::Name>,
        prefix as Option<&str>,
        catalog_type as Option<models::CatalogType>,
        data_plane as Option<&str>,
        exact as &[String],
        read_prefixes as &[String],
        after as Option<&str>,
        limit
    )
    .fetch_all(db)
    .await?;
    Ok(names)
}

/// Fetches names for reverse-paginated query. Note that the names must still
/// be returned in asc order, according to:
/// https://relay.dev/graphql/connections.htm#sec-Edge-order
async fn fetch_live_specs_names_before(
    db: &sqlx::PgPool,
    names: Vec<models::Name>,
    prefix: Option<&str>,
    catalog_type: Option<models::CatalogType>,
    data_plane: Option<&str>,
    exact: &[String],
    read_prefixes: &[String],
    before: Option<&str>,
    limit: i64,
) -> anyhow::Result<Vec<String>> {
    assert!(
        !names.is_empty() || prefix.is_some() || !read_prefixes.is_empty(),
        "must have a name, prefix, or read-prefixes predicate when querying live specs"
    );
    // `exact` and `read_prefixes` are caller- and grant-derived strings, so
    // they bind as text[] rather than the catalog_name domain array: sqlx does
    // not run domain-constraint validation on bind.
    let mut names = sqlx::query_scalar!(
        r#"select ls.catalog_name as "name!: String"
        from live_specs ls
        left outer join data_planes dp on ls.data_plane_id = dp.id
        where (coalesce(array_length($1::catalog_name[], 1), 0) = 0 or ls.catalog_name = any($1::catalog_name[]))
        and ($2::text is null or ls.catalog_name::text ^@ $2::text)
        and ($3::catalog_spec_type is null or ls.spec_type = $3::catalog_spec_type)
        and ($4::text is null or $4::text = dp.data_plane_name)
        and (coalesce(array_length($5::text[], 1), 0) = 0 or ls.catalog_name::text = any($5::text[]))
        and (coalesce(array_length($6::text[], 1), 0) = 0 or ls.catalog_name::text ^@ any($6::text[]))
        and ($7::catalog_name is null or ls.catalog_name < $7::catalog_name)
        order by ls.catalog_name desc
        limit $8"#,
        names as Vec<models::Name>,
        prefix as Option<&str>,
        catalog_type as Option<models::CatalogType>,
        data_plane as Option<&str>,
        exact as &[String],
        read_prefixes as &[String],
        before as Option<&str>,
        limit
    )
    .fetch_all(db)
    .await?;

    names.reverse();
    Ok(names)
}

#[cfg(test)]
mod test {
    use crate::test_server;

    // Helper: run the query with the given variables and return the list of
    // returned catalog names, asserting no GraphQL errors.
    async fn query_names(
        server: &test_server::TestServer,
        token: &str,
        variables: serde_json::Value,
    ) -> Vec<String> {
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                        query($by: LiveSpecsBy, $filter: LiveSpecsFilter) {
                            liveSpecs(by: $by, filter: $filter) {
                                edges { node { catalogName } }
                            }
                        }
                    "#,
                    "variables": variables,
                }),
                Some(token),
            )
            .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response}"
        );
        response["data"]["liveSpecs"]["edges"]
            .as_array()
            .expect("edges array")
            .iter()
            .map(|edge| edge["node"]["catalogName"].as_str().unwrap().to_string())
            .collect()
    }

    // Helper: assert a set of variables is rejected with a GraphQL error whose
    // first message contains `expected_message`.
    async fn expect_error(
        server: &test_server::TestServer,
        token: &str,
        variables: serde_json::Value,
        expected_message: &str,
    ) {
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                        query($by: LiveSpecsBy, $filter: LiveSpecsFilter) {
                            liveSpecs(by: $by, filter: $filter) {
                                edges { node { catalogName } }
                            }
                        }
                    "#,
                    "variables": variables,
                }),
                Some(token),
            )
            .await;
        let message = response["errors"][0]["message"]
            .as_str()
            .unwrap_or_default();
        assert!(
            message.contains(expected_message),
            "expected error containing {expected_message:?}, got {response} for variables {variables}"
        );
    }

    // The `filter` argument scopes results to the caller's authorized
    // prefixes, narrowing by catalog-name subtree (`startsWith`) or exact set
    // (`in`). The deprecated `by` keeps its stricter contract: it requires a
    // `names` or `prefix` selection and fails outright on unauthorized names.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn live_specs_filter_scopes_by_catalog_name(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let snapshot = test_server::snapshot(pool.clone(), false).await;
        let server = test_server::TestServer::start(pool.clone(), snapshot).await;
        let alice_token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);
        let bob_token = server.make_access_token(uuid::Uuid::from_bytes([0x22; 16]), None);

        // Omitting both `by` and `filter` returns every readable spec: alice
        // reads aliceCo/ (admin) and ops/dp/public/ (role grant), and the ops
        // fixture specs live outside that scope.
        let no_filter = query_names(&server, &alice_token, serde_json::json!({})).await;
        assert_eq!(
            no_filter,
            vec![
                "aliceCo/data/foo",
                "aliceCo/in/capture-foo",
                "aliceCo/out/materialize-bar"
            ]
        );

        // A `startsWith` filter narrows to the matching subtree, the same
        // result the deprecated `by: { prefix }` produces.
        let narrowed = query_names(
            &server,
            &alice_token,
            serde_json::json!({ "filter": { "catalogName": { "startsWith": "aliceCo/data/" } } }),
        )
        .await;
        assert_eq!(narrowed, vec!["aliceCo/data/foo"]);

        // The filter can never widen scope past the caller's grants.
        let cross_tenant = query_names(
            &server,
            &alice_token,
            serde_json::json!({ "filter": { "catalogName": { "startsWith": "ops/tasks/" } } }),
        )
        .await;
        assert!(cross_tenant.is_empty());

        // An empty filter — and an empty `catalogName` within it — behave like
        // omitting the filter: neither narrows anything.
        let empty_filter =
            query_names(&server, &alice_token, serde_json::json!({ "filter": {} })).await;
        assert_eq!(empty_filter, no_filter);
        let empty_catalog_name = query_names(
            &server,
            &alice_token,
            serde_json::json!({ "filter": { "catalogName": {} } }),
        )
        .await;
        assert_eq!(empty_catalog_name, no_filter);

        // `in` matches an exact set of names, like `by: { names }`. Entries
        // outside the caller's scope or naming nothing are dropped rather than
        // erroring — unlike `by`, which fails the request on the ops entry.
        let exact = query_names(
            &server,
            &alice_token,
            serde_json::json!({
                "filter": { "catalogName": { "in": [
                    "aliceCo/in/capture-foo",
                    "ops/tasks/public/one/logs",
                    "aliceCo/does-not-exist",
                ] } }
            }),
        )
        .await;
        assert_eq!(exact, vec!["aliceCo/in/capture-foo"]);

        // A caller with no grants sees an empty result, not an error.
        let no_access = query_names(&server, &bob_token, serde_json::json!({})).await;
        assert!(no_access.is_empty());

        // `by` and `filter` are mutually exclusive.
        expect_error(
            &server,
            &alice_token,
            serde_json::json!({
                "by": { "prefix": "aliceCo/" },
                "filter": { "catalogName": { "startsWith": "aliceCo/" } },
            }),
            "provide either `by` or `filter`",
        )
        .await;

        // Within a filter, `startsWith` and `in` are mutually exclusive.
        expect_error(
            &server,
            &alice_token,
            serde_json::json!({
                "filter": { "catalogName": {
                    "startsWith": "aliceCo/",
                    "in": ["aliceCo/data/foo"],
                } },
            }),
            "mutually exclusive; provide only one",
        )
        .await;

        // An empty `in` set is rejected at input validation, rather than
        // ambiguously meaning "match nothing" or "match everything".
        expect_error(
            &server,
            &alice_token,
            serde_json::json!({ "filter": { "catalogName": { "in": [] } } }),
            "",
        )
        .await;

        // The deprecated `by` keeps requiring a `names` or `prefix` selection...
        expect_error(
            &server,
            &alice_token,
            serde_json::json!({ "by": { "catalogType": "capture" } }),
            "must provide at least one of `names` or `prefix`, or omit `by` entirely",
        )
        .await;

        // ...and keeps failing the entire request on an unauthorized prefix,
        // where `filter` returns an empty result instead.
        expect_error(
            &server,
            &alice_token,
            serde_json::json!({ "by": { "prefix": "ops/tasks/" } }),
            "not authorized",
        )
        .await;
    }

    // A caller who can read more than MAX_PREFIXES prefixes is refused an
    // unfiltered listing, but succeeds once a filter narrows the authorized
    // set back under the cap.
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn live_specs_filter_narrows_below_max_prefixes(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let carol_id = uuid::Uuid::from_bytes([0x33; 16]);
        sqlx::query("insert into auth.users (id, email) values ($1, 'carol@example.com')")
            .bind(carol_id)
            .execute(&pool)
            .await
            .unwrap();
        for i in 0..super::MAX_PREFIXES + 1 {
            sqlx::query(
                "insert into user_grants (user_id, object_role, capability) values ($1, $2, 'read')",
            )
            .bind(carol_id)
            .bind(format!("tenant{i:02}/"))
            .execute(&pool)
            .await
            .unwrap();
        }

        let snapshot = test_server::snapshot(pool.clone(), false).await;
        let server = test_server::TestServer::start(pool.clone(), snapshot).await;
        let carol_token = server.make_access_token(carol_id, None);

        expect_error(
            &server,
            &carol_token,
            serde_json::json!({}),
            "Too many accessible prefixes",
        )
        .await;

        // Both filter modes narrow the authorized set back under the cap. No
        // specs exist under these prefixes, so the results are simply empty.
        let narrowed = query_names(
            &server,
            &carol_token,
            serde_json::json!({ "filter": { "catalogName": { "startsWith": "tenant00/" } } }),
        )
        .await;
        assert!(narrowed.is_empty());
        let narrowed = query_names(
            &server,
            &carol_token,
            serde_json::json!({ "filter": { "catalogName": { "in": ["tenant01/some/spec"] } } }),
        )
        .await;
        assert!(narrowed.is_empty());
    }
}
