use crate::{
    alerts::Alert,
    server::public::graphql::{PgDataLoader, alerts, live_specs, publication_history, status},
};
use async_graphql::{
    ComplexObject, Context, SimpleObject, dataloader,
    types::connection::{self, Connection},
};

const DEFAULT_PAGE_SIZE: usize = 50;

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
    /// Returns a paginated list of live specs under the given prefix and
    /// matching the given type.
    ///
    /// Note that the `user_capability` that's returned as part of the reference
    /// represents the user's capability to the whole prefix, and it is possible
    /// that there are more specific grants for a broader capability. In other
    /// words, this capability represents the _minimum_ capability that the user
    /// has for the given spec.
    pub async fn live_specs(
        &self,
        ctx: &Context<'_>,
        by: LiveSpecsBy,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> async_graphql::Result<PaginatedLiveSpecsRefs> {
        let env = ctx.data::<crate::Envelope>()?;

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
            return Err("must provide at least one of `names` or `prefix`".into());
        }

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
                            prefix,
                            catalog_type,
                            data_plane.as_deref(),
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
                            prefix,
                            catalog_type,
                            data_plane.as_deref(),
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
    prefix: Option<models::Prefix>,
    catalog_type: Option<models::CatalogType>,
    data_plane: Option<&str>,
    after: Option<&str>,
    limit: i64,
) -> anyhow::Result<Vec<String>> {
    assert!(
        !names.is_empty() || prefix.is_some(),
        "must have name or prefix predicate when querying live specs"
    );
    let names = sqlx::query_scalar!(
        r#"select ls.catalog_name as "name!: String"
        from live_specs ls
        left outer join data_planes dp on ls.data_plane_id = dp.id
        where (coalesce(array_length($1::catalog_name[], 1), 0) = 0 or ls.catalog_name = any($1::catalog_name[]))
        and ($2::text is null or ls.catalog_name::text ^@ $2::text)
        and ($3::catalog_spec_type is null or ls.spec_type = $3::catalog_spec_type)
        and ($4::text is null or $4::text = dp.data_plane_name)
        and ($5::catalog_name is null or ls.catalog_name > $5::catalog_name)
        order by ls.catalog_name asc
        limit $6"#,
        names as Vec<models::Name>,
        prefix as Option<models::Prefix>,
        catalog_type as Option<models::CatalogType>,
        data_plane as Option<&str>,
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
    prefix: Option<models::Prefix>,
    catalog_type: Option<models::CatalogType>,
    data_plane: Option<&str>,
    before: Option<&str>,
    limit: i64,
) -> anyhow::Result<Vec<String>> {
    assert!(
        !names.is_empty() || prefix.is_some(),
        "must have name or prefix predicate when querying live specs"
    );
    let mut names = sqlx::query_scalar!(
        r#"select ls.catalog_name as "name!: String"
        from live_specs ls
        left outer join data_planes dp on ls.data_plane_id = dp.id
        where (coalesce(array_length($1::catalog_name[], 1), 0) = 0 or ls.catalog_name = any($1::catalog_name[]))
        and ($2::text is null or ls.catalog_name::text ^@ $2::text)
        and ($3::catalog_spec_type is null or ls.spec_type = $3::catalog_spec_type)
        and ($4::text is null or $4::text = dp.data_plane_name)
        and ($5::catalog_name is null or ls.catalog_name < $5::catalog_name)
        order by ls.catalog_name desc
        limit $6"#,
        names as Vec<models::Name>,
        prefix as Option<models::Prefix>,
        catalog_type as Option<models::CatalogType>,
        data_plane as Option<&str>,
        before as Option<&str>,
        limit
    )
    .fetch_all(db)
    .await?;

    names.reverse();
    Ok(names)
}
