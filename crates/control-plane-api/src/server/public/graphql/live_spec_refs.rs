use async_graphql::{
    dataloader,
    types::connection::{self, Connection},
    ComplexObject, Context, SimpleObject,
};

use std::sync::Arc;

use crate::server::{
    public::graphql::{alerts, live_specs, status, PgDataLoader},
    App, ControlClaims,
};

const DEFAULT_PAGE_SIZE: usize = 50;

/// Input type for returning live specs references by prefix and catalog type.
#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct ByPrefixAndType {
    pub prefix: models::Prefix,
    pub catalog_type: models::CatalogType,
}

#[derive(Debug, Clone, async_graphql::OneofObject)]
pub enum LiveSpecsBy {
    PrefixAndType(ByPrefixAndType),
    Names(Vec<models::Name>),
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
    async fn active_alerts(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Option<Vec<alerts::Alert>>> {
        if self.user_capability.is_none() {
            tracing::info!(catalog_name = %self.catalog_name, "not showing firing_alerts because user is not authorized");
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
            tracing::info!(catalog_name = %self.catalog_name, "not showing alert_history because user is not authorized");
            return Ok(None);
        }
        alerts::live_spec_alert_history(ctx, &self.catalog_name, before, last)
            .await
            .map(|c| Some(c))
    }

    /// Returns the status of the live spec.
    async fn status(&self, ctx: &Context<'_>) -> async_graphql::Result<Option<status::Status>> {
        if self.user_capability.is_none() {
            tracing::info!(catalog_name = %self.catalog_name, "not showing alert_history because user is not authorized");
            return Ok(None);
        }
        let loader = ctx.data::<async_graphql::dataloader::DataLoader<PgDataLoader>>()?;
        let status = loader
            .load_one(status::StatusKey(self.catalog_name.to_string()))
            .await?;
        Ok(status)
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
    if all_names.is_empty() {
        return Ok(connection::Connection::new(false, false));
    }

    let app = ctx.data::<Arc<App>>()?;
    let claims = ctx.data::<ControlClaims>()?;

    let all_refs = app.attach_user_capabilities(claims, all_names, |name, maybe_capability| {
        if require_min_capability.is_some_and(|min_cap| maybe_capability < Some(min_cap)) {
            return None;
        }

        Some(LiveSpecRef {
            catalog_name: models::Name::new(name),
            user_capability: maybe_capability,
        })
    });
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
        match by {
            LiveSpecsBy::PrefixAndType(by_prefix) => {
                fetch_live_specs_by_prefix(ctx, by_prefix, after, before, first, last).await
            }
            LiveSpecsBy::Names(by_name) => {
                fetch_live_specs_by_name(ctx, by_name, after, before, first, last).await
            }
        }
    }
}

async fn fetch_live_specs_by_name(
    ctx: &Context<'_>,
    by_names: Vec<models::Name>,
    after: Option<String>,
    before: Option<String>,
    first: Option<i32>,
    last: Option<i32>,
) -> async_graphql::Result<PaginatedLiveSpecsRefs> {
    let mut names: Vec<String> = by_names.into_iter().map(|n| n.into()).collect();
    // Sort the names, so that we can paginate the results by liexicographic
    // order, just like we do for fetching by prefix.
    names.sort();

    // We essentially just lookup the users capability to each spec that they've
    // requested. There's no verification of whether the live spec exists
    // unless/until the query resolves some sub-field on the `LiveSpecRef`.
    // There's also no error if the user does not have access to the given
    // names. We rely on our existing auth checks in `LiveSpecRef` resolver
    // functions. This allows clients to easily check which specific names the
    // user does not have access to by querying the user capability, just like
    // it does for `readsFrom` and `writesTo`
    paginate_live_specs_refs(ctx, None, names, after, before, first, last).await
}

async fn fetch_live_specs_by_prefix(
    ctx: &Context<'_>,
    by: ByPrefixAndType,
    after: Option<String>,
    before: Option<String>,
    first: Option<i32>,
    last: Option<i32>,
) -> async_graphql::Result<PaginatedLiveSpecsRefs> {
    let ByPrefixAndType {
        prefix,
        catalog_type,
    } = by;
    let app = ctx.data::<Arc<App>>()?;
    let claims = ctx.data::<ControlClaims>()?;

    let _ = app
        .verify_user_authorization(claims, vec![prefix.to_string()], models::Capability::Read)
        .await?;

    let pg_pool = app.pg_pool.clone();
    let (names, has_prev, has_next) =
        connection::query_with::<String, _, _, _, async_graphql::Error>(
            after,
            before,
            first,
            last,
            |after, before, first, last| async move {
                let db = pg_pool;
                let limit = first.or(last).unwrap_or(DEFAULT_PAGE_SIZE);

                let result = if before.is_some() || last.is_some() {
                    let names = fetch_live_specs_names_before(
                        &db,
                        prefix.as_str(),
                        catalog_type,
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
                        &db,
                        prefix.as_str(),
                        catalog_type,
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
    let edges = app.attach_user_capabilities(claims, names, |name, user_capability| {
        Some(connection::Edge::new(
            name.clone(),
            LiveSpecRef {
                catalog_name: models::Name::new(name),
                user_capability,
            },
        ))
    });

    let mut conn = PaginatedLiveSpecsRefs::new(has_prev, has_next);
    conn.edges = edges;
    async_graphql::Result::<PaginatedLiveSpecsRefs>::Ok(conn)
}

async fn fetch_live_specs_names_after(
    db: &sqlx::PgPool,
    prefix: &str,
    catalog_type: models::CatalogType,
    after: Option<&str>,
    limit: i64,
) -> anyhow::Result<Vec<String>> {
    let names = sqlx::query_scalar!(
        r#"select catalog_name as "name!: String"
        from live_specs
        where starts_with(catalog_name, $1)
        and case when $3::catalog_name is null then true else catalog_name > $3::catalog_name end
        and spec_type = $2::catalog_spec_type
        order by catalog_name asc
        limit $4"#,
        prefix as &str,
        catalog_type as models::CatalogType,
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
    prefix: &str,
    catalog_type: models::CatalogType,
    before: Option<&str>,
    limit: i64,
) -> anyhow::Result<Vec<String>> {
    let mut names = sqlx::query_scalar!(
        r#"select catalog_name as "name!: String"
        from live_specs
        where starts_with(catalog_name, $1)
        and case when $3::catalog_name is null then true else catalog_name < $3::catalog_name end
        and spec_type = $2::catalog_spec_type
        order by catalog_name desc
        limit $4"#,
        prefix as &str,
        catalog_type as models::CatalogType,
        before as Option<&str>,
        limit
    )
    .fetch_all(db)
    .await?;

    names.reverse();
    Ok(names)
}
