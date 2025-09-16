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

/// Input type for returning live specs references by prefix and catalog type.
#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct ByPrefix {
    pub prefix: models::Prefix,
    pub catalog_type: models::CatalogType,
}

/// Represents a reference from one live spec to another.
#[derive(Debug, Clone, SimpleObject)]
#[graphql(complex)]
pub struct LiveSpecRef {
    /// The catalog_name of the referent.
    pub catalog_name: models::Name,
    /// The current user's capability to the referent. None indicates no access.
    /// Note that
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
            // return Err(async_graphql::Error::new(
            //     "user is not authorized to read this live spec".to_string(),
            // ));
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
        let live_spec = loader.load_one(key).await?.ok_or_else(|| {
            async_graphql::Error::new(format!("no live spec found for {}", self.catalog_name))
        })?;
        Ok(Some(live_spec))
    }

    /// Returns all alerts that are currently firing for this live spec.
    async fn firing_alerts(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<alerts::Alert>> {
        if self.user_capability.is_none() {
            tracing::info!(catalog_name = %self.catalog_name, "not showing firing_alerts because user is not authorized");
            return Ok(Vec::new());
        }
        let loader = ctx.data::<async_graphql::dataloader::DataLoader<PgDataLoader>>()?;
        let alerts = loader
            .load_one(alerts::FiringAlerts(self.catalog_name.to_string()))
            .await?;
        Ok(alerts.unwrap_or_default())
    }

    /// Returns the history of resolved alerts for this live spec. Alerts are
    /// returned in reverse chronological order based on the `firedAt`
    /// timestamp, and are paginated.
    async fn alert_history(
        &self,
        ctx: &Context<'_>,
        before: Option<String>,
        last: i32,
    ) -> async_graphql::Result<alerts::PaginatedAlerts> {
        if self.user_capability.is_none() {
            tracing::info!(catalog_name = %self.catalog_name, "not showing alert_history because user is not authorized");
            return Ok(alerts::PaginatedAlerts::new(false, false));
        }
        alerts::live_spec_alert_history(ctx, &self.catalog_name, before, last).await
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

/// Applies the given pagination parameters to `all_names` and returns a `Connection` suitable for a graphql response.
/// `all_names` is expected to contain the complete list of **sorted** live specs names.
pub async fn paginate_live_specs_refs(
    ctx: &Context<'_>,
    all_names: &[String],
    after: Option<String>,
    first: Option<i32>,
) -> async_graphql::Result<PaginatedLiveSpecsRefs> {
    if all_names.is_empty() {
        return Ok(connection::Connection::new(false, false));
    }

    let app = ctx.data::<Arc<App>>()?;
    let claims = ctx.data::<ControlClaims>()?;

    connection::query(after, None, first, None, |after, _, first, _| async move {
        let mut has_next = false;
        let mut name_slice = all_names;
        if let Some(after_name) = &after {
            let cursor_idx = all_names.partition_point(|name| name <= after_name);
            name_slice = &name_slice[cursor_idx..];
        }
        if let Some(take) = first {
            has_next = name_slice.len() > take;
            name_slice = &name_slice[..take.min(name_slice.len())];
        }

        let edges: Vec<connection::Edge<String, LiveSpecRef, connection::EmptyFields>> = app
            .attach_user_capabilities(
                claims,
                name_slice.iter().map(|n| n.to_string()),
                |catalog_name, user_capability| Some(new_ref_edge(&catalog_name, user_capability)),
            );
        let mut conn = PaginatedLiveSpecsRefs::new(false, has_next);
        conn.edges = edges;
        Result::<PaginatedLiveSpecsRefs, async_graphql::Error>::Ok(conn)
    })
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
        by: ByPrefix,
        after: Option<String>,
        first: Option<i32>,
    ) -> async_graphql::Result<PaginatedLiveSpecsRefs> {
        let app = ctx.data::<Arc<App>>()?;
        let claims = ctx.data::<ControlClaims>()?;

        let snapshot = app.snapshot().read().unwrap();
        // Verify user authorization for the entire prefix being requested. This is
        // technically overkill, and maybe we'll someday want to instead allow users
        // to request to list, for example, `a/` when all they have is a grant to
        // `a/nested/`. But this seems like the easy way to do things for now.
        let authorization = tables::UserGrant::get_user_capability(
            snapshot.role_grants.as_slice(),
            &snapshot.user_grants,
            claims.sub,
            &by.prefix,
        );
        let Some(user_capability) = authorization else {
            return Err(async_graphql::Error::new(format!(
                "user is not authorized to access prefix: '{}'",
                by.prefix
            )));
        };

        let limit = if let Some(f) = first {
            if f < 1 {
                return Err(async_graphql::Error::new(format!(
                    "invalid limit, must be greater than 0: '{}'",
                    f
                )));
            }
            f as usize
        } else {
            100
        };

        let edges: Vec<connection::Edge<String, LiveSpecRef, connection::EmptyFields>> =
            match by.catalog_type {
                models::CatalogType::Collection => snapshot
                    .list_collections(&by.prefix, after.as_deref())
                    .take(limit)
                    .map(|collection| {
                        new_ref_edge(collection.collection_name.as_str(), Some(user_capability))
                    })
                    .collect(),
                task_type => snapshot
                    .list_tasks(&by.prefix, after.as_deref())
                    .filter(|task| task.spec_type == task_type)
                    .take(limit)
                    .map(|task| new_ref_edge(task.task_name.as_str(), Some(user_capability)))
                    .collect(),
            };
        tracing::warn!(%limit, ?by, count = %edges.len(), "list live specs");

        let mut conn = PaginatedLiveSpecsRefs::new(false, edges.len() == limit);
        conn.edges = edges;
        async_graphql::Result::<PaginatedLiveSpecsRefs>::Ok(conn)
    }
}

fn new_ref_edge(
    catalog_name: &str,
    user_capability: Option<models::Capability>,
) -> connection::Edge<String, LiveSpecRef, connection::EmptyFields> {
    connection::Edge::new(
        catalog_name.to_string(),
        LiveSpecRef {
            catalog_name: models::Name::new(catalog_name),
            user_capability,
        },
    )
}
