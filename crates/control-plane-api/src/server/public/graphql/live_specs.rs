use async_graphql::{
    types::connection::{self, Connection},
    ComplexObject, Context, SimpleObject,
};
use chrono::{DateTime, Utc};
use models::Id;
use std::sync::Arc;

use crate::server::{public::graphql::alerts, App, ControlClaims};

#[derive(Debug, Clone, SimpleObject)]
#[graphql(complex)]
pub struct LiveSpec {
    pub live_spec_id: Id,
    pub catalog_name: String,
    pub spec_type: models::CatalogType,
    pub model: async_graphql::Json<async_graphql::Value>,
    pub last_build_id: Id,
    pub last_pub_id: Id,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub built_spec: async_graphql::Json<async_graphql::Value>,
    pub is_disabled: bool,
}

#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct ByPrefix {
    pub prefix: String,
    pub catalog_type: Option<models::CatalogType>,
}

#[derive(async_graphql::OneofObject)]
pub enum LiveSpecBy {
    Id(models::Id),
    Name(String),
}

pub type PaginatedLiveSpecs = Connection<
    String,
    LiveSpec,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

#[ComplexObject]
impl LiveSpec {
    /// Returns all alerts that are currently firing for this live spec.
    async fn firing_alerts(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<alerts::Alert>> {
        let loader = ctx.data::<async_graphql::dataloader::DataLoader<alerts::AlertLoader>>()?;
        let alerts = loader
            .load_one(alerts::FiringAlerts {
                catalog_name: self.catalog_name.clone(),
            })
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
        alerts::live_spec_alert_history(ctx, &self.catalog_name, before, last).await
    }

    // async fn controller_status(
    //     &self,
    //     ctx: &Context<'_>,
    // ) -> async_graphql::Result<Option<models::status::capture::CaptureStatus>> {
    //     todo!()
    // }
}

#[derive(Debug, Default)]
pub struct LiveSpecsQuery;

#[async_graphql::Object]
impl LiveSpecsQuery {
    pub async fn live_specs(
        &self,
        ctx: &Context<'_>,
        by: ByPrefix,
        after: Option<String>,
        first: Option<i32>,
    ) -> async_graphql::Result<PaginatedLiveSpecs> {
        list_live_specs_by_prefix(ctx, by, after, first).await
    }
}

pub async fn list_live_specs_by_prefix(
    ctx: &Context<'_>,
    by: ByPrefix,
    after: Option<String>,
    first: Option<i32>,
) -> async_graphql::Result<PaginatedLiveSpecs> {
    let app = ctx.data::<Arc<App>>()?;
    let claims = ctx.data::<ControlClaims>()?;

    // Verify user authorization for the catalog names
    let _ = app
        .verify_user_authorization(claims, vec![by.prefix.clone()], models::Capability::Read)
        .await
        .map_err(|e| async_graphql::Error::new(format!("Authorization failed: {}", e)))?;

    let node_field = ctx.look_ahead().field("edges").field("node");
    let model_selected = node_field.field("model").exists();
    let built_selected = node_field.field("builtSpec").exists();
    tracing::info!(%model_selected, %built_selected, ?after, ?first, "live specs query");

    let out = connection::query(after, None, first, None, |after, _before, first, _last| async move {
            let ByPrefix { prefix, catalog_type } = by;
            let limit = first.unwrap_or(50) as i64;

            let name_filter = after.as_deref().unwrap_or("");

            let rows = sqlx::query!(
                r#"select
                        ls.catalog_name,
                        ls.id as "live_spec_id: models::Id",
                        ls.spec_type as "spec_type!: models::CatalogType",
                        case when $3 then ls.spec::text else null end as "model: crate::TextJson<async_graphql::Value>",
                        ls.last_build_id as "last_build_id: models::Id",
                        ls.last_pub_id as "last_pub_id: models::Id",
                        ls.created_at,
                        ls.updated_at,
                        case when $4 then ls.built_spec::text else null end as "built_spec: crate::TextJson<async_graphql::Value>",
                        coalesce(ls.spec->'shards'->>'disable', ls.spec->'derive'->'shards'->>'disable', 'false')::boolean as "is_disabled!: bool"
                    from live_specs ls
                    where starts_with(ls.catalog_name::text, $1)
                    and ($2::catalog_spec_type is null or ls.spec_type = $2::catalog_spec_type)
                    and ls.catalog_name > $5
                    order by ls.catalog_name asc
                    limit $6
                "#,
                prefix.as_str(),
                catalog_type as Option<models::CatalogType>,
                model_selected,
                built_selected,
                name_filter,
                limit,
            )
            .fetch_all(&app.pg_pool)
            .await
            .map_err(|e| async_graphql::Error::new(format!("Failed to fetch live specs: {}", e)))?;


            let edges = rows
                .into_iter()
                .map(|row| {
                    let cursor = row.catalog_name.clone();
                    let live = LiveSpec {
                                    catalog_name: row.catalog_name,
                                    live_spec_id: row.live_spec_id,
                                    spec_type: row.spec_type,
                                    model: async_graphql::Json(row.model.map(|j| j.0).unwrap_or_default()),
                                    last_build_id: row.last_build_id,
                                    last_pub_id: row.last_pub_id,
                                    created_at: row.created_at,
                                    updated_at: row.updated_at,
                                    built_spec: async_graphql::Json(row.built_spec.map(|j| j.0).unwrap_or_default()),
                                    is_disabled: row.is_disabled,
                                };
                    connection::Edge::new(cursor, live)
                })
                .collect::<Vec<connection::Edge<String, LiveSpec, connection::EmptyFields>>>();
            let mut res = Connection::new(false, edges.len() as i64 == limit);
            res.edges = edges;
            async_graphql::Result::<PaginatedLiveSpecs>::Ok(res)
        }).await?;

    Ok(out)
}
