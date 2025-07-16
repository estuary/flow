use async_graphql::{
    Context as GraphQLContext, EmptyMutation, EmptySubscription, Object, Schema, SimpleObject,
};
use axum::Extension;
use chrono::{DateTime, Utc};
use models::{Alert, AlertType, CatalogType, Id};
use serde_json::value::RawValue;
use std::sync::Arc;

use crate::api::public::status::fetch_status;
use crate::api::{App, ControlClaims};

pub struct LiveSpec<T: models::ModelDef, B> {
    pub id: Id,
    pub spec_type: models::CatalogType,
    pub model: Option<T>,
    pub last_build_id: Id,
    pub last_pub_id: Id,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub built_spec: Option<B>,
}

impl LiveSpec<models::CaptureDef, proto_flow::capture::CaptureDef> {}

pub type GraphQLSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Get live specs by catalog name
    async fn live_specs(
        &self,
        ctx: &GraphQLContext<'_>,
        #[graphql(desc = "Catalog name to filter by")] catalog_name: String,
    ) -> async_graphql::Result<Vec<LiveSpec>> {
        let app = ctx.data::<Arc<App>>()?;
        let claims = ctx.data::<ControlClaims>()?;

        // Verify user authorization for the catalog name
        let authorized_names = app
            .verify_user_authorization(claims, vec![catalog_name.clone()], models::Capability::Read)
            .await
            .map_err(|e| async_graphql::Error::new(format!("Authorization failed: {}", e)))?;

        if authorized_names.is_empty() {
            return Ok(vec![]);
        }

        // Fetch live specs using existing function
        let live_catalog = crate::live_specs::get_live_specs(
            claims.sub,
            &authorized_names,
            Some(models::Capability::Read),
            &app.pg_pool,
        )
        .await
        .map_err(|e| async_graphql::Error::new(format!("Failed to fetch live specs: {}", e)))?;

        let mut results = Vec::new();

        Ok(results)
    }

    /// Get statuses by live_spec_id or catalog_name
    async fn statuses(
        &self,
        ctx: &GraphQLContext<'_>,
        #[graphql(desc = "Live spec ID to filter by")] live_spec_id: Option<String>,
        #[graphql(desc = "Catalog name to filter by")] catalog_name: Option<String>,
    ) -> async_graphql::Result<Vec<Status>> {
        let app = ctx.data::<Arc<App>>()?;
        let claims = ctx.data::<ControlClaims>()?;

        let names = if let Some(name) = catalog_name {
            vec![name]
        } else if let Some(_id) = live_spec_id {
            // Query by ID requires a different approach
            let rows = sqlx::query!(
                r#"
                SELECT catalog_name as "catalog_name!: String"
                FROM live_specs
                WHERE id = $1
                "#,
                _id.parse::<models::Id>()
                    .map_err(|e| async_graphql::Error::new(format!(
                        "Invalid live_spec_id format: {}",
                        e
                    )))?
            )
            .fetch_all(&app.pg_pool)
            .await
            .map_err(|e| {
                async_graphql::Error::new(format!("Failed to query live_spec_id: {}", e))
            })?;

            if rows.is_empty() {
                return Ok(vec![]);
            }

            rows.into_iter().map(|row| row.catalog_name).collect()
        } else {
            return Err(async_graphql::Error::new(
                "Either live_spec_id or catalog_name must be provided",
            ));
        };

        // Verify user authorization
        let authorized_names = app
            .verify_user_authorization(claims, names, models::Capability::Read)
            .await
            .map_err(|e| async_graphql::Error::new(format!("Authorization failed: {}", e)))?;

        if authorized_names.is_empty() {
            return Ok(vec![]);
        }

        // Use existing fetch_status function
        let status_responses = fetch_status(&app.pg_pool, &authorized_names, false)
            .await
            .map_err(|e| async_graphql::Error::new(format!("Failed to fetch statuses: {}", e)))?;

        let results = status_responses
            .into_iter()
            .map(|status| Status {
                catalog_name: status.catalog_name,
                live_spec_id: status.live_spec_id.to_string(),
                spec_type: status.spec_type,
                disabled: status.disabled,
                last_pub_id: status.last_pub_id.to_string(),
                last_build_id: status.last_build_id.to_string(),
                live_spec_updated_at: status.live_spec_updated_at,
                controller_updated_at: status.controller_updated_at,
                controller_error: status.controller_error,
                controller_failures: status.controller_failures,
            })
            .collect();

        Ok(results)
    }

    /// Get alerts from alert_history by catalog_name
    async fn alerts(
        &self,
        ctx: &GraphQLContext<'_>,
        #[graphql(desc = "Catalog name to filter by")] catalog_name: String,
        #[graphql(desc = "Maximum number of alerts to return", default = 100)] limit: i32,
    ) -> async_graphql::Result<Vec<Alert>> {
        let app = ctx.data::<Arc<App>>()?;
        let claims = ctx.data::<ControlClaims>()?;

        // Verify user authorization
        let authorized_names = app
            .verify_user_authorization(claims, vec![catalog_name.clone()], models::Capability::Read)
            .await
            .map_err(|e| async_graphql::Error::new(format!("Authorization failed: {}", e)))?;

        if authorized_names.is_empty() {
            return Ok(vec![]);
        }

        // Query alert_history table
        let rows = sqlx::query!(
            r#"
            SELECT
                alert_type as "alert_type!: AlertType",
                catalog_name as "catalog_name!: String",
                fired_at,
                resolved_at,
                arguments,
                resolved_arguments
            FROM alert_history
            WHERE catalog_name = $1
            ORDER BY fired_at DESC
            LIMIT $2
            "#,
            catalog_name,
            limit as i64,
        )
        .fetch_all(&app.pg_pool)
        .await
        .map_err(|e| async_graphql::Error::new(format!("Failed to fetch alerts: {}", e)))?;

        let results = rows
            .into_iter()
            .map(|row| Alert {
                alert_type: row.alert_type,
                catalog_name: row.catalog_name,
                fired_at: row.fired_at,
                resolved_at: row.resolved_at,
                arguments: row.arguments.to_string(),
                resolved_arguments: row.resolved_arguments.map(|v| v.to_string()),
            })
            .collect();

        Ok(results)
    }
}

#[derive(SimpleObject)]
struct LiveSpec {
    catalog_name: String,
    spec_type: CatalogType,
    spec: Option<String>,
    built_spec: Option<String>,
    last_pub_id: Id,
    last_build_id: Id,
}

#[derive(SimpleObject)]
struct Status {
    catalog_name: String,
    live_spec_id: String,
    spec_type: Option<CatalogType>,
    disabled: bool,
    last_pub_id: String,
    last_build_id: String,
    live_spec_updated_at: DateTime<Utc>,
    controller_updated_at: DateTime<Utc>,
    controller_error: Option<String>,
    controller_failures: i32,
}

pub fn create_schema() -> GraphQLSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription).finish()
}

pub async fn graphql_handler(
    schema: Extension<GraphQLSchema>,
    claims: Extension<ControlClaims>,
    app_state: axum::extract::State<Arc<App>>,
    req: axum::extract::Json<async_graphql::Request>,
) -> axum::Json<async_graphql::Response> {
    let request = req.0.data(app_state.0).data(claims.0);

    let response = schema.execute(request).await;
    axum::Json(response)
}

pub async fn graphql_playground() -> impl axum::response::IntoResponse {
    axum::response::Html(async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/api/v1/graphql"),
    ))
}
