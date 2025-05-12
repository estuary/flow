use std::sync::Arc;

use crate::api::error::ApiErrorExt;
use crate::api::{ApiError, App, ControlClaims};
use axum::http::StatusCode;
use axum::{Extension, Json};
// axum_extra's `Query` is needed here because unlike the one from `axum`, it
// handles multiple query parameters with the same name
use axum_extra::extract::Query;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use models::status::{self, connector::ConnectorStatus, StatusResponse, Summary};
use models::{CatalogType, Id};

/// Query parameters for the status endpoint
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct StatusQuery {
    /// The catalog name of the live spec to get the status of
    pub name: Vec<String>,
    /// Whether to return a smaller response that excludes the `connector_status` and `controller_status` fields.
    #[serde(default)]
    pub short: bool,
}

#[axum::debug_handler]
pub async fn handle_get_status(
    state: axum::extract::State<Arc<App>>,
    Extension(claims): Extension<ControlClaims>,
    Query(StatusQuery { name, short }): Query<StatusQuery>,
) -> Result<Json<Vec<StatusResponse>>, ApiError> {
    let name = state
        .0
        .verify_user_authorization(&claims, name, models::Capability::Read)
        .await?;

    let pool = state.0.pg_pool.clone();
    let status = fetch_status(&pool, &name, short).await?;
    Ok(Json(status))
}

async fn fetch_status(
    pool: &sqlx::PgPool,
    catalog_names: &[String],
    short: bool,
) -> Result<Vec<StatusResponse>, ApiError> {
    let rows = sqlx::query_as!(StatusRow, r#"select
        ls.catalog_name as "catalog_name!: String",
        ls.id as "live_spec_id: Id",
        ls.spec_type as "spec_type: models::CatalogType",
        coalesce(ls.spec->'shards'->>'disable', ls.spec->'derive'->'shards'->>'disable', 'false') = 'true' as "disabled!: bool",
        ls.last_pub_id as "last_pub_id: Id",
        ls.last_build_id as "last_build_id: Id",
        t.wake_at as "controller_next_run: DateTime<Utc>",
        ls.updated_at as "live_spec_updated_at: DateTime<Utc>",
        cs.flow_document as "connector_status?: ConnectorStatus",
        cj.updated_at as "controller_updated_at: DateTime<Utc>",
        cj.status as "controller_status?: status::ControllerStatus",
        cj.error as "controller_error: String",
        cj.failures as "controller_failures: i32"
    from live_specs ls
    join controller_jobs cj on ls.id = cj.live_spec_id
    join internal.tasks t on ls.controller_task_id = t.task_id
    left outer join connector_status cs on ls.catalog_name = cs.catalog_name
    where ls.catalog_name::text = any($1::text[])
        "#,
        catalog_names as &[String],
    ).fetch_all(pool)
    .await?;

    if rows.len() < catalog_names.len() {
        let actual = rows
            .into_iter()
            .map(|r| r.catalog_name)
            .collect::<std::collections::HashSet<String>>();
        let missing = catalog_names
            .iter()
            .filter(|n| !actual.contains(n.as_str()));
        return Err(
            anyhow::anyhow!("no live specs found for names: [{}]", missing.format(", "))
                .with_status(StatusCode::NOT_FOUND),
        );
    }

    let resp = rows.into_iter().map(|r| r.to_response(short)).collect();
    Ok(resp)
}

/// Intermediate struct returned from database query, which can be converted into a response
struct StatusRow {
    catalog_name: String,
    live_spec_id: Id,
    spec_type: Option<CatalogType>,
    disabled: bool,
    last_pub_id: Id,
    last_build_id: Id,
    connector_status: Option<status::connector::ConnectorStatus>,
    controller_next_run: Option<DateTime<Utc>>,
    live_spec_updated_at: DateTime<Utc>,
    controller_updated_at: DateTime<Utc>,
    controller_status: Option<status::ControllerStatus>,
    controller_error: Option<String>,
    controller_failures: i32,
}

impl StatusRow {
    fn to_response(self, summary_only: bool) -> StatusResponse {
        let StatusRow {
            catalog_name,
            live_spec_id,
            spec_type,
            disabled,
            last_pub_id,
            last_build_id,
            mut connector_status,
            controller_next_run,
            live_spec_updated_at,
            controller_updated_at,
            mut controller_status,
            controller_error,
            controller_failures,
        } = self;
        let summary = Summary::of(
            disabled,
            last_build_id,
            controller_error.as_deref(),
            controller_status.as_ref(),
            connector_status.as_ref(),
        );
        if summary_only {
            connector_status.take();
            controller_status.take();
        }
        StatusResponse {
            catalog_name,
            summary,
            live_spec_id,
            spec_type,
            disabled,
            last_pub_id,
            last_build_id,
            connector_status,
            controller_next_run,
            live_spec_updated_at,
            controller_updated_at,
            controller_status,
            controller_error,
            controller_failures,
        }
    }
}
