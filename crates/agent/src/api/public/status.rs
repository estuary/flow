use std::sync::Arc;

use crate::api::error::ApiErrorExt;
use crate::api::{ApiError, App, ControlClaims};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
// axum_extra's `Query` is needed here because unlike the one from `axum`, it
// handles multiple query parameters with the same name
use axum_extra::extract::Query;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use models::status::{self, StatusResponse};
use models::Id;

/// Query parameters for the status endpoint
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct StatusQuery {
    /// The catalog name of the live spec to get the status of
    pub name: Vec<String>,
}

#[axum::debug_handler]
pub async fn handle_get_status(
    state: State<Arc<App>>,
    Extension(claims): Extension<ControlClaims>,
    Query(params): Query<StatusQuery>,
) -> Result<Json<Vec<StatusResponse>>, ApiError> {
    if !state
        .0
        .is_user_authorized(&claims, &params.name, models::Capability::Read)
        .await?
    {
        return Err(ApiError::not_found());
    }
    let pool = state.0.pg_pool.clone();
    let status = fetch_status(&pool, &params.name).await?;
    Ok(Json(status))
}

async fn fetch_status(
    pool: &sqlx::PgPool,
    catalog_names: &[String],
) -> Result<Vec<StatusResponse>, ApiError> {
    let resp = sqlx::query_as!(StatusResponse, r#"select
        ls.catalog_name as "catalog_name!: String",
        ls.id as "live_spec_id: Id",
        ls.spec_type as "spec_type: models::CatalogType",
        coalesce(ls.spec->'shards'->>'disable', ls.spec->'derive'->'shards'->>'disable', 'false') = 'true' as "disabled!: bool",
        ls.last_pub_id as "last_pub_id: Id",
        ls.last_build_id as "last_build_id: Id",
        ls.controller_next_run,
        ls.updated_at as "live_spec_updated_at: DateTime<Utc>",
        cj.updated_at as "controller_updated_at: DateTime<Utc>",
        cj.status as "controller_status: status::ControllerStatus",
        cj.error as "controller_error: String",
        cj.failures as "controller_failures: i32"
    from live_specs ls
    join controller_jobs cj on ls.id = cj.live_spec_id
    where ls.catalog_name::text = any($1::text[])
        "#,
        catalog_names as &[String],
    ).fetch_all(pool)
    .await?;

    if resp.len() < catalog_names.len() {
        let actual = resp
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
    Ok(resp)
}
