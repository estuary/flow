use std::sync::Arc;

use crate::api::{datetime_schema, optional_datetime_schema, ApiError, App, ControlClaims};
use crate::controllers;
use axum::extract::{Path, State};
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use models::Id;
use sqlx::types;

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct StatusResponse {
    /// The name of the live spec
    catalog_name: String,
    /// The id of the live spec
    live_spec_id: Id,
    /// The type of the live spec
    spec_type: Option<models::CatalogType>,
    /// Whether the shards are disabled. Only pertinent to tasks. Omitted if false.
    #[serde(skip_serializing_if = "is_false")]
    disable: bool,
    /// The id of the last successful publication that modified the spec.
    last_pub_id: Id,
    /// The id of the last successful publication of the spec, regardless of
    /// whether the spec was modified. This value can be compared against the
    /// value of `/controller_status/activations/last_activated` in order to
    /// determine whether the most recent build has been activated in the data
    /// plane.
    last_build_id: Id,
    /// Time at which the controller is next scheduled to run. Or null if there
    /// is no run scheduled.
    #[schemars(schema_with = "optional_datetime_schema")]
    controller_next_run: Option<DateTime<Utc>>,
    /// Time of the last publication that affected the live spec.
    #[schemars(schema_with = "datetime_schema")]
    live_spec_updated_at: DateTime<Utc>,
    /// Time of the last controller run for this spec.
    #[schemars(schema_with = "datetime_schema")]
    controller_updated_at: DateTime<Utc>,
    /// The controller status json.
    #[schemars(schema_with = "controller_status_schema")]
    controller_status: sqlx::types::Json<controllers::Status>,
    /// Error from the most recent controller run, or `null` if the run was
    /// successful.
    controller_error: Option<String>,
    /// The number of consecutive failures of the controller. Resets to 0 after
    /// any successful run.
    controller_failures: i32,
}

fn controller_status_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    <controllers::Status as schemars::JsonSchema>::json_schema(gen)
}

fn is_false(b: &bool) -> bool {
    !*b
}

#[axum::debug_handler]
pub async fn handle_get_status(
    state: State<Arc<App>>,
    Extension(claims): Extension<ControlClaims>,
    Path(catalog_name): Path<String>,
) -> Result<Json<StatusResponse>, ApiError> {
    if !state
        .0
        .is_user_authorized(&claims, &catalog_name, models::Capability::Read)
        .await?
    {
        tracing::warn!(?claims, %catalog_name, "user is unauthorized");
        return Err(ApiError::not_found(&catalog_name));
    }
    let pool = state.0.pg_pool.clone();
    let status = fetch_status(&pool, &catalog_name).await?;
    Ok(Json(status))
}

async fn fetch_status(pool: &sqlx::PgPool, catalog_name: &str) -> Result<StatusResponse, ApiError> {
    let resp = sqlx::query_as!(StatusResponse, r#"select
        $1 as "catalog_name!: String",
        ls.id as "live_spec_id: Id",
        ls.spec_type as "spec_type: models::CatalogType",
        coalesce(ls.spec->'shards'->>'disable', ls.spec->'derive'->'shards'->>'disable', 'false') = 'true' as "disable!: bool",
        ls.last_pub_id as "last_pub_id: Id",
        ls.last_build_id as "last_build_id: Id",
        ls.controller_next_run,
        ls.updated_at as "live_spec_updated_at: DateTime<Utc>",
        cj.updated_at as "controller_updated_at: DateTime<Utc>",
        cj.status as "controller_status: types::Json<controllers::Status>",
        cj.error as "controller_error: String",
        cj.failures as "controller_failures: i32"
    from live_specs ls
    join controller_jobs cj on ls.id = cj.live_spec_id
    where ls.catalog_name = $1
        "#,
        catalog_name
    ).fetch_all(pool)
    .await?;

    let Some(status) = resp.into_iter().next() else {
        return Err(ApiError::not_found(catalog_name));
    };
    Ok(status)
}
