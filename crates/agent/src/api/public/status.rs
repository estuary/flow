use std::collections::BTreeSet;
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
    /// Whether to fetch statuses for connected specs, in addition to those named
    #[serde(default)]
    pub connected: bool,
}

#[axum::debug_handler]
pub(crate) async fn handle_get_status(
    state: axum::extract::State<Arc<App>>,
    Extension(claims): Extension<ControlClaims>,
    Query(StatusQuery {
        name,
        short,
        connected,
    }): Query<StatusQuery>,
) -> Result<Json<Vec<StatusResponse>>, ApiError> {
    // Any requested names must be directly authorized
    let name = state
        .0
        .verify_user_authorization(&claims, name, models::Capability::Read)
        .await?;

    let mut require_names = name.iter().map(|s| s.as_str()).collect::<BTreeSet<_>>();

    let pool = state.0.pg_pool.clone();

    // If we need to return statuses of connected specs, then resolve their
    // names now, and filter out those that the user isn't authorized to before
    // querying for the statuses.
    let status = if connected {
        // Filter out any names that the user cannot read before fetching the statuses
        let unfiltered_names = add_connected_names(&name, &pool).await?;
        let filtered = state.0.filter_results(
            &claims,
            models::Capability::Read,
            unfiltered_names,
            String::as_str,
        );
        fetch_status(&pool, &filtered, short).await?
    } else {
        fetch_status(&pool, &name, short).await?
    };

    // Check whether all of the names that were explicitly requested are present
    // in the response, and return a 404 if any are missing. Additional "connected"
    // specs are allowed to be missing.
    for result in status.iter() {
        require_names.remove(result.catalog_name.as_str());
    }
    if !require_names.is_empty() {
        return Err(anyhow::anyhow!(
            "no live specs found for names: [{}]",
            require_names.iter().format(", ")
        )
        .with_status(StatusCode::NOT_FOUND));
    }
    Ok(Json(status))
}

pub async fn fetch_status(
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

        // Omit the connector status if it's known to be stale.
        let last_activation_ts = controller_status
            .as_ref()
            .and_then(|status| status.activation_status())
            .and_then(|activation| activation.last_activated_at);
        connector_status
            .take_if(|cs| last_activation_ts.is_none_or(|ts| !cs.is_current(last_build_id, ts)));

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

async fn add_connected_names(
    catalog_names: &[String],
    db: &sqlx::PgPool,
) -> sqlx::Result<Vec<String>> {
    let names = sqlx::query_scalar!(
        r#"
        with orig as (
          select id, catalog_name from live_specs where catalog_name = any($1::catalog_name[])
        )
        select lst.catalog_name::text as "name!: String"
        from orig
        join live_spec_flows lsf on orig.id = lsf.source_id
        join live_specs lst on lsf.target_id = lst.id
        union
        select lss.catalog_name::text as "name!: String"
        from orig
        join live_spec_flows lsf on orig.id = lsf.target_id
        join live_specs lss on lsf.source_id = lss.id
        union
        select catalog_name::text as "name!: String"
        from orig
        "#,
        catalog_names as &[String],
    )
    .fetch_all(db)
    .await?;
    Ok(names)
}
