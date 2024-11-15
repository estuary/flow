use std::sync::Arc;

use crate::api::Request;
use anyhow::Context;
use axum::{extract::Path, http::request::Parts};
use validator::Validate;

use super::App;

#[derive(Debug, serde::Deserialize, schemars::JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct DiscoverReq {
    pub update_only: bool,
}

#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Changed {
    pub resource_path: crate::discovers::ResourcePath,
    pub target: models::Collection,
    pub disable: bool,
}
fn changed(resource_path: crate::discovers::ResourcePath, c: crate::discovers::Changed) -> Changed {
    Changed {
        resource_path,
        target: c.target,
        disable: c.disable,
    }
}

#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscoverResp {
    pub capture_name: models::Capture,
    pub errors: Vec<crate::draft::Error>,
    pub draft: models::Catalog,
    pub added: Vec<Changed>,
    pub modified: Vec<Changed>,
    pub removed: Vec<Changed>,
}

pub async fn test_discover(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    Path(capture_name): Path<String>,
    Request(req): Request<DiscoverReq>,
) -> axum::response::Response {
    super::wrap(async move { do_test_discover(&app, capture_name, req).await }).await
}

async fn do_test_discover(
    app: &App,
    capture_name: String,
    req: DiscoverReq,
) -> anyhow::Result<DiscoverResp> {
    let DiscoverReq { update_only } = req;

    let capture_name = models::Capture::new(capture_name);
    let pool = app.pg_pool.clone();
    let names = &[capture_name.to_string()];
    let live = crate::live_specs::get_live_specs(app.system_user_id, names, None, &pool)
        .await
        .context("fetching live spec")?;
    let tables::LiveCapture {
        capture,
        control_id,
        data_plane_id,
        last_pub_id,
        model,
        ..
    } = live
        .captures
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("no such capture: '{capture_name}'"))?;

    let mut draft = tables::DraftCatalog::default();
    draft.captures.insert(tables::DraftCapture {
        capture,
        scope: tables::synthetic_scope(models::CatalogType::Capture, &capture_name),
        model: Some(model),
        expect_pub_id: Some(last_pub_id),
        is_touch: true,
    });

    let logs_token = uuid::Uuid::new_v4();

    let user_id = app.system_user_id;
    let data_planes = agent_sql::data_plane::fetch_data_planes(
        &app.pg_pool,
        vec![data_plane_id],
        "not-a-real-data-plane",
        user_id,
    )
    .await?;

    if data_planes.len() != 1 {
        anyhow::bail!("expected 1 data_plane, got: {}", data_planes.len());
    }
    let data_plane = data_planes.into_iter().next().unwrap();
    tracing::info!(%control_id, %capture_name, data_plane_name = %data_plane.data_plane_name, "fidna discover");
    let disco = crate::discovers::Discover {
        capture_name,
        data_plane,
        logs_token,
        user_id,
        update_only,
        draft,
    };

    let crate::discovers::DiscoverOutput {
        capture_name,
        draft,
        added,
        modified,
        removed,
    } = app.discover_handler.discover(&pool, disco).await?;

    let errors = draft
        .errors
        .iter()
        .map(crate::draft::Error::from_tables_error)
        .collect();
    let resp_draft = sources::merge::into_catalog(draft);
    let added = added.into_iter().map(|(rp, c)| changed(rp, c)).collect();
    let modified = modified.into_iter().map(|(rp, c)| changed(rp, c)).collect();
    let removed = removed.into_iter().map(|(rp, c)| changed(rp, c)).collect();

    Ok(DiscoverResp {
        capture_name,
        errors,
        draft: resp_draft,
        added,
        modified,
        removed,
    })
}
