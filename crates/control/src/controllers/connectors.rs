use axum::extract::Extension;
use axum::extract::Path;
use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use validator::Validate;

use crate::context::AppContext;
use crate::error::AppError;
use crate::models::connectors::Connector;
use crate::models::connectors::NewConnector;
use crate::models::id::Id;
use crate::repo::connector_images as images_repo;
use crate::repo::connectors as connectors_repo;

pub mod routes;
mod view;

pub async fn index(Extension(ctx): Extension<AppContext>) -> Result<impl IntoResponse, AppError> {
    let connectors: Vec<Connector> = connectors_repo::fetch_all(ctx.db()).await?;

    Ok((StatusCode::OK, view::index(connectors)))
}

pub async fn create(
    Extension(ctx): Extension<AppContext>,
    Json(input): Json<NewConnector>,
) -> Result<impl IntoResponse, AppError> {
    input.validate()?;

    let connector = connectors_repo::insert(ctx.db(), input).await?;

    Ok((StatusCode::CREATED, view::create(connector)))
}

pub async fn images(
    Extension(ctx): Extension<AppContext>,
    Path(connector_id): Path<Id<Connector>>,
) -> Result<impl IntoResponse, AppError> {
    let images = images_repo::fetch_all_for_connector(ctx.db(), connector_id).await?;

    Ok((StatusCode::OK, view::images(connector_id, images)))
}
