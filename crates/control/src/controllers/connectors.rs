use axum::extract::Extension;
use axum::extract::Path;
use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use sqlx::PgPool;

use crate::error::AppError;
use crate::models::connectors::Connector;
use crate::models::connectors::NewConnector;
use crate::models::Id;
use crate::repo::connector_images as images_repo;
use crate::repo::connectors as connectors_repo;

pub mod routes;
mod view;

pub async fn index(Extension(db): Extension<PgPool>) -> Result<impl IntoResponse, AppError> {
    let connectors: Vec<Connector> = connectors_repo::fetch_all(&db).await?;

    Ok((StatusCode::OK, view::index(connectors)))
}

pub async fn create(
    Extension(db): Extension<PgPool>,
    Json(input): Json<NewConnector>,
) -> Result<impl IntoResponse, AppError> {
    let connector = connectors_repo::insert(&db, input).await?;

    Ok((StatusCode::CREATED, view::create(connector)))
}

pub async fn images(
    Extension(db): Extension<PgPool>,
    Path(connector_id): Path<Id>,
) -> Result<impl IntoResponse, AppError> {
    let images = images_repo::fetch_all_for_connector(&db, connector_id).await?;

    Ok((StatusCode::OK, view::images(connector_id, images)))
}
