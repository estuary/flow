use axum::extract::{Extension, Path};
use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use sqlx::PgPool;

use crate::controllers::json_api::RawJson;
use crate::error::AppError;
use crate::models::connector_images::CreateConnectorImage;
use crate::models::Id;
use crate::repo::connector_images as images_repo;
use crate::services::connectors;
use crate::services::subprocess::Subprocess;

pub mod routes;
mod view;

pub async fn index(Extension(db): Extension<PgPool>) -> Result<impl IntoResponse, AppError> {
    let images = images_repo::fetch_all(&db).await?;

    Ok((StatusCode::OK, view::index(images)))
}

pub async fn create(
    Extension(db): Extension<PgPool>,
    Json(input): Json<CreateConnectorImage>,
) -> Result<impl IntoResponse, AppError> {
    let image = images_repo::insert(&db, input).await?;

    Ok((StatusCode::CREATED, view::create(image)))
}

pub async fn show(
    Extension(db): Extension<PgPool>,
    Path(image_id): Path<Id>,
) -> Result<impl IntoResponse, AppError> {
    let image = images_repo::fetch_one(&db, image_id).await?;
    Ok((StatusCode::OK, view::show(image)))
}

pub async fn spec(
    Extension(db): Extension<PgPool>,
    Path(image_id): Path<Id>,
) -> Result<impl IntoResponse, AppError> {
    let image = images_repo::fetch_one(&db, image_id).await?;

    // TODO: Swap `image.pinned_version()` out with `image.full_name()`?
    let image_output = connectors::spec(&image.pinned_version()).execute().await?;
    let spec: RawJson = serde_json::from_str(&image_output)?;

    Ok((StatusCode::OK, view::spec(image, spec)))
}

pub async fn discovery(
    Extension(db): Extension<PgPool>,
    Path(image_id): Path<Id>,
    Json(input): Json<RawJson>,
) -> Result<impl IntoResponse, AppError> {
    let image = images_repo::fetch_one(&db, image_id).await?;

    // TODO: We should probably allow `flowctl api discover` to take this from
    // stdin so we don't have to write the config to a file. This is unnecessary
    // and tempfile::NamedTempFile does not _guarantee_ cleanup, which is not
    // great considering it may include credentials.
    let tmpfile = tempfile::NamedTempFile::new()?;
    serde_json::to_writer(&tmpfile, &input)?;

    let image_output = connectors::discovery(&image.pinned_version(), tmpfile.path())
        .execute()
        .await?;
    let spec: RawJson = serde_json::from_str(&image_output)?;

    Ok((StatusCode::OK, view::discovery(image, spec)))
}
