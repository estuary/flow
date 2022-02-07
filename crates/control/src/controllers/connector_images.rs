use axum::extract::{Extension, Path};
use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use sqlx::PgPool;

use crate::controllers::{Payload, RawJson};
use crate::error::AppError;
use crate::models::connector_images::CreateConnectorImage;
use crate::models::Id;
use crate::repo::connector_images as images_repo;
use crate::services::connectors;
use crate::services::subprocess::Subprocess;

pub async fn index(Extension(db): Extension<PgPool>) -> Result<impl IntoResponse, AppError> {
    let images = images_repo::fetch_all(&db).await?;

    Ok((StatusCode::OK, Json(Payload::Data(images))))
}

pub async fn create(
    Extension(db): Extension<PgPool>,
    Json(input): Json<CreateConnectorImage>,
) -> Result<impl IntoResponse, AppError> {
    let image = images_repo::insert(&db, input).await?;

    Ok((StatusCode::CREATED, Json(Payload::Data(image))))
}

pub async fn spec(
    Extension(db): Extension<PgPool>,
    Path(image_id): Path<Id>,
) -> Result<impl IntoResponse, AppError> {
    let image = images_repo::fetch_one(&db, image_id).await?;

    // TODO: Swap `image.pinned_version()` out with `image.full_name()`?
    let image_output = connectors::spec(&image.pinned_version()).execute().await?;
    let spec: RawJson = serde_json::from_str(&image_output)?;

    Ok((StatusCode::OK, Json(Payload::Data(spec))))
}
