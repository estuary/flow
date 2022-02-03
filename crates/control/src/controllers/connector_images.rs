use axum::extract::Extension;
use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use sqlx::PgPool;

use crate::controllers::Payload;
use crate::error::AppError;
use crate::models::connector_images::CreateConnectorImage;
use crate::repo::connector_images as images_repo;

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
