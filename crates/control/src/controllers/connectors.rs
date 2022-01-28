use axum::extract::Extension;
use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use sqlx::PgPool;

use crate::controllers::Payload;
use crate::repo::connectors as connectors_repo;

pub async fn index(Extension(db): Extension<PgPool>) -> impl IntoResponse {
    match connectors_repo::fetch_all(&db).await {
        Ok(connectors) => (StatusCode::OK, Json(Payload::Data(connectors))),
        Err(error) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(Payload::Error(error.to_string())),
        ),
    }
}
