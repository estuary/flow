use axum::{http::StatusCode, response::IntoResponse};
use std::sync::Arc;

pub struct App {
    pub pool: sqlx::PgPool,
}

mod authorize;

// Build an axum::Router for the agent API.
pub fn build_router(app: Arc<App>) -> axum::Router<()> {
    use axum::routing::post;

    let schema_router = axum::Router::new()
        .route("/authorize", post(authorize::authorize))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(app);

    schema_router
}

async fn wrap<F, T>(fut: F) -> axum::response::Response
where
    T: serde::Serialize,
    F: std::future::Future<Output = anyhow::Result<T>>,
{
    match fut.await {
        Ok(inner) => (StatusCode::OK, axum::Json::from(inner)).into_response(),
        Err(err) => {
            let err = format!("{err:#?}");
            tracing::warn!(err, "request failed");
            (StatusCode::BAD_REQUEST, err).into_response()
        }
    }
}
