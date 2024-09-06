use super::App;
use std::sync::Arc;

pub fn build_router(app: Arc<App>) -> axum::Router<()> {
    use axum::routing::get;

    let schema_router = axum::Router::new()
        .route("/prometheus", get(prometheus_metrics))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(app);

    schema_router
}

#[tracing::instrument(skip_all)]
async fn prometheus_metrics() -> (axum::http::StatusCode, String) {
    match prometheus::TextEncoder::new().encode_to_string(&prometheus::default_registry().gather())
    {
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Ok(result) => (axum::http::StatusCode::OK, result),
    }
}
