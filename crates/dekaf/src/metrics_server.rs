use super::App;
use std::sync::Arc;

pub fn build_router(app: Arc<App>) -> axum::Router<()> {
    use axum::routing::get;

    let schema_router = axum::Router::new()
        .route("/metrics", get(prometheus_metrics))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(app);

    schema_router
}

fn record_jemalloc_stats() {
    let stats = allocator::current_mem_stats();
    metrics::gauge!("dekaf_mem_allocated").set(stats.allocated as f64);
    metrics::gauge!("dekaf_mem_mapped").set(stats.mapped as f64);
    metrics::gauge!("dekaf_mem_metadata").set(stats.metadata as f64);
    metrics::gauge!("dekaf_mem_resident").set(stats.resident as f64);
    metrics::gauge!("dekaf_mem_retained").set(stats.retained as f64);
    metrics::gauge!("dekaf_mem_active").set(stats.active as f64);
}

#[tracing::instrument(skip_all)]
async fn prometheus_metrics() -> (axum::http::StatusCode, String) {
    record_jemalloc_stats();

    match prometheus::TextEncoder::new().encode_to_string(&prometheus::default_registry().gather())
    {
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Ok(result) => (axum::http::StatusCode::OK, result),
    }
}
