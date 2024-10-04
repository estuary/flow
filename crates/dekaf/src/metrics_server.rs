use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

pub fn build_router() -> axum::Router<()> {
    use axum::routing::get;

    let prom = PrometheusBuilder::new()
        .set_buckets(
            &prometheus::exponential_buckets(0.00001, 2.5, 15)
                .expect("calculating histogram buckets"),
        )
        .expect("calculating histogram buckets")
        .install_recorder()
        .expect("failed to install prometheus recorder");

    let schema_router = axum::Router::new()
        .route("/metrics", get(prometheus_metrics))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(prom);

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
async fn prometheus_metrics(
    axum::extract::State(prom_handle): axum::extract::State<PrometheusHandle>,
) -> (axum::http::StatusCode, String) {
    record_jemalloc_stats();

    (axum::http::StatusCode::OK, prom_handle.render())
}
