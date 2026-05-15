//! Prometheus `/metrics` exporter, folded into the [`crate::admin`] router so a
//! service exposes its scrape endpoint on the same loopback admin port.
//!
//! [`install_recorder`] sets the global [`metrics`](https://docs.rs/metrics)
//! recorder to a [`PrometheusRecorder`](metrics_exporter_prometheus::PrometheusRecorder)
//! (so any crate emitting via the `metrics` facade is captured), spawns a
//! background tick that calls [`PrometheusHandle::run_upkeep`] to bound
//! histogram memory growth, and returns an `axum::Router` exposing
//! `GET /metrics`. The install is idempotent — repeated calls reuse the same
//! handle and don't re-spawn the upkeep tick.
//!
//! ## Two distinct kinds of upkeep
//!
//! - **Histogram draining** (`PrometheusHandle::run_upkeep`) is *not* driven by
//!   scrapes; without a periodic tick, raw histogram samples accumulate
//!   indefinitely. The convenience constructors `PrometheusBuilder::install` /
//!   `::build` spawn this for you, but `install_recorder` (which we use,
//!   because we own the HTTP surface) does not — see the **Upkeep and
//!   maintenance** section of the upstream crate docs. This module spawns the
//!   tick.
//!
//! - **Idle-metric pruning** (`idle_timeout`, configured here at 10 minutes) is
//!   driven by [`PrometheusHandle::render`] itself, which calls the recency
//!   check per metric on every scrape. No background tick is required for it;
//!   the trade-off is that idle metrics are only actually freed when a scrape
//!   arrives. With the typical 15-30s Prometheus scrape interval that's fine.

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_util::MetricKindMask;
use std::sync::OnceLock;
use std::time::Duration;

/// Idle metrics older than this are dropped from the registry on the next
/// scrape — picked to be longer than any reasonable Prometheus scrape interval
/// so we never drop a metric an active scraper is still tracking.
const IDLE_TIMEOUT: Duration = Duration::from_secs(600);

/// How often to call [`PrometheusHandle::run_upkeep`] to drain accumulated
/// histogram samples. Matches the upstream `PrometheusBuilder` default.
const UPKEEP_INTERVAL: Duration = Duration::from_secs(5);

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Install the process-wide Prometheus recorder (idempotent), spawn the
/// histogram-upkeep tick, and return an `axum::Router` exposing `GET /metrics`.
///
/// Must be called from within a Tokio runtime — the upkeep tick is
/// `tokio::spawn`ed on first call. Panics if some other crate has already
/// installed a global `metrics` recorder; service-kit owns this slot.
pub fn install_recorder() -> axum::Router<()> {
    let handle = HANDLE.get_or_init(|| {
        let handle = PrometheusBuilder::new()
            .with_recommended_naming(true)
            .idle_timeout(MetricKindMask::ALL, Some(IDLE_TIMEOUT))
            .install_recorder()
            .expect("installing prometheus recorder");

        let upkeep_handle = handle.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(UPKEEP_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                upkeep_handle.run_upkeep();
            }
        });

        handle
    });

    axum::Router::new()
        .route("/metrics", axum::routing::get(render))
        .with_state(handle.clone())
}

async fn render(
    axum::extract::State(handle): axum::extract::State<PrometheusHandle>,
) -> (axum::http::StatusCode, String) {
    (axum::http::StatusCode::OK, handle.render())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn metrics_route_renders_recorded_counter() {
        // Install before emitting — `metrics::counter!` discards into a no-op
        // recorder until a global one is set. The same recorder lives for the
        // whole test process (OnceLock), so a unique counter name avoids
        // collisions with any future test in this crate.
        let router = install_recorder();
        // `with_recommended_naming(true)` suffixes counters with `_total`, so
        // assert on the rendered name below.
        ::metrics::counter!("service_kit_metrics_test_hits").increment(3);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(
            body.contains("service_kit_metrics_test_hits_total 3"),
            "expected counter in scrape output, got:\n{body}",
        );
    }
}
