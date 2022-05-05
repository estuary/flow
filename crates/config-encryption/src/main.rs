mod encrypt;

use axum::{
    extract::MatchedPath,
    http::Request,
    middleware::{self, Next},
    response::IntoResponse,
    routing::get,
    Router,
};
use clap::Parser;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
#[allow(unused)]
use std::future::ready;
use std::time::Instant;
use tower_http::cors;
use tower_http::trace::TraceLayer;

// The openapi spec is hand-written instead of being generated. I tried a few different ways of
// generating the spec from the source code, and none of them seemed worth the hassle. They
// required so many derive attributes that it seemed better to just write it the old fashioned way.
// Axum has plans for built-in support for openapi spec generation, and that seems like it'll be
// the best way to go, once it's released.
const OPENAPI_SPEC: &str = include_str!("openapi-spec.json");

/// Serves an encrypt-config endpoint that ...encrypts endpoint configs ;)
#[derive(Debug, clap::Parser)]
pub struct Args {
    #[clap(flatten)]
    pub logging: flow_cli_common::LogArgs,
    #[clap(long, env, default_value = "8765")]
    pub port: u16,
    #[clap(long, env, default_value = "0.0.0.0")]
    pub bind_addr: std::net::IpAddr,
    #[clap(flatten)]
    pub sops: SopsArgs,
}

/// Arguments to be passed to sops whenever documents are encrypted.
#[derive(Debug, clap::Args, Clone)]
pub struct SopsArgs {
    /// The fully qualified name of the GCP KMS key to use for encryption, e.g. projects/<your-project>/locations/<your-region>/keyRings/<your-keyring>/cryptoKeys/<your-key-name>
    #[clap(long, env)]
    pub gcp_kms: String,

    /// The suffix used to identify fields within configuration objects that should be encrypted.
    #[clap(long, default_value = "_sops")]
    pub encrypted_suffix: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    flow_cli_common::init_logging(&args.logging);
    tracing::debug!(?args, "successfully parsed arguments");

    let openapi_spec_json = serde_json::from_str::<Box<serde_json::value::RawValue>>(OPENAPI_SPEC)?;

    // We're currently runnning this in CloudRun, which doesn't support scraping this endpoint
    // anyway. It's commented out so that we don't need to hide the service behide a load balancer
    // in order to prevent outside access to `/metrics`.
    //let recorder_handle = setup_metrics_recorder();
    let app = Router::new()
        .route("/v1", get(|| async { axum::Json(openapi_spec_json) }))
        //.route("/metrics", get(move || ready(recorder_handle.render())))
        .merge(encrypt::handler::router(args.sops.clone()))
        .layer(
            cors::CorsLayer::new()
                .allow_origin(cors::Any)
                .allow_headers([axum::http::header::CONTENT_TYPE, axum::http::header::ACCEPT]),
        )
        .layer(TraceLayer::new_for_http())
        .route_layer(middleware::from_fn(track_metrics));

    let bind_addr = std::net::SocketAddr::new(args.bind_addr, args.port);
    axum::Server::bind(&bind_addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
    Ok(())
}

#[allow(dead_code)]
fn setup_metrics_recorder() -> PrometheusHandle {
    const EXPONENTIAL_SECONDS: &[f64] = &[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

    PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full("http_requests_duration_seconds".to_string()),
            EXPONENTIAL_SECONDS,
        )
        .unwrap()
        .install_recorder()
        .unwrap()
}

async fn track_metrics<B>(req: Request<B>, next: Next<B>) -> impl IntoResponse {
    let start = Instant::now();
    let path = if let Some(matched_path) = req.extensions().get::<MatchedPath>() {
        matched_path.as_str().to_owned()
    } else {
        req.uri().path().to_owned()
    };

    let response = next.run(req).await;

    let latency = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();

    let labels = [("path", path), ("status", status)];

    metrics::increment_counter!("http_requests_total", &labels);
    metrics::histogram!("http_requests_duration_seconds", latency, &labels);

    response
}
