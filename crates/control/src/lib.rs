use std::future::Future;
use std::net::TcpListener;

use axum::{routing::get, Router};
use chrono::Utc;
use tower::{limit::ConcurrencyLimitLayer, ServiceBuilder};
use tower_http::trace::TraceLayer;

pub fn run(
    listener: TcpListener,
) -> anyhow::Result<impl Future<Output = Result<(), hyper::Error>>> {
    tracing_subscriber::fmt::init();

    let app = Router::new()
        .route("/health_check", get(health_check))
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(ConcurrencyLimitLayer::new(64)),
        );

    let server = axum::Server::from_tcp(listener)?.serve(app.into_make_service());

    Ok(server)
}

async fn health_check() -> String {
    format!("{}", Utc::now())
}
