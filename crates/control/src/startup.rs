use std::future::Future;
use std::net::TcpListener;

use axum::routing::get;
use axum::AddExtensionLayer;
use axum::Router;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tower::limit::ConcurrencyLimitLayer;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use crate::config;
use crate::controllers;
use crate::shutdown;

pub fn run(
    listener: TcpListener,
    db: PgPool,
) -> anyhow::Result<impl Future<Output = Result<(), hyper::Error>>> {
    let app = Router::new()
        .route("/health_check", get(controllers::health_check::show))
        .route("/connectors", get(controllers::connectors::index))
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(ConcurrencyLimitLayer::new(64))
                .layer(AddExtensionLayer::new(db)),
        );

    let server = axum::Server::from_tcp(listener)?
        .serve(app.into_make_service())
        .with_graceful_shutdown(shutdown::signal());

    Ok(server)
}

pub async fn connect_to_postgres() -> PgPool {
    let pool = PgPoolOptions::new()
        .min_connections(1)
        .connect(&config::settings().database.url())
        .await
        .expect("Failed to connect to postgres");

    pool
}
