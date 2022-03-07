use std::future::Future;
use std::net::TcpListener;

use axum::{AddExtensionLayer, Router};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::config::{self, DatabaseSettings};
use crate::context::AppContext;
use crate::middleware::cors;
use crate::routes::routes;
use crate::shutdown;

pub use crate::services::builds_root::{FetchBuilds, PutBuilds};

pub fn run(
    listener: TcpListener,
    ctx: AppContext,
) -> anyhow::Result<impl Future<Output = Result<(), hyper::Error>>> {
    info!("Running in {} mode", config::app_env().as_str());
    info!(
        "Listening on http://{}",
        config::settings().application.address()
    );

    let app = app(ctx.clone());

    let server = axum::Server::from_tcp(listener)?
        .serve(app.into_make_service())
        .with_graceful_shutdown(shutdown::signal());

    Ok(server)
}

pub fn app(ctx: AppContext) -> Router {
    routes()
        .layer(cors::cors_layer())
        .layer(TraceLayer::new_for_http())
        .layer(ConcurrencyLimitLayer::new(64))
        .layer(AddExtensionLayer::new(ctx))
}

pub async fn connect_to_postgres(db_settings: &DatabaseSettings) -> PgPool {
    let pool = PgPoolOptions::new()
        .min_connections(1)
        .connect(&db_settings.url())
        .await
        .expect("Failed to connect to postgres");

    pool
}
