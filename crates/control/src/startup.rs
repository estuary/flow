use std::future::Future;
use std::net::TcpListener;

use axum::AddExtensionLayer;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tower::limit::ConcurrencyLimitLayer;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use crate::config::DatabaseSettings;
use crate::routes::routes;
use crate::shutdown;

pub fn run(
    listener: TcpListener,
    db: PgPool,
) -> anyhow::Result<impl Future<Output = Result<(), hyper::Error>>> {
    let app = routes().layer(
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

pub async fn connect_to_postgres(db_settings: &DatabaseSettings) -> PgPool {
    let pool = PgPoolOptions::new()
        .min_connections(1)
        .connect(&db_settings.url())
        .await
        .expect("Failed to connect to postgres");

    pool
}
