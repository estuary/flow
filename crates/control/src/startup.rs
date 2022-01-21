use std::future::Future;
use std::net::TcpListener;

use axum::routing::get;
use axum::Router;
use tower::limit::ConcurrencyLimitLayer;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use crate::routes;
use crate::shutdown;

pub fn run(
    listener: TcpListener,
) -> anyhow::Result<impl Future<Output = Result<(), hyper::Error>>> {
    let app = Router::new()
        .route("/health_check", get(routes::health_check))
        .route("/connectors", get(routes::list_connectors))
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(ConcurrencyLimitLayer::new(64)),
        );

    let server = axum::Server::from_tcp(listener)?
        .serve(app.into_make_service())
        .with_graceful_shutdown(shutdown::signal());

    Ok(server)
}
