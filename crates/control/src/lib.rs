use std::fmt::Debug;
use std::future::Future;
use std::net::TcpListener;

use axum::Json;
use axum::{routing::get, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tower::{limit::ConcurrencyLimitLayer, ServiceBuilder};
use tower_http::trace::TraceLayer;

pub mod config;
mod shutdown;

pub fn run(
    listener: TcpListener,
) -> anyhow::Result<impl Future<Output = Result<(), hyper::Error>>> {
    let app = Router::new()
        .route("/health_check", get(health_check))
        .route("/connectors", get(list_connectors))
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

async fn health_check() -> String {
    format!("{}", Utc::now())
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ConnectorType {
    Source,
    Materialization,
}

#[derive(Debug, Deserialize, Serialize)]
struct Connector {
    description: String,
    image: String,
    name: String,
    owner: String,
    r#type: ConnectorType,
    tags: Vec<String>,
}

async fn list_connectors() -> Json<Vec<Connector>> {
    let connectors = vec![Connector {
        description: "A flood of greetings.".to_owned(),
        image: "ghcr.io/estuary/source-hello-world".to_owned(),
        name: "source-hello-world".to_owned(),
        owner: "Estuary".to_owned(),
        r#type: ConnectorType::Source,
        tags: vec!["dev".to_owned()],
    }];

    Json(connectors)
}
