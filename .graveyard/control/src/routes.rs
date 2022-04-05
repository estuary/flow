use crate::controllers;
use crate::middleware::sessions::validate_authentication_token;
use axum::routing::{get, post};
use axum::Router;

pub fn routes() -> Router {
    Router::new()
        .merge(accounts_routes())
        .merge(builds_routes())
        .merge(connectors_routes())
        .merge(connector_images_routes())
        .layer(axum::middleware::from_fn(validate_authentication_token))
        .merge(health_check_routes())
        .merge(sessions_routes())
}

fn accounts_routes() -> Router {
    Router::new()
        .route(
            "/accounts",
            get(controllers::accounts::index).post(controllers::accounts::create),
        )
        .route("/accounts/:id", get(controllers::accounts::show))
}

fn builds_routes() -> Router {
    Router::new()
        .route(
            "/builds",
            get(controllers::builds::index).post(controllers::builds::create),
        )
        .route("/builds/:id", get(controllers::builds::show))
}

fn connectors_routes() -> Router {
    Router::new()
        .route(
            "/connectors",
            get(controllers::connectors::index).post(controllers::connectors::create),
        )
        .route(
            "/connectors/:connector_id/connector_images",
            get(controllers::connectors::images),
        )
}

fn connector_images_routes() -> Router {
    Router::new()
        .route(
            "/connector_images",
            get(controllers::connector_images::index).post(controllers::connector_images::create),
        )
        .route(
            "/connector_images/:image_id",
            get(controllers::connector_images::show),
        )
        .route(
            "/connector_images/:image_id/spec",
            get(controllers::connector_images::spec),
        )
        .route(
            "/connector_images/:image_id/discovered_catalog",
            post(controllers::connector_images::discovered_catalog),
        )
}

fn health_check_routes() -> Router {
    Router::new().route("/health_check", get(controllers::health_check::show))
}

fn sessions_routes() -> Router {
    Router::new().route("/sessions/:issuer", post(controllers::sessions::create))
}
