use hyper::Method;
use tower_http::cors::{self, AnyOr, Origin};

use crate::config::settings;

pub fn cors_layer() -> cors::CorsLayer {
    let configured_origins = settings().application.cors.allowed_origins();

    cors::CorsLayer::new()
        .allow_methods(vec![Method::GET, Method::POST, Method::OPTIONS])
        .allow_origin(allowed_origins(configured_origins))
}

fn allowed_origins(configured_origins: &[String]) -> AnyOr<Origin> {
    if configured_origins == &["*"] {
        cors::any().into()
    } else {
        cors::Origin::list(configured_origins.iter().map(|o| o.parse().unwrap())).into()
    }
}
