use hyper::header;
use hyper::Method;
use tower_http::cors::{self, AnyOr, Origin};

use crate::config::settings;

static ALLOWED_HEADERS: &[header::HeaderName] = &[
    // Safe Header List from https://developer.mozilla.org/en-US/docs/Glossary/CORS-safelisted_response_header
    header::CACHE_CONTROL,
    header::CONTENT_LANGUAGE,
    header::CONTENT_LENGTH,
    header::CONTENT_TYPE,
    header::EXPIRES,
    header::LAST_MODIFIED,
    header::PRAGMA,
    // Headers we need for the function of the API
    header::AUTHORIZATION,
];

pub fn cors_layer() -> cors::CorsLayer {
    let configured_origins = settings().application.cors.allowed_origins();

    cors::CorsLayer::new()
        .allow_headers(ALLOWED_HEADERS.to_vec())
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
