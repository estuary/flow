mod open_metrics;
pub mod status;

use axum::{http::StatusCode, response::IntoResponse};
use std::sync::Arc;

use crate::api::{authorize, error::ApiErrorExt, ApiError, App};

/// Creates a router for the public API that can be merged into an existing router.
/// All endpoints registered here are documented in an OpenAPI spec. For adding new
/// endpoints, the general rule is to use a handler function signature like:
///
/// ```ignore
/// fn handle_{get|post|etc}_{resource_name}(
///     state: State<Arc<App>>, // has the database connection pool, etc
///     claims: Extension<ControlClaims>, // claims from a verified JWT (unauthenticated requests would be rejected automatically)
///     other_stuff: T, // other extracted data from the request
/// ) -> Result<Json<Resp>, ApiError>
/// ```
///
/// and register the handler using `.api_route(path, aide::axum::routing::get(handle_get_thing))`.
///
/// Other input parameters can be used, as long as they implement
/// `aide::operation::OperationInput`. The basic ones, like `Path` and `Query`
/// all do so already. This just ensures that the parameters are documented in
/// the OpenAPI spec. You can `impl aide::operation::OperationInput for MyInput
/// {}` if you don't want it to show in the spec.
///
/// For accepting query parameters, define a struct with `Deserialize` and
/// `JsonSchema` impls, and use a parameter of type
/// `axum_extra::extract::Query<MyQueryParams>` to extract it. This will
/// automatically return a 400 response if the given query parameters can't be
/// deserialized into the struct.
///
/// The output type `Result<Json<T>, ApiError>` is suitable for any handler that
/// returns JSON, which is all of them. Just ensure that `T` implements
/// `serde::Serialize` and `schemars::JsonSchema`. See the `crate::api::error` module
/// docs for more information on error handling.
pub(crate) fn api_v1_router(app: Arc<App>) -> axum::Router<Arc<App>> {
    // When errors occur during the process of generating an openapi spec, aide
    // will call this function with the error so we can log it. They have a note
    // in their docs warning about false positives where it logs errors even
    // when it's able to return a valid response. I know it smells, but seems
    // better than the available alternatives.
    aide::gen::on_error(|error| {
        tracing::error!(?error, "aide gen error");
        if cfg!(test) {
            panic!("aide gen error: {:?}", error);
        }
    });

    // Routes are defined in groups, with the first group all being
    // authenticated routes that require a valid authentication token, and the
    // second group being unauthenticated routes that can be accessed by anyone.
    let router = aide::axum::ApiRouter::new()
        .api_route(
            "/api/v1/catalog/status",
            aide::axum::routing::get(status::handle_get_status)
                .route_layer(axum::middleware::from_fn(ensure_accepts_json)),
        )
        .api_route(
            "/api/v1/metrics/*prefix",
            aide::axum::routing::get(open_metrics::handle_get_metrics),
        )
        // All routes below this are publicly accessible to anyone, without an authentication token
        .layer(axum::middleware::from_fn_with_state(app.clone(), authorize))
        // The openapi json is itself documented as an API route
        .api_route("/api/v1/openapi.json", aide::axum::routing::get(serve_docs))
        // The docs UI is not documented as an API route
        .route(
            "/api/v1/docs",
            axum::routing::get(
                aide::scalar::Scalar::new("/api/v1/openapi.json")
                    .with_title(API_TITLE)
                    .axum_handler(),
            ),
        )
        .with_state(app.clone());

    // There's kind of a weird twist here, where we take the `OpenApi` that
    // holds the generated documentation, and add it as an extension to the
    // router that we just generated the documentation from.
    let mut api = aide::openapi::OpenApi::default();
    let router = router.finish_api_with(&mut api, api_docs);
    router.layer(axum::Extension(Arc::new(api)))
}

/// Our API currently only supports JSON responses, so we check to make sure
/// that the accept header permits those.
async fn ensure_accepts_json(
    headers: axum::http::HeaderMap,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if let Some(val) = headers.get("accept") {
        let Ok(accept) = val.to_str() else {
            return anyhow::anyhow!("invalid accept header was not ascii")
                .with_status(StatusCode::BAD_REQUEST)
                .into_response();
        };
        if !accept.contains("application/json") && !accept.contains("*/*") {
            return anyhow::anyhow!("only application/json responses are supported at this time")
                .with_status(StatusCode::NOT_ACCEPTABLE)
                .into_response();
        }
    }
    next.run(req).await
}

/// Handler that serves the openapi spec as JSON
async fn serve_docs(
    axum::extract::Extension(api): axum::extract::Extension<Arc<aide::openapi::OpenApi>>,
) -> impl aide::axum::IntoApiResponse {
    axum::Json(api).into_response()
}

const API_TITLE: &str = "Flow Control Plane V1 API";

fn api_docs(api: aide::transform::TransformOpenApi) -> aide::transform::TransformOpenApi {
    api.title(API_TITLE)
        .summary("Controlling the control plane")
        .description("API for the Flow control plane")
        .security_scheme(
            "ApiKey",
            aide::openapi::SecurityScheme::Http {
                scheme: "bearer".to_string(),
                bearer_format: Some("JWT".to_string()),
                description: Some("Estuary authentication token".to_string()),
                extensions: Default::default(),
            },
        )
        .security_requirement("ApiKey")
        .default_response_with::<axum::Json<ApiError>, _>(|res| {
            res.example(ApiError::unauthorized("acmeCo/anvils/"))
        })
}
