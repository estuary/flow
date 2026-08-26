use axum::response::IntoResponse;
use std::sync::Arc;

pub mod graphql;
mod open_metrics;
pub mod status;
pub mod stripe_webhooks;
pub mod token_exchange;

/// Creates a router for the public API that can be merged into an existing router.
/// All endpoints registered here are documented in an OpenAPI spec. For adding new
/// endpoints, the general rule is to use a handler function signature like:
///
/// ```ignore
/// fn handle_{get|post|etc}_{resource_name}(
///     // has the database connection pool, verified claims, capability mask, etc
///     crate::Authority { envelope: env, .. }: crate::Authority,
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
/// `serde::Serialize` and `schemars::JsonSchema`. See the `crate::server::error` module
/// docs for more information on error handling.
pub(crate) fn api_v1_router(
    app: Arc<crate::App>,
    alert_config_defaults: models::AlertConfig,
) -> axum::Router<Arc<crate::App>> {
    // When errors occur during the process of generating an openapi spec, aide
    // will call this function with the error so we can log it. They have a note
    // in their docs warning about false positives where it logs errors even
    // when it's able to return a valid response. I know it smells, but seems
    // better than the available alternatives.
    aide::generate::on_error(|error| {
        tracing::error!(?error, "aide gen error");
        if cfg!(test) {
            panic!("aide gen error: {:?}", error);
        }
    });

    let graphql_schema = graphql::create_schema(alert_config_defaults);
    let router = aide::axum::ApiRouter::new()
        .api_route(
            "/api/v1/catalog/status",
            aide::axum::routing::get(status::handle_get_status)
                .route_layer(axum::middleware::from_fn(ensure_accepts_json)),
        )
        .api_route(
            "/api/v1/metrics/{*prefix}",
            aide::axum::routing::get(open_metrics::handle_get_metrics),
        )
        .route(
            "/api/graphql",
            axum::routing::post(graphql::graphql_handler),
        )
        .route("/graphiql", axum::routing::get(graphql::graphql_graphiql))
        // Stripe webhook receiver. Registered as a plain route (not `.api_route`)
        // because it isn't part of our documented public API and authenticates
        // via a signed raw body rather than the usual JWT/JSON convention.
        .route(
            "/api/v1/stripe/webhook",
            axum::routing::post(stripe_webhooks::handle_post_stripe_webhook),
        )
        // The openapi json is itself documented as an API route
        .api_route("/api/v1/openapi.json", aide::axum::routing::get(serve_docs))
        // The docs UI is not documented as an API route
        .api_route(
            "/api/v1/auth/token",
            aide::axum::routing::post(token_exchange::handle_post_token),
        )
        .route(
            "/api/v1/docs",
            axum::routing::get(
                aide::scalar::Scalar::new("/api/v1/openapi.json")
                    .with_title(API_TITLE)
                    .axum_handler(),
            ),
        )
        // Makes the graphql schema available to handlers
        .layer(axum::Extension(graphql_schema))
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
            return crate::ApiError::Status(tonic::Status::invalid_argument(
                "invalid accept header was not ascii",
            ))
            .into_response();
        };
        if !accept.contains("application/json") && !accept.contains("*/*") {
            return crate::ApiError::Status(tonic::Status::invalid_argument(
                "only application/json responses are supported at this time",
            ))
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
}

#[cfg(test)]
mod test {
    /// The real public v1 router over an empty Snapshot and a pool which is
    /// never dialed: these tests exercise extraction-time capability
    /// requirements, which reject before any handler body runs.
    async fn router() -> (axum::Router, tokens::jwt::EncodingKey) {
        let snapshot = crate::test_server::empty_snapshot().await;
        let pg_pool = sqlx::PgPool::connect_lazy("postgres://unused-by-extraction").unwrap();
        let app = crate::test_server::build_app(pg_pool, snapshot, None);
        let encoding_key = app.control_plane_jwt_encode_key.clone();

        let router =
            super::api_v1_router(app.clone(), models::AlertConfig::default()).with_state(app);

        (router, encoding_key)
    }

    fn sign_token(
        encoding_key: &tokens::jwt::EncodingKey,
        capability_mask: Option<Vec<&str>>,
    ) -> String {
        let now = tokens::now();
        let claims = models::authorizations::ControlClaims {
            iat: now.timestamp() as u64,
            exp: (now + chrono::Duration::hours(1)).timestamp() as u64,
            sub: uuid::Uuid::nil(),
            role: "authenticated".to_string(),
            aud: "authenticated".to_string(),
            email: None,
            capability_mask: capability_mask
                .map(|names| names.into_iter().map(String::from).collect()),
        };
        jsonwebtoken::encode(&jsonwebtoken::Header::default(), &claims, encoding_key).unwrap()
    }

    async fn fetch(router: &axum::Router, path: &str, bearer: &str) -> (u16, String) {
        use tower::ServiceExt;

        let request = axum::http::Request::builder()
            .uri(path)
            .header("authorization", format!("Bearer {bearer}"))
            .header("accept", "application/json")
            .body(axum::body::Body::empty())
            .unwrap();

        let (parts, body) = router.clone().oneshot(request).await.unwrap().into_parts();
        let body = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        (parts.status.as_u16(), String::from_utf8_lossy(&body).into())
    }

    #[test]
    fn test_viewer_requirement_matches_legacy_read() {
        // The route consts on /catalog/status and /metrics and their
        // handlers' walk requirement (legacy Read) are two statements of one
        // fact; this pins them together so they cannot drift.
        assert_eq!(
            <crate::RequireViewer as crate::Requirement>::REQUIRED,
            models::authz::bits_for_legacy(models::Capability::Read),
        );
    }

    #[tokio::test]
    async fn test_status_and_metrics_reject_mask_shortfall_at_extraction() {
        let (router, key) = router().await;

        // A mask which doesn't cover the Viewer bits is rejected with the
        // structured 403 before the handler (or any database access) runs.
        let masked = sign_token(&key, Some(vec!["SpecEdit"]));
        for path in [
            "/api/v1/catalog/status?name=acmeCo/thing",
            "/api/v1/metrics/acmeCo/",
        ] {
            let (status, body) = fetch(&router, path, &masked).await;
            assert_eq!(status, 403, "{path}: {body}");

            let body: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(body["error"], "missing_capabilities", "{path}");
            assert_eq!(
                body["missing_capabilities"],
                serde_json::json!([
                    "CatalogRead",
                    "JournalRead",
                    "ViewDataPlanePrivateNetworking"
                ]),
                "{path}",
            );
        }

        // A mask which covers the requirement passes extraction: with no
        // grants in the (empty) Snapshot the walk then provisionally denies,
        // which surfaces as the 307 retry — proof the const is a necessary
        // condition on the mask only, never a grant decision.
        let covering = sign_token(
            &key,
            Some(vec![
                "CatalogRead",
                "JournalRead",
                "ViewDataPlanePrivateNetworking",
            ]),
        );
        // An unmasked bearer behaves identically.
        let unmasked = sign_token(&key, None);

        for bearer in [&covering, &unmasked] {
            for path in [
                "/api/v1/catalog/status?name=acmeCo/thing",
                "/api/v1/metrics/acmeCo/",
            ] {
                let (status, body) = fetch(&router, path, bearer).await;
                assert_eq!(status, 307, "{path}: {body}");
            }
        }
    }
}
