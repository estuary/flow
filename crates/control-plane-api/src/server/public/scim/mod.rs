//! SCIM 2.0 API for user provisioning/deprovisioning and group-based access.
//!
//! Authenticates via hashed bearer tokens in `internal.scim_tokens`, scoped to
//! a tenant. Supports user provisioning (via GoTrue admin API), deprovisioning,
//! and group management (groups map to catalog prefix + capability grants).

mod discovery;
pub(crate) mod groups;
pub(crate) mod users;

use std::sync::Arc;

/// Middleware that logs the method, URI, and request body for SCIM requests.
async fn log_scim_request(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let method = request.method().clone();
    let uri = request.uri().clone();

    let (parts, body) = request.into_parts();
    let bytes = axum::body::to_bytes(body, 64 * 1024)
        .await
        .unwrap_or_default();

    let body_str = String::from_utf8_lossy(&bytes);
    tracing::info!(%method, %uri, body = %body_str, "SCIM request");

    let request = axum::extract::Request::from_parts(parts, axum::body::Body::from(bytes));
    next.run(request).await
}

/// Build the SCIM v2 router, nested under `/api/v1/scim/v2/`.
///
/// Discovery endpoints (ServiceProviderConfig, Schemas, ResourceTypes) are
/// unauthenticated per the SCIM spec — IdPs hit these during setup before
/// a token is configured. User and Group endpoints require a valid SCIM bearer token.
pub fn scim_router() -> axum::Router<Arc<crate::App>> {
    axum::Router::new()
        // Discovery endpoints — no authentication required.
        .route(
            "/api/v1/scim/v2/ServiceProviderConfig",
            axum::routing::get(discovery::service_provider_config),
        )
        .route(
            "/api/v1/scim/v2/Schemas",
            axum::routing::get(discovery::schemas),
        )
        .route(
            "/api/v1/scim/v2/ResourceTypes",
            axum::routing::get(discovery::resource_types),
        )
        // User endpoints — require SCIM bearer token (ScimContext extractor).
        .route(
            "/api/v1/scim/v2/Users",
            axum::routing::get(users::list_users).post(users::create_user),
        )
        .route(
            "/api/v1/scim/v2/Users/{id}",
            axum::routing::get(users::get_user).patch(users::patch_user),
        )
        // Group endpoints — require SCIM bearer token (ScimContext extractor).
        .route(
            "/api/v1/scim/v2/Groups",
            axum::routing::get(groups::list_groups).post(groups::create_group),
        )
        .route(
            "/api/v1/scim/v2/Groups/{id}",
            axum::routing::get(groups::get_group)
                .patch(groups::patch_group)
                .put(groups::replace_group)
                .delete(groups::delete_group),
        )
        .layer(axum::middleware::from_fn(log_scim_request))
}

/// Context extracted from a SCIM bearer token. Authenticates the request and
/// identifies the tenant the SCIM client is acting on behalf of.
pub struct ScimContext {
    /// The tenant prefix (e.g. "acmeCo/") that this SCIM token is scoped to.
    pub tenant: String,
    /// The tenant's SSO provider ID, used to scope user lookups by email domain.
    pub sso_provider_id: uuid::Uuid,
    /// Database connection pool.
    pub pg_pool: sqlx::PgPool,
    /// The full application state (for GoTrue API calls, etc.).
    pub app: Arc<crate::App>,
}

/// Rejection type for SCIM auth failures.
#[derive(Debug)]
pub enum ScimRejection {
    /// Missing or malformed Authorization header.
    MissingToken,
    /// Token hash not found in `internal.scim_tokens`.
    InvalidToken,
    /// Tenant has no SSO provider configured.
    NoSsoProvider,
    /// Internal error during auth.
    Internal(String),
}

impl axum::response::IntoResponse for ScimRejection {
    fn into_response(self) -> axum::response::Response {
        use axum::http::StatusCode;

        let (status, detail) = match self {
            ScimRejection::MissingToken => (StatusCode::UNAUTHORIZED, "missing bearer token"),
            ScimRejection::InvalidToken => (StatusCode::UNAUTHORIZED, "invalid bearer token"),
            ScimRejection::NoSsoProvider => (
                StatusCode::FORBIDDEN,
                "tenant has no SSO provider configured",
            ),
            ScimRejection::Internal(ref msg) => {
                tracing::error!(error = %msg, "SCIM auth internal error");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error")
            }
        };

        let body = serde_json::json!({
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"],
            "status": status.as_str(),
            "detail": detail,
        });

        (status, axum::Json(body)).into_response()
    }
}

impl axum::extract::FromRequestParts<Arc<crate::App>> for ScimContext {
    type Rejection = ScimRejection;

    fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &Arc<crate::App>,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        async move {
            // Extract bearer token from Authorization header.
            use axum_extra::{
                TypedHeader,
                headers::{Authorization, authorization::Bearer},
            };
            let TypedHeader(auth) =
                TypedHeader::<Authorization<Bearer>>::from_request_parts(parts, state)
                    .await
                    .map_err(|_| ScimRejection::MissingToken)?;

            // SHA-256 hash the token.
            use sha2::Digest;
            let hash = sha2::Sha256::digest(auth.token().as_bytes());
            let token_hash = hex::encode(hash);

            // Look up the token and join to tenants to get the tenant prefix and SSO provider.
            let row = sqlx::query!(
                r#"
                SELECT
                    t.tenant AS "tenant!: String",
                    t.sso_provider_id AS "sso_provider_id: uuid::Uuid"
                FROM internal.scim_tokens st
                JOIN tenants t ON t.id = st.tenant_id
                WHERE st.token_hash = $1
                "#,
                token_hash,
            )
            .fetch_optional(&state.pg_pool)
            .await
            .map_err(|e| ScimRejection::Internal(e.to_string()))?;

            let row = row.ok_or(ScimRejection::InvalidToken)?;
            let sso_provider_id = row.sso_provider_id.ok_or(ScimRejection::NoSsoProvider)?;

            Ok(ScimContext {
                tenant: row.tenant,
                sso_provider_id,
                pg_pool: state.pg_pool.clone(),
                app: Arc::clone(state),
            })
        }
    }
}

/// Helper to hash a plaintext SCIM token to its storage form.
pub fn hash_token(plaintext: &str) -> String {
    use sha2::Digest;
    let hash = sha2::Sha256::digest(plaintext.as_bytes());
    hex::encode(hash)
}
