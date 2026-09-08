use serde::{Deserialize, Serialize};
use sqlx::types::Uuid;

pub mod alert_subscriptions;
pub mod alerts;
pub mod billing;
pub mod connector_tags;
pub mod connectors;
pub mod controllers;
pub mod data_plane;
pub mod directives;
pub mod discovers;
pub mod draft;
mod envelope;
pub mod grants;
mod interval;
pub mod jobs;
pub mod live_specs;
pub mod logs;
pub mod publications;
pub mod server;
mod text_json;

#[cfg(test)]
pub(crate) mod test_server;

/// TextJson encodes JSON for Postgres while preserving property ordering.
pub use text_json::TextJson;

/// TODO(johnny): Could we use sqlx's native PgInterval type?
pub use interval::Interval;

/// ControlClaims are claims encoded within control-plane access tokens.
type ControlClaims = models::authorizations::ControlClaims;

/// DataClaims are claims encoded within data-plane access tokens.
pub type DataClaims = proto_gazette::Claims;

/// AuthZResult is the result of an authorization policy evaluation,
/// designed to be used with Envelope::authorization_outcome.
///
/// Its Ok variant contains an optional `cordon_at` DateTime which denotes when
/// the authorization will become invalid due to cordoning, which (when present)
/// upper-bounds the expiry of a derived authorization.
pub type AuthZResult<Ok> = tonic::Result<(Option<tokens::DateTime>, Ok)>;

/// Envelope is common fields and parameters of every API request.
pub use envelope::{Envelope, Locale, MaybeControlClaims};

// TODO(johnny): These types are all fundamental to this crate, and should be
// hoisted from the `server` module. For now, just re-export to minimize churn.
pub(crate) use server::evaluate_names_authorization;
pub use server::{
    ApiError, App, AuthZRetry, build_router,
    snapshot::{self, Snapshot},
};

// Re-export the GraphQL schema SDL function for flow-client build script
pub use server::public::graphql::schema_sdl as graphql_schema_sdl;

// TODO(johnny): Move to publications module?
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "flow_type")]
#[sqlx(rename_all = "snake_case")]
pub enum FlowType {
    Capture,
    Collection,
    Materialization,
    Test,
    SourceCapture,
}

impl From<models::CatalogType> for FlowType {
    fn from(c: models::CatalogType) -> Self {
        use models::CatalogType;
        match c {
            CatalogType::Capture => FlowType::Capture,
            CatalogType::Collection => FlowType::Collection,
            CatalogType::Materialization => FlowType::Materialization,
            CatalogType::Test => FlowType::Test,
        }
    }
}

/// Returns the user ID for the given email address, or an error if the email address is not found.
pub async fn get_user_id_for_email(email: &str, db: &sqlx::PgPool) -> sqlx::Result<Uuid> {
    sqlx::query_scalar!(
        r#"
        SELECT id
        FROM auth.users
        WHERE email = $1
        "#,
        email
    )
    .fetch_one(db)
    .await
}

fn status_into_response(mut status: tonic::Status) -> axum::response::Response {
    let http_code = tokens::rest::grpc_status_code_to_http(status.code());
    let http_code = axum::http::StatusCode::from_u16(http_code).unwrap();
    let mut builder = axum::response::Response::builder().status(http_code);

    // Map Status Metadata into HTTP headers.
    let mut headers = std::mem::take(status.metadata_mut()).into_headers();
    std::mem::swap(builder.headers_mut().unwrap(), &mut headers);

    let body = axum::body::Body::from(status.message().to_string());
    builder.body(body).unwrap()
}
