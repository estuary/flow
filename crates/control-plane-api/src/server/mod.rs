use crate::AuthZResult;
use anyhow::Context;
use axum::{http::StatusCode, response::IntoResponse};
use std::sync::Arc;

mod authorize_dekaf;
mod authorize_task;
mod authorize_user_collection;
mod authorize_user_prefix;
mod authorize_user_task;
mod create_data_plane;
mod error;
pub mod public;
pub mod snapshot;
mod update_l2_reporting;

pub use error::{ApiError, AuthZRetry};
pub use snapshot::Snapshot;

/// Request wraps a JSON-deserialized request type T which
/// also implements the validator::Validate trait.
#[derive(Debug, Clone, Copy, Default)]
pub struct Request<T>(pub T);

/// Rejection is an error type of reasons why an API request may fail.
#[derive(Debug, thiserror::Error)]
pub enum Rejection {
    #[error(transparent)]
    ValidationError(#[from] validator::ValidationErrors),
    #[error(transparent)]
    JsonError(#[from] axum::extract::rejection::JsonRejection),
}

/// App is the wired application state of the control-plane API.
pub struct App {
    pub _id_generator: std::sync::Mutex<models::IdGenerator>,
    pub billing_provider: Option<Arc<dyn crate::billing::BillingProvider>>,
    pub control_plane_jwt_decode_keys: Vec<tokens::jwt::DecodingKey>,
    pub control_plane_jwt_encode_key: tokens::jwt::EncodingKey,
    pub pg_pool: sqlx::PgPool,
    pub publisher: crate::publications::Publisher,
    pub snapshot: Arc<dyn tokens::Watch<Snapshot>>,
}

impl App {
    pub fn new(
        id_generator: models::IdGenerator,
        billing_provider: Option<Arc<dyn crate::billing::BillingProvider>>,
        jwt_secret: &[u8],
        pg_pool: sqlx::PgPool,
        publisher: crate::publications::Publisher,
        snapshot: Arc<dyn tokens::Watch<Snapshot>>,
    ) -> Self {
        Self {
            _id_generator: std::sync::Mutex::new(id_generator),
            billing_provider,
            control_plane_jwt_decode_keys: vec![tokens::jwt::DecodingKey::from_secret(jwt_secret)],
            control_plane_jwt_encode_key: tokens::jwt::EncodingKey::from_secret(jwt_secret),
            pg_pool,
            publisher,
            snapshot,
        }
    }
}

/// Evaluate whether the user identified by `claims` is authorized to access all
/// of the enumerated `prefixes_or_names` with at least `min_capability`.
/// Return a policy_result shape which fits Envelope::authorization_outcome.
///
/// `min_capability` accepts any value that converts into a `CapabilitySet`:
/// legacy `models::Capability` (mapped via `bits_for_legacy`), a single
/// `models::authz::Capability` bit, or an explicit `CapabilitySet`.
pub fn evaluate_names_authorization<'r, Iter, S, C>(
    snapshot: &Snapshot,
    claims: &crate::ControlClaims,
    min_capability: C,
    prefixes_or_names: Iter,
) -> AuthZResult<()>
where
    Iter: IntoIterator<Item = S>,
    S: AsRef<str> + std::fmt::Display,
    C: Into<models::authz::CapabilitySet> + std::fmt::Display + Copy,
{
    let models::authorizations::ControlClaims {
        sub: user_id,
        email: user_email,
        ..
    } = claims;
    let user_email = user_email.as_ref().map(String::as_str).unwrap_or("user");

    for prefix_or_name in prefixes_or_names.into_iter() {
        if !tables::UserGrant::is_authorized(
            &snapshot.role_grants,
            &snapshot.user_grants,
            *user_id,
            prefix_or_name.as_ref(),
            min_capability,
        ) {
            return Err(tonic::Status::permission_denied(format!(
                "{user_email} is not authorized to access prefix or name '{prefix_or_name}' with required capability {min_capability}",
            )));
        }
    }
    Ok((None, ()))
}

/// Looks up the user's authorization grants for each item in
/// `prefixes_or_names`, and calls the provided `attach` function with each
/// item and its capability. The `Some` results are returned in a vec.
pub fn attach_user_capabilities<I, F, T>(
    snapshot: &Snapshot,
    claims: &crate::ControlClaims,
    prefixes_or_names: I,
    mut attach: F,
) -> Vec<T>
where
    I: IntoIterator<Item = String>,
    F: FnMut(String, Option<models::Capability>) -> Option<T>,
{
    prefixes_or_names
        .into_iter()
        .flat_map(|prefix| {
            let capability = tables::UserGrant::get_user_capability(
                &snapshot.role_grants,
                &snapshot.user_grants,
                claims.sub,
                &prefix,
            );
            attach(prefix, capability)
        })
        .collect()
}

/// Build the agent's API router.
pub fn build_router(
    app: Arc<App>,
    allow_origin: &[String],
    alert_config_defaults: models::AlertConfig,
) -> anyhow::Result<axum::Router<()>> {
    use axum::routing::post;

    let allow_origin = allow_origin
        .into_iter()
        .map(|o| o.parse())
        .collect::<Result<Vec<_>, _>>()
        .context("failed to parse allowed origins")?;

    let allow_headers = [
        "Cache-Control",
        "Content-Language",
        "Content-Length",
        "Content-Type",
        "Expires",
        "Last-Modified",
        "Pragma",
        "Authorization",
    ]
    .into_iter()
    .map(|h| h.parse().unwrap())
    .collect::<Vec<_>>();

    // Sets the Access-Control-Max-Age header to 1 hour, to allow browsers to
    // avoid making a ton of extra pre-flight requests. We don't change this
    // often, so 1 hour seemed like a reasonable bound on when clients would
    // observe any changes to cors. Max supported by chome is 2 hours.
    let cors_max_age = std::time::Duration::from_secs(60 * 60 * 1);
    let cors = tower_http::cors::CorsLayer::new()
        .max_age(cors_max_age)
        .allow_methods(tower_http::cors::AllowMethods::mirror_request())
        .allow_origin(tower_http::cors::AllowOrigin::list(allow_origin))
        .allow_headers(allow_headers);

    let public_api_router = public::api_v1_router(app.clone(), alert_config_defaults);

    let main_router = axum::Router::new()
        .route("/authorize/task", post(authorize_task::authorize_task))
        .route("/authorize/dekaf", post(authorize_dekaf::authorize_dekaf))
        .route(
            "/authorize/user/collection",
            post(authorize_user_collection::authorize_user_collection).options(preflight_handler),
        )
        .route(
            "/authorize/user/prefix",
            post(authorize_user_prefix::authorize_user_prefix).options(preflight_handler),
        )
        .route(
            "/authorize/user/task",
            post(authorize_user_task::authorize_user_task).options(preflight_handler),
        )
        .route(
            "/admin/create-data-plane",
            post(create_data_plane::create_data_plane),
        )
        .route(
            "/admin/update-l2-reporting",
            post(update_l2_reporting::update_l2_reporting),
        )
        .merge(public_api_router)
        .layer(
            tower_http::trace::TraceLayer::new_for_http()
                .on_failure(tower_http::trace::DefaultOnFailure::new().level(tracing::Level::INFO)),
        )
        .layer(cors)
        .with_state(app);

    Ok(main_router)
}

async fn preflight_handler() -> impl IntoResponse {
    (StatusCode::NO_CONTENT, "")
}

impl<T, S> axum::extract::FromRequest<S> for Request<T>
where
    T: serde::de::DeserializeOwned + validator::Validate,
    S: Send + Sync,
    axum::extract::Json<T>:
        axum::extract::FromRequest<S, Rejection = axum::extract::rejection::JsonRejection>,
{
    type Rejection = Rejection;

    fn from_request(
        req: axum::extract::Request,
        state: &S,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        async move {
            let axum::extract::Json(value) =
                axum::extract::Json::<T>::from_request(req, state).await?;
            value.validate()?;
            Ok(Request(value))
        }
    }
}

impl axum::response::IntoResponse for Rejection {
    fn into_response(self) -> axum::response::Response {
        match self {
            Rejection::ValidationError(inner) => {
                let message = format!("Input validation error: [{inner}]").replace('\n', ", ");
                (StatusCode::BAD_REQUEST, message).into_response()
            }
            Rejection::JsonError(inner) => inner.into_response(),
        }
    }
}

pub async fn exchange_refresh_token(
    pg_pool: &sqlx::PgPool,
    refresh_token: &str,
) -> tonic::Result<String> {
    #[derive(Debug, serde::Deserialize)]
    struct RefreshToken {
        id: models::Id,
        secret: String,
    }
    #[derive(Debug, serde::Deserialize)]
    struct GenerateTokenResponse {
        access_token: String,
    }

    let bearer = tokens::jwt::parse_base64(refresh_token)?;
    let bearer: RefreshToken = serde_json::from_slice(&bearer)
        .map_err(|err| tonic::Status::invalid_argument(format!("invalid bearer token: {err}")))?;

    let response = sqlx::query!(
        "select generate_access_token($1, $2) as token",
        bearer.id as models::Id,
        bearer.secret,
    )
    .fetch_one(pg_pool)
    .await
    .map_err(|err| {
        tonic::Status::unauthenticated(format!("failed to exchange refresh token: {err}"))
    })?;

    let GenerateTokenResponse { access_token } =
        serde_json::from_value(response.token.unwrap_or_default()).map_err(|err| {
            tonic::Status::internal(format!("invalid access token generated: {err}"))
        })?;

    Ok(access_token)
}

/// Authenticate a service-account API key presented as a bearer credential,
/// returning the claims it proves.
///
/// API keys are evaluated *statefully only*: every presentation is verified
/// against the database, and a key is never exchanged for a signed JWT. This
/// is what makes key revocation immediate — there are no outstanding minted
/// tokens to wait out — and it's why key secrets are hashed with SHA-256
/// rather than bcrypt: the secret is high-entropy random (so a slow hash adds
/// no brute-force protection) and this verification is in the per-request hot
/// path, where a fast hash matters.
pub async fn authenticate_api_key(
    pg_pool: &sqlx::PgPool,
    api_key: &str,
) -> tonic::Result<tokens::Verified<crate::ControlClaims>> {
    let raw = api_key
        .strip_prefix("flow_sa_")
        .expect("caller dispatches on the flow_sa_ prefix");

    use base64::Engine;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| tonic::Status::invalid_argument("malformed api key: invalid base64"))?;
    let decoded = String::from_utf8(decoded)
        .map_err(|_| tonic::Status::invalid_argument("malformed api key: invalid UTF-8"))?;

    let (id_str, secret) = decoded
        .split_once(':')
        .ok_or_else(|| tonic::Status::invalid_argument("malformed api key payload"))?;
    let key_id: models::Id = id_str
        .parse()
        .map_err(|_| tonic::Status::invalid_argument("malformed api key: invalid key id"))?;

    // Validate the secret, expiry, and revocation in one query, stamping
    // last_used_at on both the key and its service account as part of the
    // same verification round-trip.
    let row = sqlx::query!(
        r#"
        WITH verified AS (
            UPDATE internal.api_keys
            SET last_used_at = now()
            WHERE id = $1
              AND secret_hash = encode(digest($2, 'sha256'), 'hex')
              AND expires_at > now()
              AND revoked_at IS NULL
            RETURNING service_account_id
        )
        UPDATE internal.service_accounts sa
        SET last_used_at = now()
        FROM verified v
        WHERE sa.user_id = v.service_account_id
        RETURNING sa.user_id
        "#,
        key_id as models::Id,
        secret,
    )
    .fetch_optional(pg_pool)
    .await
    .map_err(|err| tonic::Status::internal(format!("failed to authenticate api key: {err}")))?
    .ok_or_else(|| tonic::Status::unauthenticated("invalid, expired, or revoked api key"))?;

    // As with refresh-token bearer authentication: verification re-runs on
    // every presentation, making revocation near-immediate, and the small
    // expiry only bounds any future caching of this authentication.
    let now = tokens::now();
    let exp = now + chrono::Duration::minutes(5);

    let claims = crate::ControlClaims {
        iat: now.timestamp() as u64,
        exp: exp.timestamp() as u64,
        sub: row.user_id,
        role: "authenticated".to_string(),
        aud: "authenticated".to_string(),
        email: None,
    };

    Ok(tokens::Verified::assert_authenticity(claims, exp))
}

/// Parse a data-plane claims token without verifying its signature.
/// Returns an `Unverified` wrapper to make clear the claims have not been verified.
fn parse_untrusted_data_plane_claims(
    token: &str,
) -> tonic::Result<tokens::jwt::Unverified<proto_gazette::Claims>> {
    let unverified = tokens::jwt::parse_unverified::<proto_gazette::Claims>(token.as_bytes())?;
    let claims = unverified.claims();

    tracing::debug!(?claims, "decoded authorization request");

    if claims.sub.is_empty() {
        return Err(tonic::Status::unauthenticated(
            "missing required JWT `sub` claim (task or shard ID)",
        ));
    }
    if claims.iss.is_empty() {
        return Err(tonic::Status::unauthenticated(
            "missing required JWT `iss` claim (data-plane FQDN)",
        ));
    }
    if claims.cap & proto_flow::capability::AUTHORIZE == 0 {
        return Err(tonic::Status::unauthenticated(
            "missing required AUTHORIZE capability",
        ));
    }

    Ok(unverified)
}

fn ops_suffix(task: &snapshot::SnapshotTask) -> String {
    let ops_kind = match task.spec_type {
        models::CatalogType::Capture => "capture",
        models::CatalogType::Collection => "derivation",
        models::CatalogType::Materialization => "materialization",
        models::CatalogType::Test => "test",
    };
    format!(
        "/kind={ops_kind}/name={}/pivot=00",
        labels::percent_encoding(&task.task_name).to_string(),
    )
}

const fn map_capability_to_gazette(capability: models::Capability) -> u32 {
    match capability {
        models::Capability::None => {
            panic!("gazette capability mapping requires Read, Write, or Admin")
        }
        models::Capability::Read => {
            proto_gazette::capability::LIST | proto_gazette::capability::READ
        }
        models::Capability::Write => {
            proto_gazette::capability::LIST
                | proto_gazette::capability::READ
                | proto_gazette::capability::APPEND
        }
        models::Capability::Admin => {
            proto_gazette::capability::LIST
                | proto_gazette::capability::READ
                | proto_gazette::capability::APPEND
                | proto_gazette::capability::APPLY
        }
    }
}
