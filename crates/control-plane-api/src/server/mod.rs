use axum::{http::StatusCode, response::IntoResponse};
use base64::Engine;
use std::sync::{Arc, Mutex};

mod authorize_dekaf;
mod authorize_task;
mod authorize_user_collection;
mod authorize_user_prefix;
mod authorize_user_task;
mod create_data_plane;
mod error;
pub mod public;
mod snapshot;
mod update_l2_reporting;

use anyhow::Context;
use snapshot::Snapshot;

pub use error::{ApiError, ApiErrorExt};

use crate::proxy_connectors::DataPlaneConnectors;

/// Request wraps a JSON-deserialized request type T which
/// also implements the validator::Validate trait.
#[derive(Debug, Clone, Copy, Default)]
pub struct Request<T>(pub T);

/// ControlClaims are claims encoded within control-plane access tokens.
type ControlClaims = models::authorizations::ControlClaims;

/// DataClaims are claims encoded within data-plane access tokens.
/// TODO(johnny): This should be a bare alias for proto_gazette::Claims.
/// We can do this once data-plane-gateway is updated to be a "dumb" proxy
/// which requires / forwards authorizations but doesn't inspect them.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct DataClaims {
    #[serde(flatten)]
    inner: proto_gazette::Claims,
    // prefixes exclusively used by legacy auth checks in data-plane-gateway.
    prefixes: Vec<String>,
}

/// Rejection is an error type of reasons why an API request may fail.
#[derive(Debug, thiserror::Error)]
pub enum Rejection {
    #[error(transparent)]
    ValidationError(#[from] validator::ValidationErrors),
    #[error(transparent)]
    JsonError(#[from] axum::extract::rejection::JsonRejection),
}

pub(crate) struct App {
    _id_generator: Mutex<models::IdGenerator>,
    control_plane_jwt_verifier: jsonwebtoken::DecodingKey,
    control_plane_jwt_signer: jsonwebtoken::EncodingKey,
    jwt_validation: jsonwebtoken::Validation,
    pg_pool: sqlx::PgPool,
    publisher: crate::publications::Publisher<DataPlaneConnectors>,
    snapshot: std::sync::RwLock<Snapshot>,
}

impl App {
    // TODO(johnny): This should return a VerifiedClaims struct which
    // wraps the validated prefixes, with a const generic over the Capability.
    // It's a larger lift then I want to do right now, because models::Capability
    // cannot directly be used as a const generic, so IMO we'll instead want to
    // switch to using a u32 for representing const capability expectations,
    // with automatic Into conversions into lower const capabilities.
    // The intended purpose of the proposed VerifiedClaims struct is to wire it
    // through APIs such that we cannot possibly forget to verify authorizations.
    pub async fn verify_user_authorization(
        &self,
        claims: &ControlClaims,
        prefixes: Vec<String>,
        capability: models::Capability,
    ) -> Result<Vec<String>, crate::server::error::ApiError> {
        let started = chrono::Utc::now();
        loop {
            match Snapshot::evaluate(&self.snapshot, started, |snapshot: &Snapshot| {
                for prefix in &prefixes {
                    if !tables::UserGrant::is_authorized(
                        &snapshot.role_grants,
                        &snapshot.user_grants,
                        claims.sub,
                        prefix,
                        capability,
                    ) {
                        return Err(ApiError::unauthorized(prefix));
                    }
                }
                Ok((None, ()))
            }) {
                Ok((_exp, ())) => return Ok(prefixes),
                Err(Ok(backoff)) => {
                    tracing::debug!(?backoff, "waiting before retrying authZ check");
                    () = tokio::time::sleep(backoff).await;
                }
                Err(Err(err)) => return Err(err),
            }
        }
    }

    pub fn snapshot(&self) -> &std::sync::RwLock<Snapshot> {
        &self.snapshot
    }

    /// Uses the current authorization snapshot to filter `unfiltered_results`
    /// to include only the items that the user has `min_capability` to. The
    /// authorization snapshot won't be refreshed, so if it is empty or missing
    /// authorizations that have recently been added, then the filtering could
    /// be too strict.
    pub fn filter_results<I, R, F>(
        &self,
        claims: &ControlClaims,
        min_capability: models::Capability,
        unfiltered_results: I,
        extract_prefix: F,
    ) -> Vec<R>
    where
        I: IntoIterator<Item = R>,
        F: for<'a> Fn(&'a R) -> &'a str,
    {
        let started = chrono::Utc::now();
        let unfiltered_results = unfiltered_results.into_iter();
        let mut results = Vec::with_capacity(unfiltered_results.size_hint().0);

        Snapshot::evaluate(&self.snapshot, started, |snapshot: &Snapshot| {
            for candidate in unfiltered_results {
                let name = extract_prefix(&candidate);
                if tables::UserGrant::is_authorized(
                    &snapshot.role_grants,
                    &snapshot.user_grants,
                    claims.sub,
                    name,
                    min_capability,
                ) {
                    results.push(candidate);
                }
            }
            Ok((None, ()))
        })
        .expect("filter_results Snapshot::evaluate always returns Ok");
        results
    }
}

/// Build the agent's API router.
pub fn build_router(
    id_generator: models::IdGenerator,
    jwt_secret: Vec<u8>,
    pg_pool: sqlx::PgPool,
    publisher: crate::publications::Publisher<DataPlaneConnectors>,
    allow_origin: &[String],
) -> anyhow::Result<axum::Router<()>> {
    let mut jwt_validation = jsonwebtoken::Validation::default();
    jwt_validation.set_audience(&["authenticated"]);

    let app = Arc::new(App {
        _id_generator: Mutex::new(id_generator),
        control_plane_jwt_verifier: jsonwebtoken::DecodingKey::from_secret(&jwt_secret),
        control_plane_jwt_signer: jsonwebtoken::EncodingKey::from_secret(&jwt_secret),
        jwt_validation,
        pg_pool,
        publisher,
        snapshot: std::sync::RwLock::new(Snapshot::empty()),
    });
    tokio::spawn(snapshot::fetch_loop(app.clone()));

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

    let cors = tower_http::cors::CorsLayer::new()
        .allow_methods(tower_http::cors::AllowMethods::mirror_request())
        .allow_origin(tower_http::cors::AllowOrigin::list(allow_origin))
        .allow_headers(allow_headers);

    let public_api_router = public::api_v1_router(app.clone());

    let main_router = axum::Router::new()
        .route("/authorize/task", post(authorize_task::authorize_task))
        .route("/authorize/dekaf", post(authorize_dekaf::authorize_dekaf))
        .route(
            "/authorize/user/collection",
            post(authorize_user_collection::authorize_user_collection)
                .route_layer(axum::middleware::from_fn_with_state(app.clone(), authorize))
                .options(preflight_handler),
        )
        .route(
            "/authorize/user/prefix",
            post(authorize_user_prefix::authorize_user_prefix)
                .route_layer(axum::middleware::from_fn_with_state(app.clone(), authorize))
                .options(preflight_handler),
        )
        .route(
            "/authorize/user/task",
            post(authorize_user_task::authorize_user_task)
                .route_layer(axum::middleware::from_fn_with_state(app.clone(), authorize))
                .options(preflight_handler),
        )
        .route(
            "/admin/create-data-plane",
            post(create_data_plane::create_data_plane)
                .route_layer(axum::middleware::from_fn_with_state(app.clone(), authorize)),
        )
        .route(
            "/admin/update-l2-reporting",
            post(update_l2_reporting::update_l2_reporting)
                .route_layer(axum::middleware::from_fn_with_state(app.clone(), authorize)),
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

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        let axum::extract::Json(value) = axum::extract::Json::<T>::from_request(req, state).await?;
        value.validate()?;
        Ok(Request(value))
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

// Middleware which accepts either a refresh token or a control-plane access token,
// verifies it before proceeding, and then attaches verified Claims.
async fn authorize(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum_extra::TypedHeader(bearer): axum_extra::TypedHeader<
        axum_extra::headers::Authorization<axum_extra::headers::authorization::Bearer>,
    >,
    mut req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let mut token = bearer.token();
    let exchanged_token: Option<String>;

    // Is this is a refresh token? If so, first exchange for an access token.
    if !token.contains(".") {
        match exchange_refresh_token(&app, token).await {
            Ok(exchanged) => {
                exchanged_token = Some(exchanged);
                token = exchanged_token.as_ref().unwrap();
            }
            Err(err) => {
                return (
                    StatusCode::UNAUTHORIZED,
                    format!("failed to exchange refresh token: {err}"),
                )
                    .into_response();
            }
        }
    }

    let token = match jsonwebtoken::decode::<ControlClaims>(
        token,
        &app.control_plane_jwt_verifier,
        &app.jwt_validation,
    ) {
        Ok(claims) => claims,
        Err(err) => {
            return (
                StatusCode::UNAUTHORIZED,
                format!("failed to parse authorization token: {err}"),
            )
                .into_response();
        }
    };

    req.extensions_mut().insert(token.claims);
    next.run(req).await
}

async fn exchange_refresh_token(app: &App, refresh_token: &str) -> anyhow::Result<String> {
    #[derive(Debug, serde::Deserialize)]
    struct RefreshToken {
        id: models::Id,
        secret: String,
    }
    #[derive(Debug, serde::Deserialize)]
    struct GenerateTokenResponse {
        access_token: String,
    }

    let bearer = base64::engine::general_purpose::STANDARD
        .decode(refresh_token)
        .context("failed to base64-decode bearer token")?;
    let bearer: RefreshToken =
        serde_json::from_slice(&bearer).context("failed to decode refresh token")?;

    let response = sqlx::query!(
        "select generate_access_token($1, $2) as token",
        bearer.id as models::Id,
        bearer.secret,
    )
    .fetch_one(&app.pg_pool)
    .await
    .context("failed to generate access token")?;

    let GenerateTokenResponse { access_token } = response
        .token
        .map(|token| serde_json::from_value(token))
        .context("token response was null")?
        .context("failed to decode generated access token")?;

    Ok(access_token)
}

// Parse a data-plane claims token without verifying it's signature.
fn parse_untrusted_data_plane_claims(
    token: &str,
) -> Result<(jsonwebtoken::Header, proto_gazette::Claims), ApiError> {
    use error::ApiErrorExt;

    let jsonwebtoken::TokenData { header, claims }: jsonwebtoken::TokenData<proto_gazette::Claims> = {
        // In this pass we do not validate the signature,
        // because we don't yet know which data-plane the JWT is signed by.
        let empty_key = jsonwebtoken::DecodingKey::from_secret(&[]);
        let mut validation = jsonwebtoken::Validation::default();
        validation.insecure_disable_signature_validation();
        jsonwebtoken::decode(token, &empty_key, &validation)
            .map_err(|err| anyhow::anyhow!(err).with_status(StatusCode::BAD_REQUEST))?
    };
    tracing::debug!(?claims, ?header, "decoded authorization request");

    if claims.sub.is_empty() {
        return Err(
            anyhow::anyhow!("missing required JWT `sub` claim (task or shard ID)")
                .with_status(StatusCode::BAD_REQUEST),
        );
    }
    if claims.iss.is_empty() {
        return Err(
            anyhow::anyhow!("missing required JWT `iss` claim (data-plane FQDN)")
                .with_status(StatusCode::BAD_REQUEST),
        );
    }

    Ok((header, claims))
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

// Support the legacy data-plane by re-writing its internal service
// addresses to use the data-plane-gateway in external contexts.
fn maybe_rewrite_address(external: bool, address: &str) -> String {
    if external && address.contains("svc.cluster.local:") {
        "https://us-central1.v1.estuary-data.dev".to_string()
    } else {
        address.to_string()
    }
}

const fn map_capability_to_gazette(capability: models::Capability) -> u32 {
    match capability {
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
