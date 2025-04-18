use axum::{http::StatusCode, response::IntoResponse};
use models::Capability;
use std::sync::{Arc, Mutex};

mod authorize_dekaf;
mod authorize_task;
mod authorize_user_collection;
mod authorize_user_task;
mod create_data_plane;
mod error;
mod public;
mod snapshot;
mod update_l2_reporting;

use anyhow::Context;
use snapshot::Snapshot;

pub use error::ApiError;

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

struct App {
    _id_generator: Mutex<models::IdGenerator>,
    control_plane_jwt_verifier: jsonwebtoken::DecodingKey,
    control_plane_jwt_signer: jsonwebtoken::EncodingKey,
    jwt_validation: jsonwebtoken::Validation,
    pg_pool: sqlx::PgPool,
    publisher: crate::publications::Publisher,
    snapshot: std::sync::RwLock<Snapshot>,
}

impl App {
    pub async fn is_user_authorized(
        &self,
        claims: &ControlClaims,
        catalog_names: &[impl AsRef<str>],
        capability: Capability,
    ) -> Result<bool, crate::api::ApiError> {
        let started_unix = jsonwebtoken::get_current_timestamp();
        loop {
            match Snapshot::evaluate(&self.snapshot, started_unix, |snapshot: &Snapshot| {
                for catalog_name in catalog_names {
                    if !tables::UserGrant::is_authorized(
                        &snapshot.role_grants,
                        &snapshot.user_grants,
                        claims.sub,
                        catalog_name.as_ref(),
                        capability,
                    ) {
                        tracing::debug!(
                            catalog_name=%catalog_name.as_ref(),
                            required_capability = ?capability,
                            user_id = %claims.sub,
                            "user is unauthorized"
                        );
                        return Ok(false);
                    }
                }
                Ok(true)
            }) {
                Ok(authz_result) => return Ok(authz_result),
                Err(Ok(retry_millis)) => {
                    tracing::debug!(%retry_millis, "waiting before retrying authZ check");
                    () = tokio::time::sleep(std::time::Duration::from_millis(retry_millis)).await;
                }
                Err(Err(err)) => return Err(err),
            }
        }
    }
}

/// Build the agent's API router.
pub fn build_router(
    id_generator: models::IdGenerator,
    jwt_secret: Vec<u8>,
    pg_pool: sqlx::PgPool,
    publisher: crate::publications::Publisher,
    allow_origin: &[String],
) -> anyhow::Result<axum::Router<()>> {
    let mut jwt_validation = jsonwebtoken::Validation::default();
    jwt_validation.set_audience(&["authenticated"]);

    let (snapshot, seed_rx) = snapshot::seed();

    let app = Arc::new(App {
        _id_generator: Mutex::new(id_generator),
        control_plane_jwt_verifier: jsonwebtoken::DecodingKey::from_secret(&jwt_secret),
        control_plane_jwt_signer: jsonwebtoken::EncodingKey::from_secret(&jwt_secret),
        jwt_validation,
        pg_pool,
        publisher,
        snapshot: std::sync::RwLock::new(snapshot),
    });
    tokio::spawn(snapshot::fetch_loop(app.clone(), seed_rx));

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
            "/authorize/user/task",
            post(authorize_user_task::authorize_user_task)
                .route_layer(axum::middleware::from_fn_with_state(app.clone(), authorize))
                .options(preflight_handler),
        )
        .route(
            "/authorize/user/collection",
            post(authorize_user_collection::authorize_user_collection)
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

#[axum::async_trait]
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

    let bearer = base64::decode(refresh_token).context("failed to base64-decode bearer token")?;
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

fn exp_seconds() -> u64 {
    use rand::Rng;

    // Select a random expiration time in range [40, 80) minutes,
    // which spreads out load from re-authorization requests over time.
    rand::thread_rng().gen_range(40 * 60..80 * 60)
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
