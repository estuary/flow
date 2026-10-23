//! OAuth authorization backed by existing refresh-token records.
use aes_siv::{KeyInit, aead::Aead};
use axum::response::IntoResponse;
use base64::Engine;
use sha2::Digest;
use std::sync::Arc;

mod cimd;
#[cfg(test)]
mod tests;

/// Host integration for session authorization and Estuary refresh-token storage.
#[async_trait::async_trait]
pub trait Backend: Send + Sync + Sized + 'static {
    type Session: axum::extract::FromRequestParts<Arc<Self>> + Send;

    fn config(&self) -> Result<&Config, Error>;

    async fn authorize_user(self: &Arc<Self>, session: Self::Session) -> Result<uuid::Uuid, Error>;

    async fn create_refresh_token(
        &self,
        user_id: uuid::Uuid,
        client_id: &str,
    ) -> anyhow::Result<RefreshTokenResponse>;

    async fn generate_access_token(
        &self,
        id: models::Id,
        secret: &str,
    ) -> tonic::Result<TokenResponse>;
}

pub struct TokenResponse {
    pub access_token: String,
    pub refresh_token: Option<RefreshTokenResponse>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct RefreshTokenResponse {
    pub id: models::Id,
    pub secret: String,
}

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::URL_SAFE_NO_PAD;
const SESSION_SECONDS: i64 = 600;
const CODE_SECONDS: i64 = 60;

/// Explicit issuer and UI locations avoid trusting request Host headers.
#[derive(clap::Args, serde::Deserialize)]
#[group(id = "oauth", multiple = true)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// HTTPS origin of the OAuth authorization server.
    #[arg(long = "oauth-issuer", env = "OAUTH_ISSUER", required = false,
        requires_all = ["consent_url", "encryption_key"])]
    pub issuer: url::Url,
    /// URL of the dashboard consent page.
    #[arg(long = "oauth-consent-url", env = "OAUTH_CONSENT_URL", required = false,
        requires_all = ["issuer", "encryption_key"])]
    pub consent_url: url::Url,
    /// Base64url (without padding) encoding of 64 random bytes, kept server-side.
    #[arg(long = "oauth-encryption-key", env = "OAUTH_ENCRYPTION_KEY", required = false,
        hide_env_values = true, requires_all = ["issuer", "consent_url"])]
    pub encryption_key: String,
}

impl Config {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.issuer.scheme() == "https"
                && self.issuer.host_str().is_some()
                && self.issuer.path() == "/"
                && self.issuer.query().is_none()
                && self.issuer.fragment().is_none()
                && self.issuer.username().is_empty()
                && self.issuer.password().is_none(),
            "OAuth issuer must be an HTTPS origin"
        );
        anyhow::ensure!(
            (self.consent_url.scheme() == "https"
                || (self.consent_url.scheme() == "http"
                    && cimd::is_loopback_host(self.consent_url.host())))
                && self.consent_url.host_str().is_some()
                && self.consent_url.fragment().is_none()
                && self.consent_url.query().is_none()
                && self.consent_url.username().is_empty()
                && self.consent_url.password().is_none(),
            "OAuth consent URL must use HTTPS (or HTTP for loopback) without credentials, query, or fragment"
        );
        anyhow::ensure!(
            B64.decode(&self.encryption_key)?.len() == 64,
            "OAuth encryption key must have 64 bytes"
        );
        Ok(())
    }
}

pub fn router<B: Backend>() -> axum::Router<Arc<B>> {
    axum::Router::new()
        // Let clients discover the server's endpoints and supported OAuth features.
        .route(
            "/.well-known/oauth-authorization-server",
            axum::routing::get(metadata::<B>),
        )
        // GET redirects the user to the consent UI; POST handles approval or denial.
        .route(
            "/oauth/authorize",
            axum::routing::get(authorize::<B>).post(consent::<B>),
        )
        // Decrypt the consent ticket so the dashboard can display the pending request.
        .route(
            "/oauth/consent-context",
            axum::routing::get(consent_context::<B>),
        )
        // Exchange an encrypted authorization code or refresh token for an access token.
        .route("/oauth/token", axum::routing::post(token::<B>))
        .layer(axum::extract::DefaultBodyLimit::max(16 * 1024))
        .layer(axum::middleware::from_fn(no_store))
}

async fn no_store(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let mut response = next.run(req).await;
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    response
        .headers_mut()
        .insert("pragma", "no-cache".parse().unwrap());
    response
        .headers_mut()
        .insert("referrer-policy", "no-referrer".parse().unwrap());
    response
}

/// OAuth protocol error code and public description.
#[derive(Debug)]
pub struct Error(pub &'static str, pub &'static str);
impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let status = if self.0 == "server_error" {
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        } else {
            axum::http::StatusCode::BAD_REQUEST
        };
        (
            status,
            axum::Json(serde_json::json!({"error": self.0, "error_description": self.1})),
        )
            .into_response()
    }
}
/// Log a backend fault and return a sanitized OAuth server error.
pub fn internal(err: impl std::fmt::Display) -> Error {
    tracing::error!(error = %err, "OAuth operation failed");
    Error("server_error", "unable to complete OAuth request")
}

async fn metadata<B: Backend>(
    axum::extract::State(app): axum::extract::State<Arc<B>>,
) -> Result<axum::Json<serde_json::Value>, Error> {
    let config = app.config()?;
    Ok(axum::Json(serde_json::json!({
        "issuer": config.issuer.as_str().trim_end_matches('/'),
        "authorization_endpoint": config.issuer.join("oauth/authorize").unwrap(),
        "token_endpoint": config.issuer.join("oauth/token").unwrap(),
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "token_endpoint_auth_methods_supported": ["none"],
        "code_challenge_methods_supported": ["S256"],
        "client_id_metadata_document_supported": true,
        "authorization_response_iss_parameter_supported": true
    })))
}

#[derive(Clone, serde::Deserialize, serde::Serialize)]
struct AuthorizationRequest {
    response_type: String,
    client_id: String,
    redirect_uri: String,
    code_challenge: String,
    code_challenge_method: String,
    state: Option<String>,
}

fn validate_request(req: &AuthorizationRequest) -> Result<(), Error> {
    if req.client_id.len() > 2048 || req.redirect_uri.len() > 2048 {
        return Err(Error(
            "invalid_request",
            "client_id and redirect_uri must not exceed 2048 bytes",
        ));
    }
    if req.response_type != "code" {
        return Err(Error(
            "unsupported_response_type",
            "only response_type=code is supported",
        ));
    }
    if req.code_challenge_method != "S256"
        || B64
            .decode(&req.code_challenge)
            .map_or(true, |v| v.len() != 32)
        || req.code_challenge.len() != 43
    {
        return Err(Error(
            "invalid_request",
            "a valid S256 code_challenge is required",
        ));
    }
    if req.state.as_ref().is_some_and(|s| s.len() > 2048) {
        return Err(Error("invalid_request", "state must not exceed 2048 bytes"));
    }
    Ok(())
}

/// Encryption keeps the code credential confidential until PKCE verification.
#[derive(serde::Serialize, serde::Deserialize)]
struct Sealed<T> {
    issuer: String,
    purpose: String,
    exp: i64,
    value: T,
}
fn seal<T: serde::Serialize>(
    config: &Config,
    purpose: &str,
    exp: i64,
    value: T,
) -> Result<String, Error> {
    let key = B64.decode(&config.encryption_key).map_err(internal)?;
    let cipher = aes_siv::Aes256SivAead::new_from_slice(&key).map_err(internal)?;
    let nonce: [u8; 16] = rand::random();
    let body = serde_json::to_vec(&Sealed {
        issuer: config.issuer.to_string(),
        purpose: purpose.to_owned(),
        exp,
        value,
    })
    .map_err(internal)?;
    let ciphertext = cipher
        .encrypt(aes_siv::Nonce::from_slice(&nonce), body.as_slice())
        .map_err(|_| Error("server_error", "unable to complete OAuth request"))?;
    Ok(B64.encode([nonce.as_slice(), ciphertext.as_slice()].concat()))
}
fn unseal<T: serde::de::DeserializeOwned>(
    config: &Config,
    purpose: &str,
    value: &str,
) -> Result<T, Error> {
    let error_description = if purpose == "code" {
        "unknown or expired authorization code"
    } else {
        "unknown or expired consent ticket"
    };
    if value.len() > 12000 {
        return Err(Error("invalid_grant", error_description));
    }
    let bytes = B64
        .decode(value)
        .map_err(|_| Error("invalid_grant", error_description))?;
    if bytes.len() < 32 {
        return Err(Error("invalid_grant", error_description));
    }
    let key = B64.decode(&config.encryption_key).map_err(internal)?;
    let cipher = aes_siv::Aes256SivAead::new_from_slice(&key).map_err(internal)?;
    let plain = cipher
        .decrypt(aes_siv::Nonce::from_slice(&bytes[..16]), &bytes[16..])
        .map_err(|_| Error("invalid_grant", error_description))?;
    let sealed: Sealed<T> =
        serde_json::from_slice(&plain).map_err(|_| Error("invalid_grant", error_description))?;
    if sealed.purpose != purpose
        || sealed.issuer != config.issuer.as_str()
        || sealed.exp <= tokens::now().timestamp()
    {
        return Err(Error("invalid_grant", error_description));
    }
    Ok(sealed.value)
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Pending {
    request: AuthorizationRequest,
    client_name: String,
}

// GET /oauth/authorize lands here
async fn authorize<B: Backend>(
    axum::extract::State(app): axum::extract::State<Arc<B>>,
    query: Result<
        axum::extract::Query<AuthorizationRequest>,
        axum::extract::rejection::QueryRejection,
    >,
) -> Result<axum::response::Redirect, Error> {
    let axum::extract::Query(req) = query.map_err(|_| {
        Error(
            "invalid_request",
            "missing or malformed authorization parameters",
        )
    })?;
    let config = app.config()?;
    validate_request(&req)?;
    let client = cimd::fetch_client_metadata(&req.client_id).await?;
    cimd::assert_redirect_uri_allowed(&req.redirect_uri, &client.redirect_uris)?;
    let pending = Pending {
        request: req,
        client_name: client.client_name,
    };
    let ticket = seal(
        config,
        "consent",
        tokens::now().timestamp() + SESSION_SECONDS,
        pending,
    )?;
    let mut destination = config.consent_url.clone();
    destination
        .query_pairs_mut()
        .append_pair("request", &ticket);
    Ok(axum::response::Redirect::to(destination.as_str()))
}

#[derive(serde::Deserialize)]
struct Ticket {
    request: String,
}
async fn consent_context<B: Backend>(
    axum::extract::State(app): axum::extract::State<Arc<B>>,
    axum::extract::Query(ticket): axum::extract::Query<Ticket>,
) -> Result<axum::Json<Pending>, Error> {
    Ok(axum::Json(unseal(
        app.config()?,
        "consent",
        &ticket.request,
    )?))
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Decision {
    request: String,
    approved: bool,
}
#[derive(serde::Serialize, serde::Deserialize)]
struct IssuedCode {
    request: AuthorizationRequest,
    id: models::Id,
    secret: String,
}

// POST /oauth/authorize lands here
async fn consent<B: Backend>(
    axum::extract::State(app): axum::extract::State<Arc<B>>,
    session: B::Session,
    axum::Json(decision): axum::Json<Decision>,
) -> Result<axum::Json<serde_json::Value>, Error> {
    let config = app.config()?;
    let pending: Pending = unseal(config, "consent", &decision.request)?;
    validate_request(&pending.request)?;
    let user_id = app.authorize_user(session).await?;
    let mut redirect = cimd::validate_redirect_uri(&pending.request.redirect_uri)?;
    if let Some(state) = &pending.request.state {
        redirect.query_pairs_mut().append_pair("state", state);
    }
    redirect
        .query_pairs_mut()
        .append_pair("iss", config.issuer.as_str().trim_end_matches('/'));
    if !decision.approved {
        redirect
            .query_pairs_mut()
            .append_pair("error", "access_denied");
        return Ok(axum::Json(serde_json::json!({"redirect_uri": redirect})));
    }
    let credential = app
        .create_refresh_token(user_id, &pending.request.client_id)
        .await
        .map_err(internal)?;
    let now = tokens::now().timestamp();
    let code = seal(
        config,
        "code",
        now + CODE_SECONDS,
        IssuedCode {
            request: pending.request,
            id: credential.id,
            secret: credential.secret,
        },
    )?;
    redirect.query_pairs_mut().append_pair("code", &code);
    Ok(axum::Json(serde_json::json!({"redirect_uri": redirect})))
}

#[derive(serde::Deserialize)]
struct TokenRequest {
    grant_type: String,
    client_id: Option<String>,
    code: Option<String>,
    redirect_uri: Option<String>,
    code_verifier: Option<String>,
    refresh_token: Option<String>,
}

fn validate_exchange(config: &Config, req: &TokenRequest) -> Result<RefreshTokenResponse, Error> {
    match req.grant_type.as_str() {
        "refresh_token" => {
            let value = req
                .refresh_token
                .as_deref()
                .ok_or(Error("invalid_request", "refresh_token is required"))?;
            let bytes = tokens::jwt::parse_base64(value)
                .map_err(|_| Error("invalid_grant", "refresh_token is malformed"))?;
            serde_json::from_slice(&bytes)
                .map_err(|_| Error("invalid_grant", "refresh_token is malformed"))
        }
        "authorization_code" => {
            let code: IssuedCode = unseal(
                config,
                "code",
                req.code
                    .as_deref()
                    .ok_or(Error("invalid_request", "code is required"))?,
            )?;
            if req
                .client_id
                .as_ref()
                .is_some_and(|id| id != &code.request.client_id)
            {
                return Err(Error(
                    "invalid_grant",
                    "client_id does not match the authorization request",
                ));
            }
            if req
                .redirect_uri
                .as_ref()
                .is_some_and(|uri| uri != &code.request.redirect_uri)
            {
                return Err(Error(
                    "invalid_grant",
                    "redirect_uri does not match the authorization request",
                ));
            }
            let verifier = req
                .code_verifier
                .as_deref()
                .ok_or(Error("invalid_request", "code_verifier is required"))?;
            if !(43..=128).contains(&verifier.len())
                || !verifier
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-._~".contains(&b))
                || B64.encode(sha2::Sha256::digest(verifier.as_bytes()))
                    != code.request.code_challenge
            {
                return Err(Error("invalid_grant", "PKCE verification failed"));
            }
            Ok(RefreshTokenResponse {
                id: code.id,
                secret: code.secret,
            })
        }
        _ => Err(Error(
            "unsupported_grant_type",
            "only authorization_code and refresh_token grants are supported",
        )),
    }
}

// POST /oauth/token lands here
async fn token<B: Backend>(
    axum::extract::State(app): axum::extract::State<Arc<B>>,
    form: Result<axum::Form<TokenRequest>, axum::extract::rejection::FormRejection>,
) -> Result<axum::Json<serde_json::Value>, Error> {
    let axum::Form(req) =
        form.map_err(|_| Error("invalid_request", "missing or malformed token request form"))?;
    let config = app.config()?;
    let credential = validate_exchange(config, &req)?;
    let exchanged = app
        .generate_access_token(credential.id, &credential.secret)
        .await
        .map_err(|err| {
            if err.code() == tonic::Code::Unauthenticated {
                Error(
                    "invalid_grant",
                    "credential is invalid, expired, revoked, or already used",
                )
            } else {
                internal(err)
            }
        })?;
    // Reuse the original refresh token when the backend doesn't rotate it.
    let refresh_token = base64::engine::general_purpose::STANDARD.encode(
        serde_json::to_vec(&exchanged.refresh_token.unwrap_or(credential)).map_err(internal)?,
    );
    Ok(axum::Json(serde_json::json!({
        "access_token": exchanged.access_token,
        "token_type": "Bearer",
        "expires_in": 3600,
        "refresh_token": refresh_token,
    })))
}
