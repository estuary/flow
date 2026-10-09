//! OAuth authorization server; the dashboard owns login and consent presentation.

mod client;
#[cfg(test)]
mod test;
pub use client::{Config, Metadata};

use base64::Engine;
use sha2::Digest;

const SCOPE: &str = "estuary";
type App = std::sync::Arc<crate::App>;

pub(crate) fn router() -> axum::Router<App> {
    axum::Router::new()
        .route(
            "/.well-known/oauth-authorization-server",
            axum::routing::get(discovery),
        )
        .route("/oauth/authorize", axum::routing::get(authorize))
        .route("/oauth/token", axum::routing::post(token))
        .route(
            "/oauth/consent/{id}",
            axum::routing::get(consent).post(decide),
        )
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

#[derive(Debug)]
pub(super) struct Error(oauth2::basic::BasicErrorResponseType);

impl Error {
    fn invalid_request() -> Self {
        Self(oauth2::basic::BasicErrorResponseType::InvalidRequest)
    }
    fn invalid_client() -> Self {
        Self(oauth2::basic::BasicErrorResponseType::InvalidClient)
    }
    fn invalid_grant() -> Self {
        Self(oauth2::basic::BasicErrorResponseType::InvalidGrant)
    }
    fn invalid_scope() -> Self {
        Self(oauth2::basic::BasicErrorResponseType::InvalidScope)
    }
}

impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let status = if matches!(&self.0, oauth2::basic::BasicErrorResponseType::Extension(s) if s == "server_error")
        {
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        } else {
            axum::http::StatusCode::BAD_REQUEST
        };
        (
            status,
            axum::Json(oauth2::basic::BasicErrorResponse::new(self.0, None, None)),
        )
            .into_response()
    }
}

impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        tracing::error!(?err, "OAuth database operation failed");
        Self(oauth2::basic::BasicErrorResponseType::Extension(
            "server_error".into(),
        ))
    }
}

fn config(app: &App) -> Result<&Config, Error> {
    app.oauth.as_ref().ok_or_else(|| {
        Error(oauth2::basic::BasicErrorResponseType::Extension(
            "temporarily_unavailable".into(),
        ))
    })
}

async fn discovery(
    axum::extract::State(app): axum::extract::State<App>,
) -> Result<axum::Json<serde_json::Value>, Error> {
    let issuer = config(&app)?.resource();
    Ok(axum::Json(serde_json::json!({
        "issuer": issuer,
        "authorization_endpoint": format!("{issuer}/oauth/authorize"),
        "token_endpoint": format!("{issuer}/oauth/token"),
        "response_types_supported": ["code"],
        "response_modes_supported": ["query"],
        "grant_types_supported": ["authorization_code"],
        "token_endpoint_auth_methods_supported": ["none"],
        "code_challenge_methods_supported": ["S256"],
        "scopes_supported": [SCOPE],
        "client_id_metadata_document_supported": true,
        "authorization_response_iss_parameter_supported": true
    })))
}

#[derive(serde::Deserialize)]
struct AuthorizationRequest {
    response_type: String,
    client_id: String,
    redirect_uri: String,
    code_challenge: String,
    code_challenge_method: String,
    scope: Option<String>,
    resource: Option<String>,
    state: Option<String>,
    response_mode: Option<String>,
}

fn valid_challenge(value: &str) -> bool {
    value.len() == 43
        && base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(value)
            .is_ok_and(|b| b.len() == 32)
}

async fn authorize(
    axum::extract::State(app): axum::extract::State<App>,
    query: Result<
        axum::extract::Query<AuthorizationRequest>,
        axum::extract::rejection::QueryRejection,
    >,
) -> Result<axum::response::Redirect, Error> {
    let config = config(&app)?;
    let axum::extract::Query(req) = query.map_err(|_| Error::invalid_request())?;
    // Never send errors to an unvalidated redirect URI. Locally rejecting the
    // entire malformed request also avoids reflecting unbounded state strings.
    if req.response_type != "code"
        || req.code_challenge_method != "S256"
        || !valid_challenge(&req.code_challenge)
        || req.response_mode.as_deref().is_some_and(|m| m != "query")
        || req.state.as_ref().is_some_and(|s| s.len() > 2048)
    {
        return Err(Error::invalid_request());
    }
    if req.scope.as_deref().is_some_and(|s| s != SCOPE) {
        return Err(Error::invalid_scope());
    }
    if req
        .resource
        .as_deref()
        .is_some_and(|r| r != config.resource())
    {
        return Err(Error(oauth2::basic::BasicErrorResponseType::Extension(
            "invalid_target".into(),
        )));
    }
    let metadata = client::resolve(config, &req.client_id).await?;
    if !metadata.redirect_uris.contains(&req.redirect_uri) {
        return Err(Error::invalid_request());
    }
    let id: uuid::Uuid = sqlx::query_scalar(
        "INSERT INTO internal.oauth_requests (client_id, client_name, redirect_uri, resource, scope, state, challenge)
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id"
    ).bind(&req.client_id).bind(&metadata.client_name).bind(&req.redirect_uri)
        .bind(config.resource()).bind(SCOPE).bind(&req.state).bind(&req.code_challenge)
        .fetch_one(&app.pg_pool).await?;
    let mut dashboard = config.dashboard.clone();
    dashboard
        .query_pairs_mut()
        .append_pair("request_id", &id.to_string());
    Ok(axum::response::Redirect::to(dashboard.as_str()))
}

#[derive(sqlx::FromRow)]
struct Pending {
    client_id: String,
    client_name: String,
    redirect_uri: String,
    resource: String,
    scope: String,
    state: Option<String>,
    challenge: String,
    user_id: Option<uuid::Uuid>,
    decision: Option<bool>,
}

async fn consent_user(env: &crate::Envelope) -> Result<uuid::Uuid, Error> {
    let claims = env.claims().map_err(|_| Error::invalid_grant())?;
    if claims.role != "authenticated"
        || claims.capability_mask.is_some()
        || claims.prefix_scope.is_some()
        || crate::server::is_service_account(&env.pg_pool, claims.sub).await?
    {
        return Err(Error::invalid_grant());
    }
    Ok(claims.sub)
}

async fn consent(
    axum::extract::State(app): axum::extract::State<App>,
    env: crate::Envelope,
    axum::extract::Path(id): axum::extract::Path<uuid::Uuid>,
) -> Result<axum::Json<serde_json::Value>, Error> {
    let config = config(&app)?;
    let user = consent_user(&env).await?;
    // Bind on first authenticated view so switching accounts or sharing a
    // dashboard URL cannot approve another user's in-progress consent.
    let row: Pending = sqlx::query_as(
        "UPDATE internal.oauth_requests SET user_id = $2
         WHERE id = $1 AND expires_at > clock_timestamp() AND (user_id IS NULL OR user_id = $2)
         AND decision IS NULL AND resource = $3 RETURNING *",
    )
    .bind(id)
    .bind(user)
    .bind(config.resource())
    .fetch_optional(&app.pg_pool)
    .await?
    .ok_or_else(Error::invalid_grant)?;
    Ok(axum::Json(serde_json::json!({
        "request_id": id, "client_id": row.client_id, "client_name": row.client_name,
        "redirect_uri": row.redirect_uri, "resource": row.resource, "scope": row.scope,
        "permissions": "Full access to Estuary as you, including all current and future grants, catalog changes, data access, and credential management. Access continues until revoked or the bearer credential expires after 30 days of inactivity."
    })))
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Decision {
    approve: bool,
}

fn callback(row: &Pending, issuer: &str, code: Option<&str>) -> String {
    let mut uri = url::Url::parse(&row.redirect_uri).expect("validated redirect URI");
    {
        let mut query = uri.query_pairs_mut();
        if let Some(code) = code {
            query.append_pair("code", code);
        } else {
            query.append_pair("error", "access_denied");
        }
        if let Some(state) = &row.state {
            query.append_pair("state", state);
        }
        query.append_pair("iss", issuer);
    }
    uri.into()
}

async fn decide(
    axum::extract::State(app): axum::extract::State<App>,
    env: crate::Envelope,
    axum::extract::Path(id): axum::extract::Path<uuid::Uuid>,
    axum::Json(req): axum::Json<Decision>,
) -> Result<axum::Json<serde_json::Value>, Error> {
    let config = config(&app)?;
    let user = consent_user(&env).await?;
    let mut tx = app.pg_pool.begin().await?;
    let row: Pending = sqlx::query_as(
        "SELECT * FROM internal.oauth_requests WHERE id = $1 AND user_id = $2 AND resource = $3 FOR UPDATE"
    ).bind(id).bind(user).bind(config.resource()).fetch_optional(&mut *tx).await?
        .ok_or_else(Error::invalid_grant)?;
    // Once an approval response is lost, restart authorization: retaining the
    // plaintext code to replay the response would defeat hashed-at-rest storage.
    if row.decision.is_some() {
        return Err(Error::invalid_grant());
    }
    // The OAuth library supplies 256 bits of cryptographic randomness.
    let code = req
        .approve
        .then(|| oauth2::CsrfToken::new_random_len(32).secret().clone());
    let hash = code
        .as_ref()
        .map(|code| sha2::Sha256::digest(code.as_bytes()).to_vec());
    let updated = sqlx::query(
        "UPDATE internal.oauth_requests SET decision = $2, code_hash = $3,
             code_expires_at = CASE WHEN $2 THEN clock_timestamp() + interval '1 minute' END
         WHERE id = $1 AND expires_at > clock_timestamp()",
    )
    .bind(id)
    .bind(req.approve)
    .bind(hash)
    .execute(&mut *tx)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(Error::invalid_grant());
    }
    let redirect_uri = callback(&row, config.resource(), code.as_deref());
    tx.commit().await?;
    Ok(axum::Json(
        serde_json::json!({ "redirect_uri": redirect_uri }),
    ))
}

#[derive(serde::Deserialize)]
struct TokenRequest {
    grant_type: String,
    client_id: String,
    code: Option<String>,
    redirect_uri: Option<String>,
    code_verifier: Option<String>,
    resource: Option<String>,
    scope: Option<String>,
    client_secret: Option<String>,
}

fn challenge(verifier: Option<&str>) -> Result<String, Error> {
    let verifier = verifier.ok_or_else(Error::invalid_request)?;
    if !(43..=128).contains(&verifier.len())
        || !verifier
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~'))
    {
        return Err(Error::invalid_grant());
    }
    Ok(
        oauth2::PkceCodeChallenge::from_code_verifier_sha256(&oauth2::PkceCodeVerifier::new(
            verifier.to_string(),
        ))
        .as_str()
        .to_string(),
    )
}

async fn token(
    axum::extract::State(app): axum::extract::State<App>,
    headers: axum::http::HeaderMap,
    form: Result<axum::Form<TokenRequest>, axum::extract::rejection::FormRejection>,
) -> Result<axum::Json<oauth2::basic::BasicTokenResponse>, Error> {
    let config = config(&app)?;
    let axum::Form(req) = form.map_err(|_| Error::invalid_request())?;
    if headers.contains_key("authorization")
        || req.client_secret.is_some()
        || req.client_id.is_empty()
        || req.client_id.len() > 2048
    {
        return Err(Error::invalid_client());
    }
    if req.scope.as_deref().is_some_and(|s| s != SCOPE) {
        return Err(Error::invalid_scope());
    }
    if req
        .resource
        .as_deref()
        .is_some_and(|r| r != config.resource())
    {
        return Err(Error(oauth2::basic::BasicErrorResponseType::Extension(
            "invalid_target".into(),
        )));
    }
    if req.grant_type != "authorization_code" {
        return Err(Error(
            oauth2::basic::BasicErrorResponseType::UnsupportedGrantType,
        ));
    }
    let credential = redeem(&app.pg_pool, config, &req).await?;
    // Estuary accepts this multi-use credential directly as a bearer. Each use
    // renews its inactivity window through the existing Envelope exchange path.
    let access = base64::engine::general_purpose::STANDARD
        .encode(serde_json::to_vec(&credential).expect("JSON credential"));
    let mut response = oauth2::basic::BasicTokenResponse::new(
        oauth2::AccessToken::new(access),
        oauth2::basic::BasicTokenType::Bearer,
        oauth2::EmptyExtraTokenFields {},
    );
    // Its lifetime slides with use, so there is no fixed expires_in or separate
    // OAuth refresh_token. The client reauthorizes after inactivity or revocation.
    response.set_scopes(Some(vec![oauth2::Scope::new(SCOPE.into())]));
    Ok(axum::Json(response))
}

async fn redeem(
    pool: &sqlx::PgPool,
    config: &Config,
    req: &TokenRequest,
) -> Result<serde_json::Value, Error> {
    let challenge = challenge(req.code_verifier.as_deref())?;
    let code = req.code.as_deref().ok_or_else(Error::invalid_request)?;
    let redirect_uri = req
        .redirect_uri
        .as_deref()
        .ok_or_else(Error::invalid_request)?;
    let hash = sha2::Sha256::digest(code.as_bytes()).to_vec();
    let mut tx = pool.begin().await?;
    let row: Pending =
        sqlx::query_as("SELECT * FROM internal.oauth_requests WHERE code_hash = $1 FOR UPDATE")
            .bind(&hash)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(Error::invalid_grant)?;
    // Validate every binding before consumption. Invalid guesses cannot burn
    // the code; minting failure rolls the consumption back with the transaction.
    if row.client_id != req.client_id
        || row.redirect_uri != redirect_uri
        || row.challenge != challenge
        || row.resource != config.resource()
        || row.scope != SCOPE
        || row.decision != Some(true)
    {
        return Err(Error::invalid_grant());
    }
    let consumed = sqlx::query(
        "UPDATE internal.oauth_requests SET code_hash = null, code_expires_at = null
         WHERE code_hash = $1 AND code_expires_at > clock_timestamp()",
    )
    .bind(&hash)
    .execute(&mut *tx)
    .await?;
    if consumed.rows_affected() != 1 {
        return Err(Error::invalid_grant());
    }
    let value = sqlx::query_scalar(
        "WITH secret AS (SELECT gen_random_uuid()::text AS value),
         token AS (
             INSERT INTO public.refresh_tokens (user_id, multi_use, valid_for, hash, detail)
             SELECT $1, true, interval '30 days', crypt(value, gen_salt('bf')), $2 FROM secret
             RETURNING id
         )
         SELECT json_build_object('id', token.id, 'secret', secret.value) FROM token, secret",
    )
    .bind(row.user_id.expect("approved request has user"))
    .bind(format!("OAuth: {} ({})", row.client_name, row.client_id))
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(value)
}

/// Called by the agent even while OAuth is disabled, to clean up an earlier rollout.
pub async fn cleanup(pool: sqlx::PgPool) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
    loop {
        interval.tick().await;
        if let Err(err) = sqlx::query("SELECT internal.cleanup_oauth_requests()")
            .execute(&pool)
            .await
        {
            tracing::error!(?err, "OAuth expired-record cleanup failed");
        }
    }
}
