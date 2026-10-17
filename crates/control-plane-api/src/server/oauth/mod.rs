//! OAuth authorization server owned by the control plane. See README.md.
use axum::response::IntoResponse;
use base64::Engine;
use sha2::Digest;
use std::sync::Arc;

pub mod clients;
#[cfg(test)]
mod tests;

pub struct Config {
    pub issuer: String,
    pub consent_url: String,
    pub resources: Vec<String>,
    pub clients: Vec<clients::Client>,
    mcp_service_secrets: Vec<String>,
}

impl Config {
    pub fn new(
        issuer: String,
        consent_url: String,
        resources: Vec<String>,
        clients: Vec<clients::Client>,
    ) -> anyhow::Result<Self> {
        let issuer = issuer.trim_end_matches('/').to_string();
        for value in std::iter::once(&issuer)
            .chain(std::iter::once(&consent_url))
            .chain(resources.iter())
        {
            let url = url::Url::parse(value)?;
            anyhow::ensure!(
                url.host_str().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.fragment().is_none()
                    && url.query().is_none()
                    && (url.scheme() == "https"
                        || (url.scheme() == "http"
                            && matches!(
                                url.host_str(),
                                Some("localhost" | "127.0.0.1" | "[::1]")
                            ))),
                "OAuth URLs require HTTPS (HTTP loopback is allowed for development)"
            );
        }
        anyhow::ensure!(
            url::Url::parse(&issuer)?.path() == "/",
            "OAuth issuer must be an origin"
        );
        anyhow::ensure!(
            !resources.is_empty(),
            "OAuth requires at least one resource URI"
        );
        let mut ids = std::collections::HashSet::new();
        for client in &clients {
            clients::validate(client)
                .map_err(|_| anyhow::anyhow!("invalid configured OAuth client"))?;
            anyhow::ensure!(ids.insert(&client.client_id), "duplicate OAuth client_id");
        }
        Ok(Self {
            issuer,
            consent_url,
            resources,
            clients,
            mcp_service_secrets: Vec::new(),
        })
    }

    /// Accept overlapping secrets while MCP deployments rotate credentials.
    pub fn with_mcp_service_secrets(mut self, secrets: Vec<String>) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !secrets.is_empty()
                && secrets.iter().all(|secret| {
                    secret.len() >= 32 && secret.bytes().all(|b| b.is_ascii_graphic() && b != b',')
                }),
            "OAUTH_MCP_SERVICE_SECRETS requires secrets of at least 32 printable ASCII characters without commas"
        );
        self.mcp_service_secrets = secrets;
        Ok(self)
    }
}

pub fn router() -> axum::Router<Arc<crate::App>> {
    axum::Router::new()
        .route(
            "/.well-known/oauth-authorization-server",
            axum::routing::get(metadata),
        )
        .route("/oauth/authorize", axum::routing::get(authorize))
        .route(
            "/oauth/consent-context",
            axum::routing::get(consent_context),
        )
        .route("/oauth/consent", axum::routing::post(consent))
        .route("/oauth/token", axum::routing::post(token))
        .route("/oauth/revoke", axum::routing::post(revoke))
        .route("/oauth/grants", axum::routing::get(grants))
        .route("/oauth/grants/{id}", axum::routing::delete(revoke_grant))
        .layer(axum::extract::DefaultBodyLimit::max(16 * 1024))
}

type Result<T> = std::result::Result<T, Error>;

pub struct Error {
    status: axum::http::StatusCode,
    code: &'static str,
    description: &'static str,
}
impl Error {
    fn invalid(description: &'static str) -> Self {
        Self::new("invalid_request", description)
    }
    fn new(code: &'static str, description: &'static str) -> Self {
        Self {
            status: axum::http::StatusCode::BAD_REQUEST,
            code,
            description,
        }
    }
    fn grant() -> Self {
        Self::new(
            "invalid_grant",
            "credential is invalid, expired, revoked, or already used",
        )
    }
    fn internal() -> Self {
        Self {
            status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            code: "server_error",
            description: "OAuth request failed",
        }
    }
}
impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        tracing::error!(?err, "OAuth database operation failed");
        Self::internal()
    }
}
impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let mut response =
            json(serde_json::json!({"error": self.code, "error_description": self.description}));
        *response.status_mut() = self.status;
        response
    }
}
fn json(value: serde_json::Value) -> axum::response::Response {
    let mut response = axum::Json(value).into_response();
    let headers = response.headers_mut();
    headers.insert("Cache-Control", "no-store".parse().unwrap());
    headers.insert("Pragma", "no-cache".parse().unwrap());
    headers.insert("Referrer-Policy", "no-referrer".parse().unwrap());
    response
}
fn config(app: &crate::App) -> Result<&Config> {
    app.oauth.as_deref().ok_or(Error {
        status: axum::http::StatusCode::SERVICE_UNAVAILABLE,
        code: "temporarily_unavailable",
        description: "OAuth is not configured",
    })
}
fn secret() -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(rand::random::<[u8; 32]>())
}
fn hash(value: &str) -> Vec<u8> {
    sha2::Sha256::digest(value.as_bytes()).to_vec()
}
fn pkce(verifier: &str, challenge: &str) -> bool {
    (43..=128).contains(&verifier.len())
        && verifier
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-._~".contains(&b))
        && base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash(verifier)) == challenge
}
async fn metadata(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
) -> Result<axum::response::Response> {
    let cfg = config(&app)?;
    Ok(json(serde_json::json!({
        "issuer": cfg.issuer,
        "authorization_endpoint": format!("{}/oauth/authorize", cfg.issuer),
        "token_endpoint": format!("{}/oauth/token", cfg.issuer),
        "revocation_endpoint": format!("{}/oauth/revoke", cfg.issuer),
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "token_endpoint_auth_methods_supported": ["none"],
        "revocation_endpoint_auth_methods_supported": ["none"],
        "code_challenge_methods_supported": ["S256"],
        "client_id_metadata_document_supported": true,
        "authorization_response_iss_parameter_supported": true
    })))
}

#[derive(serde::Deserialize)]
struct Authorization {
    client_id: String,
    redirect_uri: String,
    response_type: String,
    code_challenge: String,
    code_challenge_method: String,
    resource: String,
    state: Option<String>,
}

async fn authorize(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    query: std::result::Result<
        axum::extract::Query<Authorization>,
        axum::extract::rejection::QueryRejection,
    >,
) -> Result<axum::response::Response> {
    let cfg = config(&app)?;
    let axum::extract::Query(req) =
        query.map_err(|_| Error::invalid("malformed authorization request"))?;
    if req.client_id.len() > 2048 || req.state.as_ref().is_some_and(|v| v.len() > 2048) {
        return Err(Error::invalid("authorization parameter too long"));
    }
    let client = clients::resolve(&req.client_id, &cfg.clients).await?;
    if !clients::redirect_allowed(&req.redirect_uri, &client) {
        return Err(Error::invalid(
            "redirect_uri is not registered for this client",
        ));
    }
    let failure = if req.response_type != "code" {
        Some("unsupported_response_type")
    } else if req.code_challenge_method != "S256"
        || req.code_challenge.len() != 43
        || base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&req.code_challenge)
            .map_or(true, |v| v.len() != 32)
    {
        Some("invalid_request")
    } else if !cfg.resources.contains(&req.resource) {
        Some("invalid_target")
    } else {
        None
    };
    if let Some(error) = failure {
        return Ok(redirect(callback(
            &req.redirect_uri,
            req.state.as_deref(),
            &cfg.issuer,
            "error",
            error,
        )?));
    }
    let request_id = secret();
    let mut tx = app.pg_pool.begin().await?;
    // Requests and codes have short lifetimes. Grants retain spent refresh
    // hashes for replay detection until their absolute ninety-day expiry.
    sqlx::query("DELETE FROM internal.oauth_requests WHERE expires_at < now()")
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM internal.oauth_codes WHERE expires_at < now()")
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM internal.oauth_grants WHERE expires_at < now()")
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO internal.oauth_requests (request_hash, client_id, client_name, redirect_uri, client_state, code_challenge, resource) VALUES ($1,$2,$3,$4,$5,$6,$7)")
        .bind(hash(&request_id)).bind(&req.client_id).bind(&client.client_name)
        .bind(&req.redirect_uri).bind(&req.state).bind(&req.code_challenge).bind(&req.resource)
        .execute(&mut *tx).await?;
    tx.commit().await?;
    let mut target = url::Url::parse(&cfg.consent_url).map_err(|_| Error::internal())?;
    target
        .query_pairs_mut()
        .append_pair("request", &request_id)
        .append_pair("issuer", &cfg.issuer);
    Ok(redirect(target.to_string()))
}

fn callback(
    uri: &str,
    state: Option<&str>,
    issuer: &str,
    key: &str,
    value: &str,
) -> Result<String> {
    let mut url = url::Url::parse(uri).map_err(|_| Error::internal())?;
    url.query_pairs_mut()
        .append_pair(key, value)
        .append_pair("iss", issuer);
    if let Some(state) = state {
        url.query_pairs_mut().append_pair("state", state);
    }
    Ok(url.to_string())
}
fn redirect(url: String) -> axum::response::Response {
    let mut response = axum::response::Redirect::to(&url).into_response();
    *response.status_mut() = axum::http::StatusCode::FOUND;
    response
        .headers_mut()
        .insert("Cache-Control", "no-store".parse().unwrap());
    response
        .headers_mut()
        .insert("Referrer-Policy", "no-referrer".parse().unwrap());
    response
}

async fn session(env: &crate::Envelope) -> Result<&crate::ControlClaims> {
    let claims = env.claims().map_err(|_| Error {
        status: axum::http::StatusCode::UNAUTHORIZED,
        code: "invalid_token",
        description: "a dashboard session is required",
    })?;
    if claims.aud != "authenticated"
        || claims.role != "authenticated"
        || claims.capability_mask.is_some()
        || claims.prefix_scope.is_some()
        || crate::server::is_service_account(&env.pg_pool, claims.sub).await?
    {
        return Err(Error {
            status: axum::http::StatusCode::FORBIDDEN,
            code: "access_denied",
            description: "an unrestricted user session is required",
        });
    }
    Ok(claims)
}

#[derive(serde::Deserialize)]
struct RequestId {
    request: String,
}
// Values here are validated before insertion and never sourced from dashboard text.
type Pending = (String, String, String, Option<String>, String, String);
async fn consent_context(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    env: crate::Envelope,
    axum::extract::Query(req): axum::extract::Query<RequestId>,
) -> Result<axum::response::Response> {
    config(&app)?;
    session(&env).await?;
    let pending: Option<Pending> = sqlx::query_as("SELECT client_id, client_name, redirect_uri, client_state, code_challenge, resource FROM internal.oauth_requests WHERE request_hash=$1 AND expires_at > now()")
        .bind(hash(&req.request)).fetch_optional(&app.pg_pool).await?;
    let (client_id, client_name, _, _, _, resource) = pending.ok_or_else(Error::grant)?;
    let client_host = url::Url::parse(&client_id)
        .ok()
        .and_then(|u| u.host_str().map(str::to_owned));
    Ok(json(
        serde_json::json!({"client_id":client_id, "client_name":client_name, "client_host":client_host, "resource":resource}),
    ))
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Consent {
    request: String,
    approve: bool,
}
async fn consent(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    env: crate::Envelope,
    axum::Json(req): axum::Json<Consent>,
) -> Result<axum::response::Response> {
    let cfg = config(&app)?;
    let user = session(&env).await?.sub;
    let mut tx = app.pg_pool.begin().await?;
    let pending: Option<Pending> = sqlx::query_as("DELETE FROM internal.oauth_requests WHERE request_hash=$1 AND expires_at > now() RETURNING client_id, client_name, redirect_uri, client_state, code_challenge, resource")
        .bind(hash(&req.request)).fetch_optional(&mut *tx).await?;
    let (client_id, client_name, redirect_uri, state, challenge, resource) =
        pending.ok_or_else(Error::grant)?;
    let location = if req.approve {
        let id = uuid::Uuid::new_v4();
        sqlx::query("INSERT INTO internal.oauth_grants (id,user_id,client_id,client_name,resource) VALUES ($1,$2,$3,$4,$5)")
            .bind(id).bind(user).bind(client_id).bind(client_name).bind(resource)
            .execute(&mut *tx).await?;
        let code = secret();
        sqlx::query("INSERT INTO internal.oauth_codes (code_hash,grant_id,redirect_uri,code_challenge) VALUES ($1,$2,$3,$4)")
            .bind(hash(&code)).bind(id).bind(&redirect_uri).bind(challenge).execute(&mut *tx).await?;
        callback(&redirect_uri, state.as_deref(), &cfg.issuer, "code", &code)?
    } else {
        callback(
            &redirect_uri,
            state.as_deref(),
            &cfg.issuer,
            "error",
            "access_denied",
        )?
    };
    tx.commit().await?;
    // The authenticated fetch returns a URL for the dashboard to navigate to.
    // Fetch-followed redirects would not deliver the code to the user's client.
    Ok(json(serde_json::json!({"redirect_uri": location})))
}

#[derive(serde::Deserialize)]
struct TokenRequest {
    grant_type: String,
    client_id: String,
    code: Option<String>,
    redirect_uri: Option<String>,
    code_verifier: Option<String>,
    refresh_token: Option<String>,
    resource: Option<String>,
}
type Grant = (uuid::Uuid, uuid::Uuid, String, String);

async fn token(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    form: std::result::Result<axum::Form<TokenRequest>, axum::extract::rejection::FormRejection>,
) -> Result<axum::response::Response> {
    let cfg = config(&app)?;
    let axum::Form(req) = form.map_err(|_| Error::invalid("malformed token request"))?;
    let is_code = match req.grant_type.as_str() {
        "authorization_code" => true,
        "refresh_token" => false,
        _ => {
            return Err(Error::new(
                "unsupported_grant_type",
                "unsupported grant type",
            ));
        }
    };
    let credential = if is_code {
        req.code.as_deref()
    } else {
        req.refresh_token.as_deref()
    }
    .filter(|v| v.len() == 43)
    .ok_or_else(Error::grant)?;
    if is_code && (req.code_verifier.is_none() || req.redirect_uri.is_none()) {
        return Err(Error::invalid(
            "redirect_uri and code_verifier are required",
        ));
    }
    let digest = hash(credential);
    let mut tx = app.pg_pool.begin().await?;
    let lookup = if is_code {
        "SELECT g.id,g.user_id,g.client_id,g.resource FROM internal.oauth_grants g JOIN internal.oauth_codes c ON c.grant_id=g.id WHERE c.code_hash=$1 AND g.revoked_at IS NULL AND g.expires_at > now() FOR UPDATE OF g"
    } else {
        "SELECT g.id,g.user_id,g.client_id,g.resource FROM internal.oauth_grants g JOIN internal.oauth_refresh_tokens t ON t.grant_id=g.id WHERE t.token_hash=$1 AND g.revoked_at IS NULL AND g.expires_at > now() FOR UPDATE OF g"
    };
    let grant: Option<Grant> = sqlx::query_as(lookup)
        .bind(&digest)
        .fetch_optional(&mut *tx)
        .await?;
    let (id, user, client_id, resource) = grant.ok_or_else(Error::grant)?;
    if client_id != req.client_id
        || req.resource.as_ref().is_some_and(|v| v != &resource)
        || !cfg.resources.contains(&resource)
    {
        return Err(Error::grant());
    }
    // Read credential state after acquiring the grant lock. Separate statements
    // see a preceding concurrent redemption's committed update under READ COMMITTED.
    let used = if is_code {
        let code: Option<(String, String, bool)> = sqlx::query_as("SELECT redirect_uri,code_challenge,used FROM internal.oauth_codes WHERE code_hash=$1 AND expires_at > now()")
            .bind(&digest).fetch_optional(&mut *tx).await?;
        let (uri, challenge, used) = code.ok_or_else(Error::grant)?;
        if req.redirect_uri.as_deref() != Some(&uri)
            || !pkce(req.code_verifier.as_deref().unwrap(), &challenge)
        {
            return Err(Error::grant());
        }
        used
    } else {
        sqlx::query_scalar::<_, bool>(
            "SELECT used FROM internal.oauth_refresh_tokens WHERE token_hash=$1",
        )
        .bind(&digest)
        .fetch_one(&mut *tx)
        .await?
    };
    if used {
        sqlx::query("UPDATE internal.oauth_grants SET revoked_at=now() WHERE id=$1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Err(Error::grant());
    }
    let mark = if is_code {
        "UPDATE internal.oauth_codes SET used=true WHERE code_hash=$1"
    } else {
        "UPDATE internal.oauth_refresh_tokens SET used=true WHERE token_hash=$1"
    };
    sqlx::query(mark).bind(&digest).execute(&mut *tx).await?;
    let refresh = secret();
    sqlx::query("INSERT INTO internal.oauth_refresh_tokens (token_hash,grant_id) VALUES ($1,$2)")
        .bind(hash(&refresh))
        .bind(id)
        .execute(&mut *tx)
        .await?;
    let now = tokens::now().timestamp() as u64;
    let claims = crate::ControlClaims {
        aud: resource,
        sub: user,
        iat: now,
        exp: now + 3600,
        role: "postgrest_cant_use_this_token".into(),
        email: None,
        capability_mask: None,
        prefix_scope: None,
    };
    #[derive(serde::Serialize)]
    struct Access<'a> {
        #[serde(flatten)]
        claims: crate::ControlClaims,
        iss: &'a str,
        client_id: &'a str,
    }
    let access = tokens::jwt::sign(
        Access {
            claims,
            iss: &cfg.issuer,
            client_id: &client_id,
        },
        &app.control_plane_jwt_encode_key,
    )
    .map_err(|_| Error::internal())?;
    tx.commit().await?;
    let response = serde_json::json!({"access_token":access, "token_type":"Bearer", "expires_in":3600, "refresh_token":refresh});
    Ok(json(response))
}

#[derive(serde::Deserialize)]
struct Revocation {
    token: String,
    client_id: String,
}
async fn revoke(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    form: std::result::Result<axum::Form<Revocation>, axum::extract::rejection::FormRejection>,
) -> Result<axum::response::Response> {
    config(&app)?;
    let axum::Form(req) = form.map_err(|_| Error::invalid("malformed revocation request"))?;
    sqlx::query("UPDATE internal.oauth_grants SET revoked_at=now() WHERE client_id=$1 AND id IN (SELECT grant_id FROM internal.oauth_refresh_tokens WHERE token_hash=$2)")
        .bind(req.client_id).bind(hash(&req.token)).execute(&app.pg_pool).await?;
    // RFC 7009: unknown tokens and already-revoked tokens also succeed.
    Ok(json(serde_json::json!({})))
}

async fn grants(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    env: crate::Envelope,
) -> Result<axum::response::Response> {
    config(&app)?;
    let user = session(&env).await?.sub;
    let rows: Vec<serde_json::Value> = sqlx::query_scalar("SELECT jsonb_build_object('id',id,'client_id',client_id,'client_name',client_name,'resource',resource,'created_at',created_at,'expires_at',expires_at) FROM internal.oauth_grants WHERE user_id=$1 AND revoked_at IS NULL AND expires_at > now() ORDER BY created_at DESC")
        .bind(user).fetch_all(&app.pg_pool).await?;
    Ok(json(serde_json::json!({"grants":rows})))
}
async fn revoke_grant(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    env: crate::Envelope,
    axum::extract::Path(id): axum::extract::Path<uuid::Uuid>,
) -> Result<axum::response::Response> {
    config(&app)?;
    let user = session(&env).await?.sub;
    sqlx::query("UPDATE internal.oauth_grants SET revoked_at=now() WHERE id=$1 AND user_id=$2")
        .bind(id)
        .bind(user)
        .execute(&app.pg_pool)
        .await?;
    Ok(json(serde_json::json!({})))
}

/// The MCP adapter and control plane jointly serve the configured resources.
pub(crate) fn accepts_access_token(
    app: &crate::App,
    claims: &crate::ControlClaims,
    token: &str,
    headers: &axum::http::HeaderMap,
) -> bool {
    let Some(cfg) = app.oauth.as_deref() else {
        return false;
    };
    let mut supplied = headers.get_all("x-mcp-service-secret").iter();
    let Some(supplied_secret) = supplied.next() else {
        return false;
    };
    if supplied.next().is_some() {
        return false;
    }
    // Compare fixed-size MACs in constant time, including for different secret lengths.
    // HMAC provides this comparison without implementing our own crypto primitive.
    use hmac::Mac;
    let mut candidate = hmac::Hmac::<sha2::Sha256>::new_from_slice(supplied_secret.as_bytes())
        .expect("HMAC accepts any key length");
    candidate.update(b"estuary-mcp-service-auth");
    let candidate = candidate.finalize().into_bytes();
    let mut matches = false;
    for secret in &cfg.mcp_service_secrets {
        let mut expected = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes())
            .expect("HMAC accepts any key length");
        expected.update(b"estuary-mcp-service-auth");
        matches |= expected.verify_slice(&candidate).is_ok();
    }
    matches
        && cfg.resources.contains(&claims.aud)
        && claims.role == "postgrest_cant_use_this_token"
        && tokens::jwt::parse_unverified::<serde_json::Value>(token.as_bytes())
            .is_ok_and(|v| v.claims()["iss"].as_str() == Some(cfg.issuer.as_str()))
}
