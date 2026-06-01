use std::sync::Arc;

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(tag = "grant_type")]
pub enum TokenRequest {
    #[serde(rename = "api_key")]
    ApiKey { api_key: String },
    #[serde(rename = "refresh_token")]
    RefreshToken {
        refresh_token_id: models::Id,
        secret: String,
    },
}

#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
pub struct TokenResponse {
    pub access_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<RefreshTokenResponse>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct RefreshTokenResponse {
    pub id: models::Id,
    pub secret: String,
}

pub async fn handle_post_token(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    axum::Json(req): axum::Json<TokenRequest>,
) -> Result<axum::Json<TokenResponse>, crate::ApiError> {
    match req {
        TokenRequest::ApiKey { api_key } => exchange_api_key(&app, &api_key).await,
        TokenRequest::RefreshToken {
            refresh_token_id,
            secret,
        } => exchange_refresh_token(&app, refresh_token_id, &secret).await,
    }
}

async fn exchange_api_key(
    app: &crate::App,
    api_key: &str,
) -> Result<axum::Json<TokenResponse>, crate::ApiError> {
    let raw = api_key
        .strip_prefix("flow_sa_")
        .ok_or_else(|| bad_request("api_key must start with flow_sa_"))?;

    use base64::Engine;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| bad_request("malformed api_key: invalid base64"))?;

    let decoded_str =
        String::from_utf8(decoded).map_err(|_| bad_request("malformed api_key: invalid UTF-8"))?;

    let (id_str, plaintext_secret) = decoded_str
        .split_once(':')
        .ok_or_else(|| bad_request("malformed api_key payload"))?;

    let key_id: models::Id = id_str
        .parse()
        .map_err(|_| bad_request("malformed api_key: invalid key id"))?;

    // Verify the key against bcrypt hash and check expiry in one query.
    // disabled_at is returned separately so we can give a distinct error.
    let row = sqlx::query!(
        r#"
        SELECT
            ak.service_account_id,
            sa.disabled_at
        FROM internal.api_keys ak
        JOIN internal.service_accounts sa ON sa.user_id = ak.service_account_id
        WHERE ak.id = $1
            AND ak.secret_hash = crypt($2, ak.secret_hash)
            AND ak.expires_at > now()
        "#,
        key_id as models::Id,
        plaintext_secret,
    )
    .fetch_optional(&app.pg_pool)
    .await?
    .ok_or_else(|| unauthenticated("invalid or expired api key"))?;

    if row.disabled_at.is_some() {
        return Err(unauthenticated("service account is disabled"));
    }

    // Stamp last_used_at on both the key and the service account. This is
    // best-effort telemetry: the key has already verified, so a failure here
    // must not deny the caller a token it's entitled to. Log and continue.
    if let Err(err) = sqlx::query!(
        r#"
        WITH touch_key AS (
            UPDATE internal.api_keys SET last_used_at = now() WHERE id = $1
        )
        UPDATE internal.service_accounts SET last_used_at = now() WHERE user_id = $2
        "#,
        key_id as models::Id,
        row.service_account_id,
    )
    .execute(&app.pg_pool)
    .await
    {
        tracing::warn!(
            ?err,
            %key_id,
            service_account_id = %row.service_account_id,
            "failed to update last_used_at after api key exchange"
        );
    }

    // Mint the access token directly in the application layer. This is the
    // canonical token-minting path: the plan is to retire the SQL
    // `generate_access_token` function and have all access tokens minted here.
    // Until then, the refresh-token branch below still delegates to SQL for
    // existing PostgREST callers, so any change to the access-token claim
    // shape must be made here (not in the SQL function, which is frozen).
    let now = tokens::now();
    let claims = models::authorizations::ControlClaims {
        iat: now.timestamp() as u64,
        exp: (now + chrono::Duration::hours(1)).timestamp() as u64,
        sub: row.service_account_id,
        role: "authenticated".to_string(),
        aud: "authenticated".to_string(),
        email: None,
    };

    let access_token =
        tokens::jwt::sign(&claims, &app.control_plane_jwt_encode_key).map_err(|err| {
            tracing::error!(?err, "failed to sign access token during api key exchange");
            crate::ApiError::Status(tonic::Status::internal("failed to issue access token"))
        })?;

    tracing::info!(
        %key_id,
        service_account_id = %row.service_account_id,
        "exchanged api key for access token"
    );

    Ok(axum::Json(TokenResponse {
        access_token,
        refresh_token: None,
    }))
}

// Exchange a refresh token for an access token.
//
// This delegates to the SQL `generate_access_token` function transitionally:
// existing clients (flowctl via flow-client) still authenticate against the
// PostgREST `/rpc/generate_access_token` surface, so the function must keep
// working unchanged. The plan is to migrate those callers onto this endpoint
// and then retire the SQL function, folding refresh-token minting into the
// application-layer path used by `exchange_api_key`. New clients should target
// this endpoint rather than PostgREST.
async fn exchange_refresh_token(
    app: &crate::App,
    refresh_token_id: models::Id,
    secret: &str,
) -> Result<axum::Json<TokenResponse>, crate::ApiError> {
    #[derive(Debug, serde::Deserialize)]
    struct SqlResponse {
        access_token: String,
        refresh_token: Option<RefreshTokenResponse>,
    }

    let response = sqlx::query!(
        "select generate_access_token($1, $2) as token",
        refresh_token_id as models::Id,
        secret,
    )
    .fetch_one(&app.pg_pool)
    .await
    .map_err(|err| {
        crate::ApiError::Status(tonic::Status::unauthenticated(format!(
            "failed to exchange refresh token: {err}"
        )))
    })?;

    let parsed: SqlResponse = serde_json::from_value(response.token.unwrap_or_default())
        .map_err(|err| {
            tracing::error!(?err, "generate_access_token returned an unparseable response");
            crate::ApiError::Status(tonic::Status::internal("invalid token response"))
        })?;

    Ok(axum::Json(TokenResponse {
        access_token: parsed.access_token,
        refresh_token: parsed.refresh_token,
    }))
}

fn bad_request(msg: &str) -> crate::ApiError {
    crate::ApiError::Status(tonic::Status::invalid_argument(msg))
}

fn unauthenticated(msg: &str) -> crate::ApiError {
    crate::ApiError::Status(tonic::Status::unauthenticated(msg))
}
