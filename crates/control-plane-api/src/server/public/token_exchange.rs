use std::sync::Arc;

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(tag = "grant_type")]
pub enum TokenRequest {
    #[serde(rename = "refresh_token")]
    RefreshToken {
        refresh_token_id: models::Id,
        secret: String,
    },
    #[serde(rename = "api_key")]
    ApiKey {
        // The full `flow_sa_...` service-account API key.
        api_key: String,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct TokenResponse {
    pub access_token: String,
    // `generate_access_token` omits this for multi-use tokens (no rotation),
    // so it must default to `None` when absent from the SQL JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
        TokenRequest::RefreshToken {
            refresh_token_id,
            secret,
        } => exchange_refresh_token(&app, refresh_token_id, &secret).await,
        TokenRequest::ApiKey { api_key } => exchange_api_key(&app, &api_key).await,
    }
}

// Exchange a service-account API key for a short-lived access token. This is
// the standard way to authenticate a service account: present the long-lived
// key once, then carry the signed token. Presenting the key directly as an
// `Authorization: Bearer` credential is also supported, as a convenience for
// one-off requests; that path mints the same token internally, so both route
// through `server::exchange_api_key`.
async fn exchange_api_key(
    app: &crate::App,
    api_key: &str,
) -> Result<axum::Json<TokenResponse>, crate::ApiError> {
    // `verify_api_key` dispatches on this prefix via `expect`; reject a
    // non-key here so a malformed request is a 400 rather than a panic.
    if !api_key.starts_with("flow_sa_") {
        return Err(crate::ApiError::Status(tonic::Status::invalid_argument(
            "api_key must be a flow_sa_ service-account key",
        )));
    }

    let access_token =
        crate::server::exchange_api_key(&app.pg_pool, &app.control_plane_jwt_encode_key, api_key)
            .await?;

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
// and then retire the SQL function, folding refresh-token minting into an
// application-layer path. New clients should target this endpoint rather
// than PostgREST.
async fn exchange_refresh_token(
    app: &crate::App,
    refresh_token_id: models::Id,
    secret: &str,
) -> Result<axum::Json<TokenResponse>, crate::ApiError> {
    let response = sqlx::query!(
        "select generate_access_token($1, $2) as token",
        refresh_token_id as models::Id,
        secret,
    )
    .fetch_one(&app.pg_pool)
    .await
    .map_err(|err| {
        // `generate_access_token` signals an unusable credential (unknown id,
        // bad secret, or expired token) by `raise`-ing, which surfaces as
        // SQLSTATE P0001. Those are the only legitimate 401s, and we collapse
        // them into a single generic message so the response doesn't reveal
        // which check failed. Any other error is an internal fault: log the
        // detail and return 500.
        //
        // This will change again when we retire generate_access_token and implement the logic in the application.
        if err.as_database_error().and_then(|e| e.code()).as_deref() == Some("P0001") {
            crate::ApiError::Status(tonic::Status::unauthenticated(
                "invalid, expired, or unknown refresh token",
            ))
        } else {
            tracing::error!(?err, "failed to exchange refresh token");
            crate::ApiError::Status(tonic::Status::internal("failed to exchange refresh token"))
        }
    })?;

    let parsed: TokenResponse = serde_json::from_value(response.token.unwrap_or_default())
        .map_err(|err| {
            tracing::error!(
                ?err,
                "generate_access_token returned an unparseable response"
            );
            crate::ApiError::Status(tonic::Status::internal("invalid token response"))
        })?;

    Ok(axum::Json(parsed))
}
