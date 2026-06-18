use std::sync::Arc;

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(tag = "grant_type")]
pub enum TokenRequest {
    #[serde(rename = "refresh_token")]
    RefreshToken {
        refresh_token_id: models::Id,
        secret: String,
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
        } => {
            let response = generate_access_token(&app.pg_pool, refresh_token_id, &secret).await?;
            Ok(axum::Json(response))
        }
    }
}

// Exchange a refresh token for an access token by calling the SQL
// `generate_access_token` function and returning its parsed response.
//
// Shared by the `POST /api/v1/auth/token` endpoint (above) and the
// bearer-credential authentication path
// (`crate::server::exchange_refresh_token`), so the credential-error
// sanitization below lives in exactly one place rather than being duplicated —
// and kept in sync — across both.
//
// The SQL delegation is transitional: existing clients (flowctl via
// flow-client) still authenticate against the PostgREST
// `/rpc/generate_access_token` surface, so the function must keep working
// unchanged. The plan is to migrate those callers onto this endpoint and then
// retire the SQL function, folding refresh-token minting into an
// application-layer path. New clients should target this endpoint rather than
// PostgREST.
pub(crate) async fn generate_access_token(
    pg_pool: &sqlx::PgPool,
    refresh_token_id: models::Id,
    secret: &str,
) -> tonic::Result<TokenResponse> {
    let response = sqlx::query!(
        "select generate_access_token($1, $2) as token",
        refresh_token_id as models::Id,
        secret,
    )
    .fetch_one(pg_pool)
    .await
    .map_err(|err| {
        // `generate_access_token` signals an unusable credential (unknown id,
        // bad secret, or expired/revoked token) by `raise`-ing, which surfaces
        // as SQLSTATE P0001. Those are the only legitimate 401s, and we collapse
        // them into a single generic message so the response neither reveals
        // which check failed nor leaks the raw DB error. Any other error is an
        // internal fault: log the detail and return 500.
        if err.as_database_error().and_then(|e| e.code()).as_deref() == Some("P0001") {
            tonic::Status::unauthenticated("invalid, expired, or unknown credential")
        } else {
            tracing::error!(?err, "failed to exchange refresh token");
            tonic::Status::internal("failed to exchange refresh token")
        }
    })?;

    serde_json::from_value(response.token.unwrap_or_default()).map_err(|err| {
        tracing::error!(
            ?err,
            "generate_access_token returned an unparseable response"
        );
        tonic::Status::internal("invalid token response")
    })
}
