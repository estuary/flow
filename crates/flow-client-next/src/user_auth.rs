use tokens::TimeDelta;

/// RefreshToken is the structure of an Estuary user refresh token.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct RefreshToken {
    pub id: models::Id,
    pub secret: String,
}

/// UserTokens composes a pair of access and refresh tokens.
#[derive(Clone, Default)]
pub struct UserToken {
    pub access_token: Option<String>,
    pub refresh_token: Option<RefreshToken>,
}

impl UserToken {
    pub fn access_ref(&self) -> Option<&str> {
        self.access_token.as_ref().map(String::as_str)
    }
}

/// UserTokenSource is a crate::token::Source which emits UserTokens.
/// It continues from existing `tokens`, creating or exchanging tokens as needed.
pub struct UserTokenSource {
    pub pg_client: postgrest::Postgrest,
    pub tokens: UserToken,
}

impl tokens::Source for UserTokenSource {
    type Token = UserToken;
    type Revoke = std::future::Pending<()>;

    async fn refresh(
        &mut self,
        _started: tokens::DateTime,
    ) -> tonic::Result<Result<(Self::Token, TimeDelta, Self::Revoke), TimeDelta>> {
        // Map a Some(access_token) into Some((access_token, valid_for)).
        let access_token = if let Some(token) = &self.tokens.access_token {
            let unverified =
                tokens::jwt::parse_unverified::<serde::de::IgnoredAny>(token.as_bytes())?;
            Some((token, unverified.valid_for()))
        } else {
            None
        };

        let valid_for = match (access_token, &self.tokens.refresh_token) {
            // No access or refresh token.
            (None, None) => TimeDelta::MAX,

            // If `valid_for` is at least a minute in the future, then return it.
            // Common case when resuming from a recent, saved configuration.
            (Some((_, valid_for)), Some(_)) if valid_for > TimeDelta::minutes(1) => valid_for,

            // We have an access token but no refresh token. Create one.
            (Some((access_token, _valid_for)), None) => {
                let (refresh_token, access_token, valid_for) =
                    create_refresh_token(&self.pg_client, access_token).await?;

                self.tokens = UserToken {
                    access_token: Some(access_token),
                    refresh_token: Some(refresh_token),
                };
                valid_for
            }

            // We have no access token, or it's expiring soon. Generate a new one.
            (_maybe_access_token, Some(refresh_token)) => {
                let (refresh_token, access_token, valid_for) =
                    exchange_refresh_token(&self.pg_client, refresh_token).await?;

                self.tokens = UserToken {
                    access_token: Some(access_token),
                    refresh_token: Some(refresh_token),
                };
                valid_for
            }
        };

        Ok(Ok((self.tokens.clone(), valid_for, std::future::pending())))
    }
}

pub async fn create_refresh_token(
    client: &postgrest::Postgrest,
    access_token: &str,
) -> tonic::Result<(RefreshToken, String, TimeDelta)> {
    let refresh_token = crate::postgrest::exec::<RefreshToken>(
        client.rpc(
            "create_refresh_token",
            serde_json::json!({
                "multi_use": true,
                "valid_for": "90d",
                "detail": "Created by flow-client",
            })
            .to_string(),
        ),
        Some(&access_token),
    )
    .await?;

    tracing::info!(refresh_id = %refresh_token.id, "created new refresh token");

    exchange_refresh_token(client, &refresh_token).await
}

pub async fn exchange_refresh_token(
    client: &postgrest::Postgrest,
    refresh_token: &RefreshToken,
) -> tonic::Result<(RefreshToken, String, TimeDelta)> {
    #[derive(serde::Deserialize)]
    struct Response {
        access_token: String,
        refresh_token: Option<RefreshToken>, // Set iff the token was single-use.
    }

    let Response {
        access_token,
        refresh_token: next_refresh_token,
    } = crate::postgrest::exec::<Response>(
        client.rpc(
            "generate_access_token",
            serde_json::json!({
                "refresh_token_id": refresh_token.id,
                "secret": refresh_token.secret,
            })
            .to_string(),
        ),
        None, // No access token.
    )
    .await?;

    let unverified =
        tokens::jwt::parse_unverified::<serde::de::IgnoredAny>(access_token.as_bytes())?;
    let valid_for = unverified.valid_for();

    tracing::info!(refresh_id = %refresh_token.id, ?valid_for, "exchanged refresh token for a new access token");

    Ok((
        next_refresh_token.unwrap_or_else(|| refresh_token.clone()),
        access_token,
        valid_for,
    ))
}
