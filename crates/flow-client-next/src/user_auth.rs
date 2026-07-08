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

/// UserTokenSource is a tokens::Source which emits UserTokens.
/// It continues from existing `tokens`, exchanging a refresh token for fresh
/// access tokens as needed.
///
/// `may_create` permits minting a brand-new refresh token from a bare access
/// token. This is reserved for explicit credential establishment
/// (`flowctl auth login` / `auth token`): in every other context a bare access
/// token (for example a `FLOW_AUTH_TOKEN` access token used in automation) is
/// surfaced as-is and allowed to expire, so we never strand abandoned refresh
/// tokens on the control-plane.
pub struct UserTokenSource {
    pub pg_client: postgrest::Postgrest,
    pub tokens: UserToken,
    pub may_create: bool,
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

            // We have an access token but no refresh token, and we're permitted
            // to create one.
            (Some((access_token, _valid_for)), None) if self.may_create => {
                let (refresh_token, access_token, valid_for) =
                    create_refresh_token(&self.pg_client, access_token).await?;

                self.tokens = UserToken {
                    access_token: Some(access_token),
                    refresh_token: Some(refresh_token),
                };
                valid_for
            }

            // We have an access token but no refresh token, and may NOT create
            // one. Surface the access token as-is and let it run to expiry,
            // never minting or rotating a refresh token (e.g. a FLOW_AUTH_TOKEN
            // access token used in automation). Once it has expired, fail rather
            // than attempt a rotation we cannot perform.
            (Some((_access_token, valid_for)), None) => {
                if valid_for <= TimeDelta::zero() {
                    return Err(tonic::Status::unauthenticated(
                        "access token has expired and there is no refresh token with which to obtain a new one",
                    ));
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokens::Source;

    // A client pointed nowhere: create/exchange paths would error if reached,
    // so these tests double as assertions that those network paths are NOT hit.
    fn dummy_pg() -> postgrest::Postgrest {
        crate::postgrest::new_client(&url::Url::parse("http://localhost/").unwrap(), "anon")
    }

    // Build a parseable access-token JWT expiring `valid_for` from now. Only the
    // `exp` claim (read by parse_unverified) matters; the signature is ignored.
    fn access_token(valid_for: TimeDelta) -> String {
        let exp = (tokens::now() + valid_for).timestamp();
        tokens::jwt::sign(
            serde_json::json!({ "exp": exp }),
            &tokens::jwt::EncodingKey::from_secret(b"test"),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn anonymous_source_surfaces_no_tokens() {
        let mut source = UserTokenSource {
            pg_client: dummy_pg(),
            tokens: UserToken::default(),
            may_create: false,
        };
        let (token, valid_for, _) = source.refresh(tokens::now()).await.unwrap().unwrap();
        assert!(token.access_token.is_none());
        assert!(token.refresh_token.is_none());
        assert_eq!(valid_for, TimeDelta::MAX);
    }

    #[tokio::test]
    async fn bare_access_token_without_create_is_surfaced_as_is() {
        // A FLOW_AUTH_TOKEN-style access token, with no refresh token and no
        // permission to create one, is surfaced unchanged and NOT exchanged for
        // a refresh token.
        let jwt = access_token(TimeDelta::hours(1));
        let mut source = UserTokenSource {
            pg_client: dummy_pg(),
            tokens: UserToken {
                access_token: Some(jwt.clone()),
                refresh_token: None,
            },
            may_create: false,
        };
        let (token, valid_for, _) = source.refresh(tokens::now()).await.unwrap().unwrap();
        assert_eq!(token.access_token.as_deref(), Some(jwt.as_str()));
        assert!(token.refresh_token.is_none());
        assert!(valid_for > TimeDelta::minutes(30));
    }

    #[tokio::test]
    async fn expired_access_token_without_create_fails() {
        let jwt = access_token(TimeDelta::minutes(-5));
        let mut source = UserTokenSource {
            pg_client: dummy_pg(),
            tokens: UserToken {
                access_token: Some(jwt),
                refresh_token: None,
            },
            may_create: false,
        };
        let Err(status) = source.refresh(tokens::now()).await else {
            panic!("expected an unauthenticated error");
        };
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn valid_access_and_refresh_resumes_without_exchange() {
        // A still-valid access token plus a refresh token resumes from saved
        // configuration without exchanging (which would hit the network).
        let jwt = access_token(TimeDelta::hours(1));
        let mut source = UserTokenSource {
            pg_client: dummy_pg(),
            tokens: UserToken {
                access_token: Some(jwt.clone()),
                refresh_token: Some(RefreshToken {
                    id: models::Id::zero(),
                    secret: "secret".to_string(),
                }),
            },
            may_create: false,
        };
        let (token, valid_for, _) = source.refresh(tokens::now()).await.unwrap().unwrap();
        assert_eq!(token.access_token.as_deref(), Some(jwt.as_str()));
        assert!(token.refresh_token.is_some());
        assert!(valid_for > TimeDelta::minutes(30));
    }
}
