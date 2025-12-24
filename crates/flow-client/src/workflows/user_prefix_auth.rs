/// UserPrefixAuth is a token::Source that fetches user authorization tokens over a prefix and data-plane.
pub struct UserPrefixAuth {
    pub client: crate::rest::Client,
    /// UserTokens used to authorize the request.
    pub user_tokens: proto_auth::TokenStream<crate::user_auth::UserToken>,
    /// Prefix to be authorized.
    pub prefix: models::Prefix,
    /// Name of the data-plane to be authorized.
    pub data_plane: models::Name,
    /// Requested capability level of the authorization.
    pub capability: models::Capability,
}

// Build a Gazette journal ClientStream using UserPrefixAuthorization tokens.
pub fn new_journal_client_stream(
    router: gazette::Router,
    fragment_client: reqwest::Client,
    tokens: proto_auth::TokenStream<models::authorizations::UserPrefixAuthorization>,
) -> gazette::journal::ClientStream {
    gazette::journal::new_client_stream(router, fragment_client, tokens, |token| {
        Ok((
            proto_auth::BearerToken::new(&token.broker_token)?,
            token.broker_address.clone(),
        ))
    })
}

// Build a Gazette shard ClientStream using UserPrefixAuthorization tokens.
pub fn new_shard_client_stream(
    router: gazette::Router,
    tokens: proto_auth::TokenStream<models::authorizations::UserPrefixAuthorization>,
) -> gazette::shard::ClientStream {
    gazette::shard::new_client_stream(router, tokens, |token| {
        Ok((
            proto_auth::BearerToken::new(&token.reactor_token)?,
            token.reactor_address.clone(),
        ))
    })
}

impl proto_auth::token::RestSource for UserPrefixAuth {
    type Model = models::authorizations::UserPrefixAuthorization;
    type Token = models::authorizations::UserPrefixAuthorization;

    async fn build_request(
        &mut self,
        started: std::time::SystemTime,
    ) -> tonic::Result<reqwest::RequestBuilder> {
        let started_unix = started
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let request = models::authorizations::UserPrefixAuthorizationRequest {
            started_unix,
            data_plane: self.data_plane.clone(),
            prefix: self.prefix.clone(),
            capability: self.capability,
        };

        self.user_tokens
            .map_current(|token| {
                Ok(self.client.post(
                    "/authorize/user/prefix",
                    &request,
                    token.access_token.as_ref().map(String::as_str),
                ))
            })
            .await
    }

    fn extract(
        model: Self::Model,
    ) -> tonic::Result<Result<(Self::Token, std::time::Duration), std::time::Duration>> {
        if model.retry_millis != 0 {
            return Ok(Err(std::time::Duration::from_millis(model.retry_millis)));
        }

        let broker_unverified = proto_auth::jwt::parse_unverified::<serde::de::IgnoredAny>(
            model.broker_token.as_bytes(),
        )?;
        let reactor_unverified = proto_auth::jwt::parse_unverified::<serde::de::IgnoredAny>(
            model.reactor_token.as_bytes(),
        )?;
        let valid_for = std::cmp::min(
            broker_unverified.valid_for(),
            reactor_unverified.valid_for(),
        );

        Ok(Ok((model, valid_for)))
    }
}
