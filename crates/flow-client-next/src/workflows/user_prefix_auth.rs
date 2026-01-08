use tokens::TimeDelta;

/// UserPrefixAuth is a tokens::Source for users accessing a catalog prefix
/// within a data-plane.
pub struct UserPrefixAuth {
    pub client: crate::rest::Client,
    /// UserTokens used to authorize the request.
    pub user_tokens: tokens::PendingWatch<crate::user_auth::UserToken>,
    /// Prefix to be authorized.
    pub prefix: models::Prefix,
    /// Name of the data-plane to be authorized.
    pub data_plane: models::Name,
    /// Requested capability level of the authorization.
    pub capability: models::Capability,
}

/// Build a Gazette journal Client using UserPrefixAuthorization tokens.
pub fn new_journal_client(
    fragment_client: reqwest::Client,
    router: gazette::Router,
    tokens: tokens::PendingWatch<models::authorizations::UserPrefixAuthorization>,
) -> gazette::journal::Client {
    gazette::journal::Client::new_with_tokens(
        |token| {
            Ok((
                proto_grpc::Metadata::new().with_bearer_token(&token.broker_token)?,
                token.broker_address.clone(),
            ))
        },
        fragment_client,
        router,
        tokens,
    )
}

/// Build a Gazette shard Client using UserPrefixAuthorization tokens.
pub fn new_shard_client(
    router: gazette::Router,
    tokens: tokens::PendingWatch<models::authorizations::UserPrefixAuthorization>,
) -> gazette::shard::Client {
    gazette::shard::Client::new_with_tokens(
        |token| {
            Ok((
                proto_grpc::Metadata::new().with_bearer_token(&token.reactor_token)?,
                token.reactor_address.clone(),
            ))
        },
        router,
        tokens,
    )
}

impl tokens::RestSource for UserPrefixAuth {
    type Model = models::authorizations::UserPrefixAuthorization;
    type Token = models::authorizations::UserPrefixAuthorization;

    async fn build_request(
        &mut self,
        started: tokens::DateTime,
    ) -> tonic::Result<reqwest::RequestBuilder> {
        let request = models::authorizations::UserPrefixAuthorizationRequest {
            started_unix: started.timestamp() as u64,
            data_plane: self.data_plane.clone(),
            prefix: self.prefix.clone(),
            capability: self.capability,
        };
        let user_token = self.user_tokens.ready().await.token();

        Ok(self.client.post(
            "/authorize/user/prefix",
            &request,
            user_token.result()?.access_ref(),
        ))
    }

    fn extract(model: Self::Model) -> tonic::Result<Result<(Self::Token, TimeDelta), TimeDelta>> {
        if model.retry_millis != 0 {
            return Ok(Err(TimeDelta::milliseconds(model.retry_millis as i64)));
        }

        let broker_unverified =
            tokens::jwt::parse_unverified::<serde::de::IgnoredAny>(model.broker_token.as_bytes())?;
        let reactor_unverified =
            tokens::jwt::parse_unverified::<serde::de::IgnoredAny>(model.reactor_token.as_bytes())?;
        let valid_for = std::cmp::min(
            broker_unverified.valid_for(),
            reactor_unverified.valid_for(),
        );

        Ok(Ok((model, valid_for)))
    }
}
