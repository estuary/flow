use tokens::TimeDelta;

/// UserCollectionAuth is a tokens::Source for users accessing collections.
pub struct UserCollectionAuth {
    pub client: crate::rest::Client,
    /// UserTokens used to authorize the request.
    pub user_tokens: tokens::PendingWatch<crate::user_auth::UserToken>,
    /// Collection for which to request authorization.
    pub collection: models::Collection,
    /// Requested capability level of the authorization.
    pub capability: models::Capability,
}

/// Build a Gazette journal Client using UserCollectionAuthorization tokens.
pub fn new_journal_client(
    fragment_client: reqwest::Client,
    router: gazette::Router,
    tokens: tokens::PendingWatch<models::authorizations::UserCollectionAuthorization>,
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

impl tokens::RestSource for UserCollectionAuth {
    type Model = models::authorizations::UserCollectionAuthorization;
    type Token = models::authorizations::UserCollectionAuthorization;

    async fn build_request(
        &mut self,
        started: tokens::DateTime,
    ) -> tonic::Result<reqwest::RequestBuilder> {
        let request = models::authorizations::UserCollectionAuthorizationRequest {
            started_unix: started.timestamp() as u64,
            collection: self.collection.clone(),
            capability: self.capability,
        };
        let user_token = self.user_tokens.ready().await.token();

        Ok(self.client.post(
            "/authorize/user/collection",
            &request,
            user_token.result()?.access_ref(),
        ))
    }

    fn extract(model: Self::Model) -> tonic::Result<Result<(Self::Token, TimeDelta), TimeDelta>> {
        if model.retry_millis != 0 {
            return Ok(Err(TimeDelta::milliseconds(model.retry_millis as i64)));
        }

        let unverified =
            tokens::jwt::parse_unverified::<serde::de::IgnoredAny>(model.broker_token.as_bytes())?;

        Ok(Ok((model, unverified.valid_for())))
    }
}
