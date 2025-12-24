/// UserCollectionAuth is a TokenSource that fetches tokens for a collection, on behalf of a user.
pub struct UserCollectionAuth {
    pub client: crate::rest::Client,
    /// UserTokens used to authorize the request.
    pub user_tokens: proto_auth::TokenStream<crate::user_auth::UserToken>,
    /// Collection for which to request authorization.
    pub collection: models::Collection,
    /// Requested capability level of the authorization.
    pub capability: models::Capability,
}

// Build a Gazette journal ClientStream using UserCollectionAuthorization tokens.
pub fn new_journal_client_stream(
    router: gazette::Router,
    fragment_client: reqwest::Client,
    tokens: proto_auth::TokenStream<models::authorizations::UserCollectionAuthorization>,
) -> gazette::journal::ClientStream {
    gazette::journal::new_client_stream(router, fragment_client, tokens, |token| {
        Ok((
            proto_auth::BearerToken::new(&token.broker_token)?,
            token.broker_address.clone(),
        ))
    })
}

impl proto_auth::token::RestSource for UserCollectionAuth {
    type Model = models::authorizations::UserCollectionAuthorization;
    type Token = models::authorizations::UserCollectionAuthorization;

    async fn build_request(
        &mut self,
        started: std::time::SystemTime,
    ) -> tonic::Result<reqwest::RequestBuilder> {
        let started_unix = started
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let request = models::authorizations::UserCollectionAuthorizationRequest {
            started_unix,
            collection: self.collection.clone(),
            capability: self.capability,
        };

        self.user_tokens
            .map_current(|token| {
                Ok(self.client.post(
                    "/authorize/user/collection",
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

        let unverified = proto_auth::jwt::parse_unverified::<serde::de::IgnoredAny>(
            model.broker_token.as_bytes(),
        )?;

        Ok(Ok((model, unverified.valid_for())))
    }
}
