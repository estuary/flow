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

/// Build a Gazette journal ClientFactory which produces Clients using
/// UserCollectionAuth tokens.
///
/// The AuthZ subject is ignored -- the user is the AuthZ subject.
/// Intuitively, the user is running an operation `flowctl preview`,
/// where they're executing an ephemeral task outside of a data-plane.
///
/// The AuthZ object must be a collection partition template prefix.
/// These have the form "acmeCo/path/to/collection/{generation_id}/",
/// which is mapped to a collection by stripping off the "/{generation_id}/"
/// suffix.
///
/// The partition template prefix of certain legacy collections were created
/// without a "/{generation_id}/" suffix -- these are supported as well.
///
/// Panics if the partition template prefix is not in the expected format.
pub fn new_journal_client_factory(
    api_client: crate::rest::Client,
    capability: models::Capability,
    router: gazette::Router,
    user_tokens: tokens::PendingWatch<crate::user_auth::UserToken>,
) -> gazette::journal::ClientFactory {
    let fragment_client = gazette::journal::Client::new_fragment_client();

    let factory: gazette::journal::ClientFactory = std::sync::Arc::new({
        move |_authz_sub: String, authz_obj: String| -> gazette::journal::Client {
            let collection = match authz_obj.split_at_checked(authz_obj.len() - 18) {
                // Generation ID is 16 bytes of hexadecimal, plus a leading and ending slash.
                Some((collection, gen_id)) if gen_id.starts_with("/") && gen_id.ends_with("/") => {
                    // Expected format: "acmeCo/path/to/collection/{generation_id}/"
                    // Strip off the "/{generation_id}/" suffix to get the collection.
                    collection.to_string()
                }
                _ => authz_obj
                    .strip_suffix("/")
                    .unwrap_or(&authz_obj)
                    .to_string(),
            };

            let source = UserCollectionAuth {
                capability,
                collection: models::Collection::new(collection),
                client: api_client.clone(),
                user_tokens: user_tokens.clone(),
            };
            let watch = tokens::watch(source);

            new_journal_client(fragment_client.clone(), router.clone(), watch)
        }
    });
    factory
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
