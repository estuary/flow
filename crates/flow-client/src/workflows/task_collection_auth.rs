use proto_gazette::broker;

/// TaskCollectionAuth is a TokenSource that fetches tokens for a collection, on behalf of a task.
pub struct TaskCollectionAuth {
    pub client: crate::rest::Client,
    /// Journal name(s) to authorize.
    ///
    /// - When authorizing a task's bound collection for reads or writes,
    ///   this is the collection journal name template embedded within
    ///   the task specification.
    ///
    /// - When authorizing a task's ops collection, this is the concrete
    ///   ops journal partition name drawn from the task's ShardSpec labels.
    pub journal_name_or_prefix: String,
    /// Shard ID template of the subject task we're authorizing,
    /// drawn from the task specification.
    pub shard_id_template: String,
    /// Requested capability level of the authorization.
    /// This is NOT a models::Capability. Rather, it's a bit-mask in the u32
    /// Gazette capability namespace and is restricted to:
    /// - proto_gazette::capability::READ
    /// - proto_gazette::capability::APPEND
    pub capability: u32,
    /// FQDN of the data-plane hosting the task.
    pub data_plane_fqdn: String,
    /// Signing key for authorization request claims.
    /// This key corresponds to the data-plane FQDN.
    pub data_plane_signing_key: jsonwebtoken::EncodingKey,
}

// Build a Gazette journal ClientStream using TaskAuthorization tokens.
pub fn new_journal_client_stream(
    router: gazette::Router,
    fragment_client: reqwest::Client,
    tokens: proto_auth::TokenStream<models::authorizations::TaskAuthorization>,
) -> gazette::journal::ClientStream {
    gazette::journal::new_client_stream(router, fragment_client, tokens, |token| {
        Ok((
            proto_auth::BearerToken::new(&token.token)?,
            token.broker_address.clone(),
        ))
    })
}

impl proto_auth::token::RestSource for TaskCollectionAuth {
    type Model = models::authorizations::TaskAuthorization;
    type Token = models::authorizations::TaskAuthorization;

    async fn build_request(
        &mut self,
        started: std::time::SystemTime,
    ) -> tonic::Result<reqwest::RequestBuilder> {
        let started_unix = started
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // `started_unix` could be quite a while ago in certain outage conditions.
        // Make `exp` relative to now() to avoid doomed refresh loops.
        let now = jsonwebtoken::get_current_timestamp();

        let sel = broker::LabelSelector {
            include: Some(broker::LabelSet {
                labels: vec![broker::Label {
                    name: "name".to_string(),
                    value: self.journal_name_or_prefix.to_string(),
                    prefix: true,
                }],
            }),
            exclude: None,
        };

        let claims = proto_gazette::Claims {
            cap: self.capability | proto_flow::capability::AUTHORIZE,
            exp: now + 60,
            iat: started_unix,
            iss: self.data_plane_fqdn.clone(),
            sel,
            sub: self.shard_id_template.clone(),
        };

        let token = proto_auth::jwt::sign(claims, &self.data_plane_signing_key)?;
        let request = models::authorizations::TaskAuthorizationRequest { token };

        Ok(self.client.post("/authorize/task", &request, None))
    }

    fn extract(
        model: Self::Model,
    ) -> tonic::Result<Result<(Self::Token, std::time::Duration), std::time::Duration>> {
        if model.retry_millis != 0 {
            return Ok(Err(std::time::Duration::from_millis(model.retry_millis)));
        }

        let unverified =
            proto_auth::jwt::parse_unverified::<serde::de::IgnoredAny>(model.token.as_bytes())?;

        Ok(Ok((model, unverified.valid_for())))
    }
}
