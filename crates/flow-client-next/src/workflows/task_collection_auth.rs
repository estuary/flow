use proto_gazette::broker;
use tokens::TimeDelta;

/// TaskCollectionAuth is a tokens::Source for tasks accessing collections.
pub struct TaskCollectionAuth {
    pub client: crate::rest::Client,
    /// SignedSource for authorization request claims.
    /// Build this using new_signed_source().
    pub signed_source: tokens::jwt::SignedSource<proto_gazette::Claims>,
}

/// Build a SignedSource for authoring TaskCollectionAuth request tokens scoping
/// the requesting data-plane & task, and the requested collection & capability.
///
/// `journal_name_or_prefix` is the journal name(s) to authorize.
///
/// - When authorizing a task's bound collection for reads or writes,
///   this is the collection journal name template embedded within
///   the task specification.
///
/// - When authorizing a task's ops collection, this is the concrete
///   ops journal partition name drawn from the task's ShardSpec labels
///   (or, for Dekaf tasks, the DekafAuthResponse).
///
/// `shard_id_or_template` is the Shard ID of the requesting subject task,
/// or minimally the Shard ID template drawn from the task specification
/// (the latter is relevant for Dekaf, which has no actual Shards).
///
/// `capability` is the requested capability level of the authorization.
/// This is NOT a models::Capability. Rather, it's a bit-mask in the u32
/// Gazette capability namespace and is restricted to:
/// - proto_gazette::capability::READ
/// - proto_gazette::capability::APPEND
///
/// `data_plane_fqdn` is the FQDN of the data-plane hosting the task.
///
/// `data_plane_signing_key` is the secret data-plane signing key
/// corresponding to the data-plane FQDN.
///
pub fn new_signed_source(
    journal_name_or_prefix: String,
    shard_id_or_template: String,
    capability: u32,
    data_plane_fqdn: String,
    data_plane_signing_key: jsonwebtoken::EncodingKey,
) -> tokens::jwt::SignedSource<proto_gazette::Claims> {
    let sel = broker::LabelSelector {
        include: Some(broker::LabelSet {
            labels: vec![broker::Label {
                name: "name".to_string(),
                value: journal_name_or_prefix,
                prefix: true,
            }],
        }),
        exclude: None,
    };

    let claims = proto_gazette::Claims {
        cap: capability | proto_flow::capability::AUTHORIZE,
        exp: 0,
        iat: 0,
        iss: data_plane_fqdn,
        sel,
        sub: shard_id_or_template,
    };

    tokens::jwt::SignedSource {
        claims,
        set_time_claims: Box::new(|claims, _iat, exp| {
            // claims.iat is explicitly set to the start time of the logical request.
            claims.exp = exp.timestamp() as u64;
        }),
        duration: TimeDelta::minutes(1),
        key: data_plane_signing_key,
    }
}

/// Build a Gazette journal client using TaskAuthorization tokens.
pub fn new_journal_client(
    fragment_client: reqwest::Client,
    router: gazette::Router,
    tokens: tokens::PendingWatch<models::authorizations::TaskAuthorization>,
) -> gazette::journal::Client {
    gazette::journal::Client::new_with_tokens(
        |token| {
            Ok((
                proto_grpc::Metadata::new().with_bearer_token(&token.token)?,
                token.broker_address.clone(),
            ))
        },
        fragment_client,
        router,
        tokens,
    )
}

impl tokens::RestSource for TaskCollectionAuth {
    type Model = models::authorizations::TaskAuthorization;
    type Token = models::authorizations::TaskAuthorization;

    async fn build_request(
        &mut self,
        started: tokens::DateTime,
    ) -> tonic::Result<reqwest::RequestBuilder> {
        self.signed_source.claims.iat = started.timestamp() as u64;

        let request = models::authorizations::TaskAuthorizationRequest {
            token: self.signed_source.sign()?,
        };
        Ok(self.client.post("/authorize/task", &request, None))
    }

    fn extract(model: Self::Model) -> tonic::Result<Result<(Self::Token, TimeDelta), TimeDelta>> {
        if model.retry_millis != 0 {
            return Ok(Err(TimeDelta::milliseconds(model.retry_millis as i64)));
        }

        let unverified =
            tokens::jwt::parse_unverified::<serde::de::IgnoredAny>(model.token.as_bytes())?;

        Ok(Ok((model, unverified.valid_for())))
    }
}
