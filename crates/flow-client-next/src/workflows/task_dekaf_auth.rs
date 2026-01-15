use tokens::TimeDelta;

/// TaskDekafAuth is a tokens::Source for Dekaf tasks requesting their
/// MaterializationSpec and access to persisted AVRO schemas.
pub struct TaskDekafAuth {
    pub client: crate::rest::Client,
    /// SignedSource for authorization request claims.
    /// Build this using new_signed_source().
    pub signed_source: tokens::jwt::SignedSource<proto_gazette::Claims>,
}

/// Build a SignedSource for authoring TaskDekafAuth request tokens scoping
/// the requesting data-plane & task.
///
/// `task_name` is the catalog name of the requesting subject Dekaf task.
///
/// `data_plane_fqdn` is the FQDN of the data-plane hosting the task.
///
/// `data_plane_signing_key` is the secret data-plane signing key
/// corresponding to the data-plane FQDN.
///
pub fn new_signed_source(
    task_name: String,
    data_plane_fqdn: String,
    data_plane_signing_key: tokens::jwt::EncodingKey,
) -> tokens::jwt::SignedSource<proto_gazette::Claims> {
    let claims = proto_gazette::Claims {
        cap: proto_flow::capability::AUTHORIZE,
        exp: 0,
        iat: 0,
        iss: data_plane_fqdn,
        sel: Default::default(),
        sub: task_name,
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

impl tokens::RestSource for TaskDekafAuth {
    type Model = models::authorizations::DekafAuthResponse;
    type Token = models::authorizations::DekafAuthResponse;

    async fn build_request(
        &mut self,
        started: tokens::DateTime,
    ) -> tonic::Result<reqwest::RequestBuilder> {
        self.signed_source.claims.iat = started.timestamp() as u64;

        let request = models::authorizations::TaskAuthorizationRequest {
            token: self.signed_source.sign()?,
        };
        Ok(self.client.post("/authorize/dekaf", &request, None))
    }

    fn extract(model: Self::Model) -> tonic::Result<Result<(Self::Token, TimeDelta), TimeDelta>> {
        if model.retry_millis != 0 {
            return Ok(Err(TimeDelta::milliseconds(model.retry_millis as i64)));
        }

        // Redirects don't include a token. Use a fixed periodic refresh.
        if model.redirect_dataplane_fqdn.is_some() {
            return Ok(Ok((model, TimeDelta::minutes(5))));
        }

        let unverified =
            tokens::jwt::parse_unverified::<serde::de::IgnoredAny>(model.token.as_bytes())?;

        Ok(Ok((model, unverified.valid_for())))
    }
}
