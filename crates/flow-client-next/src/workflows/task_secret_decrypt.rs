use tokens::{DateTime, TimeDelta};

/// TaskSecretDecrypt is a tokens::Source for a task decrypting one of its secrets.
///
/// Note this Source must be driven by [`tokens::fetch_once`] and never by
/// `tokens::watch`: plaintext is resolved when it's needed and is not retained,
/// and a Watch would do the opposite.
pub struct TaskSecretDecrypt {
    /// Client of the config-encryption service.
    pub client: crate::rest::Client,
    /// Secret to decrypt. Pass it to new_signed_source() as well: it names the
    /// document we expect back, while the claims name the one we're authorized
    /// to have, and config-encryption fails closed if they disagree.
    pub name: models::Secret,
    /// SignedSource for authorization request claims.
    /// Build this using new_signed_source().
    pub signed_source: tokens::jwt::SignedSource<proto_gazette::Claims>,
}

/// Build a SignedSource for authoring TaskSecretDecrypt request tokens, scoping
/// the requesting data-plane & task shard, and the requested secret.
///
/// `secret_name` is the catalog name of the secret to decrypt. It must be a
/// sibling of the task -- they share a catalog prefix -- which is the rule the
/// control-plane enforces over these claims.
///
/// `shard_id` is the Shard ID of the requesting subject task. Apply and Open
/// run within a shard and use its own ID. Discover and Validate precede any
/// shard, and instead use a synthetic Shard ID over the task's type and name,
/// having an all-zero generation ID and a full key and r-clock range.
///
/// `data_plane_fqdn` is the FQDN of the data-plane hosting the task, and
/// `data_plane_signing_key` is its corresponding secret signing key.
pub fn new_signed_source(
    secret_name: &models::Secret,
    shard_id: String,
    data_plane_fqdn: String,
    data_plane_signing_key: tokens::jwt::EncodingKey,
) -> tokens::jwt::SignedSource<proto_gazette::Claims> {
    let sel = proto_gazette::broker::LabelSelector {
        include: Some(labels::build_set([(
            labels::SECRET_NAME,
            secret_name.as_str(),
        )])),
        exclude: None,
    };

    let claims = proto_gazette::Claims {
        cap: proto_flow::capability::AUTHORIZE,
        exp: 0,
        iat: 0,
        iss: data_plane_fqdn,
        sel,
        sub: shard_id,
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

impl tokens::RestSource for TaskSecretDecrypt {
    type Model = models::authorizations::SecretDecryption;
    type Token = models::authorizations::SecretDecryption;

    async fn build_request(&mut self, started: DateTime) -> tonic::Result<reqwest::RequestBuilder> {
        self.signed_source.claims.iat = started.timestamp() as u64;

        let mut url = self
            .client
            .base_url
            .join("/secret/decrypt")
            .expect("path must be valid to join");

        url.query_pairs_mut()
            .append_pair("name", self.name.as_str());

        Ok(self
            .client
            .http_client
            .post(url)
            .bearer_auth(self.signed_source.sign()?))
    }

    fn extract(model: Self::Model) -> tonic::Result<Result<(Self::Token, TimeDelta), TimeDelta>> {
        super::extract_secret_decryption(model)
    }
}

#[cfg(test)]
mod tests {
    use super::{TaskSecretDecrypt, new_signed_source};
    use tokens::RestSource;

    /// The task path carries its subject and its secret in the token, and
    /// nothing else: `started` is recoverable from `iat`, so it doesn't travel.
    #[tokio::test]
    async fn test_task_request() {
        let name = models::Secret::new("acmeCo/password");
        let started = tokens::DateTime::from_timestamp_secs(1700000000).unwrap();

        let mut source = TaskSecretDecrypt {
            client: crate::rest::Client::new(
                &url::Url::parse("https://config-encryption.example.com/").unwrap(),
                "test",
            ),
            name: name.clone(),
            signed_source: new_signed_source(
                &name,
                "capture/acmeCo/source-pineapple/0000000000000000/00000000-00000000".to_string(),
                "fqdn.example.com".to_string(),
                tokens::jwt::EncodingKey::from_secret(b"secret"),
            ),
        };
        let request = source
            .build_request(started)
            .await
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(
            request.url().as_str(),
            "https://config-encryption.example.com/secret/decrypt?name=acmeCo%2Fpassword"
        );

        let bearer = request
            .headers()
            .get(reqwest::header::AUTHORIZATION)
            .unwrap()
            .to_str()
            .unwrap()
            .strip_prefix("Bearer ")
            .unwrap()
            .to_string();

        let claims = tokens::jwt::parse_unverified::<proto_gazette::Claims>(bearer.as_bytes())
            .unwrap()
            .claims()
            .clone();

        assert_eq!(claims.iat, started.timestamp() as u64);
        assert!(claims.exp > tokens::now().timestamp() as u64);

        insta::assert_debug_snapshot!(
            (claims.sub, claims.iss, claims.cap, claims.sel),
            @r###"
        (
            "capture/acmeCo/source-pineapple/0000000000000000/00000000-00000000",
            "fqdn.example.com",
            65536,
            LabelSelector {
                include: Some(
                    LabelSet {
                        labels: [
                            Label {
                                name: "estuary.dev/secret-name",
                                value: "acmeCo/password",
                                prefix: false,
                            },
                        ],
                    },
                ),
                exclude: None,
            },
        )
        "###
        );
    }
}
