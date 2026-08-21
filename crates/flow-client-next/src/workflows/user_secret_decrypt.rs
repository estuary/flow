use tokens::{DateTime, TimeDelta};

/// UserSecretDecrypt is a tokens::Source for a user decrypting a secret.
///
/// Note this Source must be driven by [`tokens::fetch_once`] and never by
/// `tokens::watch`, as with [`super::TaskSecretDecrypt`].
pub struct UserSecretDecrypt {
    /// Client of the config-encryption service.
    pub client: crate::rest::Client,
    /// UserTokens used to authorize the request.
    pub user_tokens: tokens::PendingWatch<crate::user_auth::UserToken>,
    /// Secret to decrypt.
    pub name: models::Secret,
}

impl tokens::RestSource for UserSecretDecrypt {
    type Model = models::authorizations::SecretDecryption;
    type Token = models::authorizations::SecretDecryption;

    async fn build_request(&mut self, started: DateTime) -> tonic::Result<reqwest::RequestBuilder> {
        let user_token = self.user_tokens.ready().await.token();

        let mut url = self
            .client
            .base_url
            .join("/secret/decrypt")
            .expect("path must be valid to join");

        url.query_pairs_mut()
            .append_pair("name", self.name.as_str())
            .append_pair("started", &started.to_rfc3339());

        let request = self.client.http_client.post(url);

        Ok(match user_token.result()?.access_ref() {
            Some(token) => request.bearer_auth(token),
            None => request,
        })
    }

    fn extract(model: Self::Model) -> tonic::Result<Result<(Self::Token, TimeDelta), TimeDelta>> {
        super::extract_secret_decryption(model)
    }
}

#[cfg(test)]
mod tests {
    use super::UserSecretDecrypt;
    use tokens::RestSource;

    /// The user path has no token of its own to read `started` from, so it
    /// travels as a parameter which the user authorize route requires.
    #[tokio::test]
    async fn test_user_request() {
        let mut source = UserSecretDecrypt {
            client: crate::rest::Client::new(
                &url::Url::parse("https://config-encryption.example.com/").unwrap(),
                "test",
            ),
            user_tokens: tokens::fixed(Ok(crate::user_auth::UserToken {
                access_token: Some("access-token".to_string()),
                refresh_token: None,
            })),
            name: models::Secret::new("acmeCo/password"),
        };
        let started = tokens::DateTime::from_timestamp_secs(1700000000).unwrap();
        let request = source
            .build_request(started)
            .await
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(
            request.url().as_str(),
            "https://config-encryption.example.com/secret/decrypt?name=acmeCo%2Fpassword&started=2023-11-14T22%3A13%3A20%2B00%3A00"
        );
        assert_eq!(
            request
                .headers()
                .get(reqwest::header::AUTHORIZATION)
                .unwrap(),
            "Bearer access-token",
        );
    }
}
