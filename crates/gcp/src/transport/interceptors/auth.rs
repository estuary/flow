use super::RequestInterceptor;
use anyhow::Context;
use anyhow::Error;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tonic::async_trait;
use tonic::body::Body;
use tonic::codegen::http;

#[derive(Clone)]
pub struct AuthInterceptor {
    provider: OnceCell<Arc<dyn gcp_auth::TokenProvider>>,
    scopes: Vec<&'static str>,
}

impl AuthInterceptor {
    pub fn new(scopes: &[&'static str]) -> Self {
        Self {
            provider: OnceCell::const_new(),
            scopes: scopes.to_vec(),
        }
    }
}

#[async_trait]
impl RequestInterceptor for AuthInterceptor {
    async fn intercept(&self, req: &mut http::Request<Body>) -> Result<()> {
        // Lazily initialize the underlying auth provider via OneCell.
        let provider = self
            .provider
            .get_or_try_init(|| async { gcp_auth::provider().await.map_err(Error::from) })
            .await
            .context("could not initialize GCP auth provider")?;

        let token = provider
            .token(&self.scopes)
            .await
            .context("could not fetch auth token")?
            .as_str()
            .to_string();

        // inject token as bearer authorization header in the request, if not empty
        if !token.is_empty() {
            let bearer = &format!("Bearer {}", token);
            let mut header =
                http::HeaderValue::from_str(bearer).context("invalid authorization header")?;
            header.set_sensitive(true);
            req.headers_mut()
                .insert(http::header::AUTHORIZATION, header);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BIGTABLE_SCOPE: &str = "https://www.googleapis.com/auth/bigtable.data";
    const OTHER_SCOPE: &str = "https://www.googleapis.com/auth/some-other-scope";

    mockall::mock! {
        pub GcpAuthTokenProvider{}

        #[async_trait]
        impl gcp_auth::TokenProvider for GcpAuthTokenProvider {
            #[mockall::concretize]
            async fn token(&self, scopes: &[&str]) -> Result<Arc<gcp_auth::Token>, gcp_auth::Error>;
            async fn project_id(&self) -> Result<Arc<str>, gcp_auth::Error>;
        }
    }

    fn request() -> http::Request<Body> {
        http::Request::get("http://example.com")
            .body(Body::default())
            .unwrap()
    }

    fn token(access_token: &str) -> Arc<gcp_auth::Token> {
        // gcp_auth::Token has no public constructor, so deserialize from json.
        Arc::new(
            serde_json::from_value(serde_json::json!({
                "access_token": access_token,
                "expires_in": 3600,
            }))
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn should_pass_configured_scopes_to_provider() {
        let mut mock_provider = MockGcpAuthTokenProvider::new();
        mock_provider
            .expect_token()
            .withf(|s| s == [BIGTABLE_SCOPE, OTHER_SCOPE])
            .returning(|_| Ok(token("test_token")))
            .times(1);

        let interceptor = AuthInterceptor {
            provider: OnceCell::const_new_with(Arc::new(mock_provider)),
            scopes: vec![BIGTABLE_SCOPE, OTHER_SCOPE],
        };

        let mut req = request();
        let res = interceptor.intercept(&mut req).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn should_replace_an_existing_auth_header() {
        let mut mock_provider = MockGcpAuthTokenProvider::new();
        mock_provider
            .expect_token()
            .returning(|_| Ok(token("fresh_token")))
            .times(1);

        let interceptor = AuthInterceptor {
            provider: OnceCell::const_new_with(Arc::new(mock_provider)),
            scopes: vec![BIGTABLE_SCOPE],
        };

        let mut req = request();
        req.headers_mut().insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_static("Bearer stale_token"),
        );

        interceptor.intercept(&mut req).await.unwrap();

        // expect the stale header to be replaced rather than appended
        let headers = req.headers().get_all(http::header::AUTHORIZATION);
        let header = req.headers().get(http::header::AUTHORIZATION).unwrap();
        assert_eq!(headers.iter().count(), 1);
        assert_eq!(header, "Bearer fresh_token");
    }

    #[tokio::test]
    async fn should_preserve_other_headers() {
        let mut mock_provider = MockGcpAuthTokenProvider::new();
        mock_provider
            .expect_token()
            .returning(|_| Ok(token("test_token")))
            .times(1);

        let interceptor = AuthInterceptor {
            provider: OnceCell::const_new_with(Arc::new(mock_provider)),
            scopes: vec![BIGTABLE_SCOPE],
        };

        let mut req = request();
        req.headers_mut().insert(
            http::HeaderName::from_static("x-goog-request-params"),
            http::HeaderValue::from_static("table_name=projects%2Fp%2Ftables%2Ft"),
        );

        interceptor.intercept(&mut req).await.unwrap();

        // expect other headers on request to remain
        let other_header = req.headers().get("x-goog-request-params").unwrap();
        assert_eq!(other_header, "table_name=projects%2Fp%2Ftables%2Ft");
    }

    #[tokio::test]
    async fn should_error_when_provider_fails() {
        let mut mock_provider = MockGcpAuthTokenProvider::new();
        mock_provider
            .expect_token()
            .returning(|_| Err(gcp_auth::Error::Str("no credentials")))
            .times(1);

        let interceptor = AuthInterceptor {
            provider: OnceCell::const_new_with(Arc::new(mock_provider)),
            scopes: vec![BIGTABLE_SCOPE],
        };

        // expect the interceptor to fail if the provider does
        let mut req = request();
        let err = interceptor.intercept(&mut req).await.unwrap_err();
        // expect the underlying error be wrapped with context
        assert_eq!(err.to_string(), "could not fetch auth token");
        assert_eq!(
            format!("{err:#}"),
            "could not fetch auth token: no credentials"
        );
        // expect the req headers to be unmodified
        assert!(req.headers().get(http::header::AUTHORIZATION).is_none());
    }

    #[tokio::test]
    async fn should_error_when_token_is_not_a_valid_header_value() {
        let mut mock_provider = MockGcpAuthTokenProvider::new();
        // simulate an invalid token: a token with a control
        // character cannot be encoded into a header value
        mock_provider
            .expect_token()
            .returning(|_| Ok(token("bad\ntoken")))
            .times(1);

        let interceptor = AuthInterceptor {
            provider: OnceCell::const_new_with(Arc::new(mock_provider)),
            scopes: vec![BIGTABLE_SCOPE],
        };

        let mut req = request();
        let err = interceptor.intercept(&mut req).await.unwrap_err();

        assert_eq!(err.to_string(), "invalid authorization header");
        assert!(req.headers().get(http::header::AUTHORIZATION).is_none());
    }

    #[tokio::test]
    async fn should_not_inject_auth_header_when_token_is_empty() {
        let mut mock_provider = MockGcpAuthTokenProvider::new();
        mock_provider
            .expect_token()
            .returning(|_| Ok(token("")))
            .times(1);

        let interceptor = AuthInterceptor {
            provider: OnceCell::const_new_with(Arc::new(mock_provider)),
            scopes: vec![BIGTABLE_SCOPE],
        };

        let mut req = request();
        let res = interceptor.intercept(&mut req).await;
        let header = req.headers().get(http::header::AUTHORIZATION);
        assert!(res.is_ok());
        assert!(header.is_none());
    }

    #[tokio::test]
    async fn should_request_a_token_for_every_request() {
        let mut mock_provider = MockGcpAuthTokenProvider::new();
        mock_provider
            .expect_token()
            .returning(|_| Ok(token("test_token")))
            .times(5);

        let interceptor = AuthInterceptor {
            provider: OnceCell::const_new_with(Arc::new(mock_provider)),
            scopes: vec![BIGTABLE_SCOPE],
        };

        for _ in 0..5 {
            let mut req = request();
            let res = interceptor.intercept(&mut req).await;
            assert!(res.is_ok());
        }
    }

    #[tokio::test]
    async fn should_inject_auth_header_into_request() {
        let mut mock_provider = MockGcpAuthTokenProvider::new();
        mock_provider
            .expect_token()
            .returning(|_| Ok(token("test_token")))
            .times(1);

        let interceptor = AuthInterceptor {
            provider: OnceCell::const_new_with(Arc::new(mock_provider)),
            scopes: vec![BIGTABLE_SCOPE],
        };

        // expect auth header to be initially empty
        let mut req = request();
        assert!(req.headers().get(http::header::AUTHORIZATION).is_none());
        // expect the interceptor to succeed
        let res = interceptor.intercept(&mut req).await;
        assert!(res.is_ok());
        // expect the token to be set as a bearer auth header
        let header = req.headers().get(http::header::AUTHORIZATION);
        assert!(header.is_some());
        assert!(header.unwrap().is_sensitive());
        assert_eq!(header.unwrap(), "Bearer test_token");
    }
}
