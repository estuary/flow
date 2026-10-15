use std::sync::Arc;

/// Verified bearer claims, optionally narrowed by a request scope header.
#[derive(Debug)]
pub struct MaybeControlClaims {
    verified: Option<tokens::jwt::Verified<crate::ControlClaims>>,
    scoped: Option<crate::ControlClaims>,
}

impl MaybeControlClaims {
    pub fn with_verified(verified: tokens::jwt::Verified<crate::ControlClaims>) -> Self {
        Self {
            verified: Some(verified),
            scoped: None,
        }
    }

    pub fn with_unauthenticated() -> Self {
        Self {
            verified: None,
            scoped: None,
        }
    }

    fn with_scope_header(mut self, headers: &axum::http::HeaderMap) -> tonic::Result<Self> {
        let Some(scope) = parse_scope_prefix(headers)? else {
            return Ok(self);
        };
        let claims = self.result()?;

        // Prefix scopes follow role grants, so even a textual sub-prefix can
        // reach authority outside another scope. Do not replace a token ceiling.
        if let Some(token_scope) = claims.subject().prefix_scope {
            if token_scope != scope {
                return Err(tonic::Status::invalid_argument(
                    "X-Estuary-Scope-Prefix must match the token's prefix_scope when set",
                ));
            }
            return Ok(self);
        }

        // All handlers, including credential issuance guards, consume these
        // effective claims and therefore use the existing prefix_scope policy.
        let mut claims = claims.clone();
        claims.prefix_scope = Some(scope);
        self.scoped = Some(claims);
        Ok(self)
    }

    pub fn result(&self) -> tonic::Result<&crate::ControlClaims> {
        match &self.verified {
            Some(verified) => Ok(self.scoped.as_ref().unwrap_or_else(|| verified.claims())),
            None => Err(tonic::Status::unauthenticated(
                "This is an authenticated API but the request is missing a required Authorization: Bearer token",
            )),
        }
    }
}

const SCOPE_PREFIX_HEADER: &str = "x-estuary-scope-prefix";

fn parse_scope_prefix(headers: &axum::http::HeaderMap) -> tonic::Result<Option<String>> {
    use validator::Validate;

    let mut values = headers.get_all(SCOPE_PREFIX_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(tonic::Status::invalid_argument(
            "X-Estuary-Scope-Prefix must be provided only once",
        ));
    }
    let value = value
        .to_str()
        .map_err(|_| tonic::Status::invalid_argument("X-Estuary-Scope-Prefix must be ASCII"))?;
    if value.is_empty() || models::Prefix::new(value).validate().is_err() {
        return Err(tonic::Status::invalid_argument(
            "X-Estuary-Scope-Prefix must be a non-empty catalog prefix ending in '/'",
        ));
    }
    Ok(Some(value.to_string()))
}

/// Locale is a placeholder, since we only support a single locale today. Once
/// we support more than one locale, we should try to determine this
/// automatically based on the request headers. For now, we just hard code it.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Locale {
    EnUS, // English, US, the only locale we currently have translations for
}

impl AsRef<str> for Locale {
    fn as_ref(&self) -> &str {
        match *self {
            Locale::EnUS => "en-US",
        }
    }
}

/// Envelope packages common fields and request-derived parameters which are
/// universal across the Estuary API.
#[derive(Debug)]
pub struct Envelope {
    /// The original request URI.
    pub original_uri: axum::http::Uri,
    /// The verified control-plane claims, if any.
    pub maybe_claims: MaybeControlClaims,
    /// If provided, the `retryAfter` query parameter attached to the request.
    /// This parameter is used to detect clients that don't honor the Retry-After
    /// header sent with 307 Temporary Redirect responses.
    ///
    /// If absent, it's set to the Unix epoch.
    pub retry_after: tokens::DateTime,
    /// The Snapshot Refresh to use throughout request processing.
    pub refresh: Arc<tokens::Refresh<crate::Snapshot>>,
    /// If provided, the `started` query parameter attached to the request.
    /// This establishes the logical start time of the request operation and is
    /// used to resolve causal ordering with respect to `refresh`.
    ///
    /// If absent, it's set to the current time.
    pub started: tokens::DateTime,
    /// Was `started` provided on the request?
    pub started_set: bool,
    /// Database pool to use during request processing.
    pub pg_pool: sqlx::PgPool,
    /// The desired locale for any internationalized text values. This is
    /// primarily just used with connectors at the moment, though it could be
    /// used by any api that returns human-readable text.
    pub locale: Locale,
}

impl Envelope {
    /// Returns verified claims narrowed by the request header, or an unauthenticated error.
    pub fn claims(&self) -> tonic::Result<&crate::ControlClaims> {
        self.maybe_claims.result()
    }

    /// Returns the request's associated Snapshot.
    pub fn snapshot(&self) -> &crate::Snapshot {
        self.refresh.result().expect("Snapshot refresh never fails")
    }

    /// Evaluate an authorization policy result and return its outcome.
    ///
    /// This method handles the complexity of Snapshot refresh, retry logic,
    /// and cordoning. Call it with a AuthZResult from your authorization
    /// evaluation policy function.
    pub async fn authorization_outcome<Ok>(
        &self,
        policy_result: crate::AuthZResult<Ok>,
    ) -> Result<(tokens::DateTime, Ok), crate::ApiError> {
        let snapshot = self.snapshot();

        // Select an expiration for the evaluated authorization (presuming it succeeds)
        // which is at-most MAX_AUTHORIZATION in the future relative to when the
        // Snapshot was taken. Jitter to smooth the load of re-authorizations.
        use rand::Rng;
        let exp = snapshot.taken
            + chrono::TimeDelta::seconds(rand::rng().random_range(
                (crate::Snapshot::MAX_AUTHORIZATION.num_seconds() / 2)
                    ..crate::Snapshot::MAX_AUTHORIZATION.num_seconds(),
            ));

        let status = match policy_result {
            // Authorization is valid and not cordoned.
            Ok((None, ok)) => return Ok((exp, ok)),
            // Authorization is valid but cordoned after a future `cordon_at`.
            Ok((Some(cordon_at), ok)) if cordon_at > self.started => {
                return Ok((std::cmp::min(exp, cordon_at), ok));
            }
            // Authorization is invalid and the Snapshot was taken after the
            // start of the authorization request. Terminal failure.
            Err(status) if snapshot.taken_after(self.started) => {
                return Err(status.into());
            }
            // Authorization is valid but is currently cordoned, and we must
            // hold it in limbo until the cordoned condition is resolved
            // by a future Snapshot.
            Ok((Some(cordon_at), _ok)) => tonic::Status::unavailable(format!(
                "this resource is temporarily unavailable due to an ongoing data-plane migration (cordoned at {cordon_at})"
            )),
            // Authorization is invalid but the Snapshot is older than the start
            // of the authorization request. It's possible that the requestor has
            // more-recent knowledge that the authorization is valid.
            Err(status) => status,
        };

        // We must await a future Snapshot to determine the definitive outcome.
        snapshot.revoke.cancel(); // Request early refresh.

        let failed = tokens::now();
        let retry_delta = self.retry_after - failed;

        tracing::warn!(
            started=%self.started,
            taken=%snapshot.taken,
            code=?status.code(),
            error=%status.message(),
            "provisional authorization failure",
        );

        let retry_after = if retry_delta > tokens::TimeDelta::zero() {
            // The client didn't honor a Retry-After we previously sent.
            // Assume they don't support it and block server-side.
            // Then use an Retry-After of the epoch to tell our future selves
            // that we must block server-side, should we need to.
            () = self.refresh.expired().await;

            tokens::DateTime::UNIX_EPOCH
        } else {
            // Determine the remaining "cool off" time before the next Snapshot starts.
            let cool_off = std::cmp::max(
                (snapshot.taken + crate::Snapshot::MIN_REFRESH_INTERVAL) - failed,
                tokens::TimeDelta::zero(),
            );

            // We don't know how long a Snapshot fetch will take. Currently it's ~1-5 seconds,
            // but our real objective here is to smooth the herd of retries awaiting a refresh.
            failed
                + cool_off
                + tokens::TimeDelta::milliseconds(rand::rng().random_range(500..10_000))
        };

        Err(crate::ApiError::AuthZRetry(crate::AuthZRetry {
            original_uri: self.original_uri.clone(),
            started: self.started,
            failed,
            retry_after,
            status,
        }))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Rejection {
    #[error(transparent)]
    Query(#[from] axum::extract::rejection::QueryRejection),
    #[error(transparent)]
    Bearer(#[from] axum_extra::typed_header::TypedHeaderRejection),
    #[error(transparent)]
    Status(#[from] tonic::Status),
}

impl axum::response::IntoResponse for Rejection {
    fn into_response(self) -> axum::response::Response {
        match self {
            Rejection::Query(rej) => rej.into_response(),
            Rejection::Bearer(rej) => rej.into_response(),
            Rejection::Status(status) => crate::status_into_response(status),
        }
    }
}

impl axum::extract::FromRequestParts<Arc<crate::App>> for Envelope {
    type Rejection = Rejection;

    fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &Arc<crate::App>,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        async move {
            // Extract the original request URI.
            // This may not equal `parts.uri` if an outer service unwrapped
            // prefixes of the path, so we use the OriginalUri extractor.
            let axum::extract::OriginalUri(original_uri) =
                Result::<_, std::convert::Infallible>::unwrap(
                    axum::extract::OriginalUri::from_request_parts(parts, state).await,
                );

            // Extract query parameters used for timings of authorization retries.
            #[derive(Debug, Default, serde::Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct Params {
                /// The logical start time of the request, maintained across retries.
                started: Option<tokens::DateTime>,
                /// The retry-after timestamp from a previous 307 response.
                retry_after: Option<tokens::DateTime>,
            }

            let axum::extract::Query(Params {
                started,
                retry_after,
            }) = axum::extract::Query::<Params>::from_request_parts(parts, state).await?;

            // Extract optional bearer token and parse into verified claims, if present.
            use axum_extra::{
                TypedHeader,
                headers::{Authorization, authorization::Bearer},
            };
            let maybe_bearer =
                Option::<TypedHeader<Authorization<Bearer>>>::from_request_parts(parts, state)
                    .await?;

            let maybe_claims = match maybe_bearer {
                Some(TypedHeader(auth)) => {
                    let mut token = auth.token();
                    let exchanged_token: Option<String>;

                    // Is this is a refresh token? If so, first exchange for an access token.
                    if !token.contains(".") {
                        exchanged_token = Some(
                            crate::server::exchange_refresh_token(&state.pg_pool, token).await?,
                        );
                        token = exchanged_token.as_ref().unwrap();
                    }

                    let verified = tokens::jwt::verify::<crate::ControlClaims>(
                        token.as_bytes(),
                        0,
                        &state.control_plane_jwt_decode_keys,
                    )?;

                    if verified.claims().aud != "authenticated" {
                        return Err(tonic::Status::unauthenticated(
                            "authorization bearer claims missing required `aud` of 'authenticated'",
                        )
                        .into());
                    }

                    MaybeControlClaims::with_verified(verified)
                }
                None => MaybeControlClaims::with_unauthenticated(),
            }
            .with_scope_header(&parts.headers)?;

            // Placeholder. In the future, we should determine this value from
            // the request headers (e.g. Accept-Language and/or the auth token).
            // For now, we hard code it, because we don't have translations for
            // any other locales anyway.
            let locale = Locale::EnUS;

            Ok(Envelope {
                maybe_claims,
                retry_after: retry_after.unwrap_or(tokens::DateTime::UNIX_EPOCH),
                refresh: state.snapshot.token(),
                started_set: started.is_some(),
                started: started.unwrap_or_else(|| tokens::now()),
                pg_pool: state.pg_pool.clone(),
                original_uri,
                locale,
            })
        }
    }
}

// Empty impl allows aide to generate OpenAPI specs for handlers using this extractor.
// The extractor is an internal detail and doesn't appear in the API documentation.
impl aide::operation::OperationInput for Envelope {}

#[cfg(test)]
mod test {
    use super::*;

    fn headers(value: &str) -> axum::http::HeaderMap {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(SCOPE_PREFIX_HEADER, value.parse().unwrap());
        headers
    }

    fn bearer(scope: Option<&str>, mask: Option<Vec<String>>) -> MaybeControlClaims {
        let claims = crate::ControlClaims {
            aud: "authenticated".to_string(),
            iat: tokens::now().timestamp() as u64,
            exp: (tokens::now() + chrono::Duration::hours(1)).timestamp() as u64,
            sub: uuid::Uuid::from_bytes([0x11; 16]),
            role: "authenticated".to_string(),
            email: None,
            capability_mask: mask,
            prefix_scope: scope.map(String::from),
        };
        let key = jsonwebtoken::EncodingKey::from_secret(b"scope-header-test");
        let token = tokens::jwt::sign(&claims, &key).unwrap();
        MaybeControlClaims::with_verified(
            tokens::jwt::verify(
                token.as_bytes(),
                0,
                &[jsonwebtoken::DecodingKey::from_secret(b"scope-header-test")],
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_scope_header_validation() {
        assert_eq!(
            parse_scope_prefix(&axum::http::HeaderMap::new()).unwrap(),
            None
        );
        for value in ["acmeCo/", "acmeCo/team/", "acmeCo/one/th.ree/"] {
            assert_eq!(
                parse_scope_prefix(&headers(value)).unwrap().as_deref(),
                Some(value)
            );
        }
        for value in [
            "",
            "/",
            "acmeCo",
            "/acmeCo/",
            "acmeCo//team/",
            "acmeCo/sp ace/",
            "acmeCo/,betaCo/",
        ] {
            assert_eq!(
                parse_scope_prefix(&headers(value)).unwrap_err().code(),
                tonic::Code::InvalidArgument
            );
        }
        let mut repeated = headers("acmeCo/");
        repeated.append(SCOPE_PREFIX_HEADER, "betaCo/".parse().unwrap());
        assert_eq!(
            parse_scope_prefix(&repeated).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        let mut non_ascii = axum::http::HeaderMap::new();
        non_ascii.insert(
            SCOPE_PREFIX_HEADER,
            axum::http::HeaderValue::from_bytes(b"\xff/").unwrap(),
        );
        assert_eq!(
            parse_scope_prefix(&non_ascii).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn test_scope_header_requires_authentication() {
        assert_eq!(
            MaybeControlClaims::with_unauthenticated()
                .with_scope_header(&headers("acmeCo/"))
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        assert!(
            MaybeControlClaims::with_unauthenticated()
                .with_scope_header(&axum::http::HeaderMap::new())
                .is_ok()
        );
    }

    #[test]
    fn test_scope_header_preserves_token_restrictions() {
        for scope in [None, Some("acmeCo/"), Some("")] {
            let claims = bearer(scope, Some(vec!["viewer".to_string()]))
                .with_scope_header(&axum::http::HeaderMap::new())
                .unwrap();
            assert_eq!(claims.result().unwrap().prefix_scope.as_deref(), scope);
            assert_eq!(
                claims.result().unwrap().capability_mask.as_ref().unwrap(),
                &["viewer"]
            );
        }
        for token_scope in ["acmeCo", "acmeCo/"] {
            assert!(
                bearer(Some(token_scope), None)
                    .with_scope_header(&headers("acmeCo/"))
                    .is_ok()
            );
            // Neither a parent, a child, nor a sibling may replace the token's graph ceiling.
            for header in ["acmeCo/team/", "betaCo/"] {
                assert_eq!(
                    bearer(Some(token_scope), None)
                        .with_scope_header(&headers(header))
                        .unwrap_err()
                        .code(),
                    tonic::Code::InvalidArgument
                );
            }
        }
        for token_scope in ["acmeCo/team/", ""] {
            assert!(
                bearer(Some(token_scope), None)
                    .with_scope_header(&headers("acmeCo/"))
                    .is_err()
            );
        }
        let claims = bearer(None, Some(vec![]))
            .with_scope_header(&headers("acmeCo/"))
            .unwrap();
        assert_eq!(
            claims.result().unwrap().prefix_scope.as_deref(),
            Some("acmeCo/")
        );
        assert_eq!(claims.result().unwrap().capability_mask, Some(vec![]));
    }

    #[test]
    fn test_scope_header_uses_existing_authorization() {
        use models::Capability::{Admin, Read};
        let user = uuid::Uuid::from_bytes([0x11; 16]);
        let snapshot = crate::test_server::snapshot_of_grants(
            &[(user, "acmeCo/", Admin), (user, "betaCo/", Admin)],
            &[("acmeCo/", "sharedCo/", Read)],
        );
        for (scope, expected) in [
            ("acmeCo/", vec!["acmeCo/", "sharedCo/"]),
            ("betaCo/", vec!["betaCo/"]),
            ("unknownCo/", vec![]),
        ] {
            let scoped = bearer(None, None)
                .with_scope_header(&headers(scope))
                .unwrap();
            let subject = scoped.result().unwrap().subject();
            let prefixes = tables::UserGrant::reachable_prefixes(
                &snapshot.role_grants,
                &snapshot.user_grants,
                &subject,
            );
            assert_eq!(prefixes.keys().copied().collect::<Vec<_>>(), expected);
        }
        let scoped = bearer(None, Some(vec![]))
            .with_scope_header(&headers("acmeCo/"))
            .unwrap();
        let subject = scoped.result().unwrap().subject();
        assert!(
            tables::UserGrant::reachable_prefixes(
                &snapshot.role_grants,
                &snapshot.user_grants,
                &subject
            )
            .is_empty()
        );
    }

    #[tokio::test]
    async fn test_scope_header_http() {
        use futures::StreamExt;
        use models::Capability::{Admin, Read};
        let user = uuid::Uuid::from_bytes([0x11; 16]);
        let snapshot = crate::test_server::snapshot_of_grants(
            &[(user, "acmeCo/", Admin), (user, "betaCo/", Admin)],
            &[("acmeCo/", "sharedCo/", Read)],
        );
        let source = tokens::StreamSource::new(
            futures::stream::iter([Ok(snapshot)]).chain(futures::stream::pending()),
        );
        // These routes only need the grant snapshot or reject before database IO.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let server =
            crate::test_server::TestServer::start(pool, tokens::watch(source).ready_owned().await)
                .await;
        let token = server.make_access_token(user, None);
        let url = format!("http://{}/api/graphql", server.addr);
        let client = reqwest::Client::new();
        let query = serde_json::json!({"query": "{ prefixes(by: { minCapability: read }) { edges { node { prefix } } } }"});
        for (scope, expected) in [
            (None, vec!["acmeCo/", "betaCo/", "sharedCo/"]),
            (Some("acmeCo/"), vec!["acmeCo/", "sharedCo/"]),
            (Some("betaCo/"), vec!["betaCo/"]),
        ] {
            let mut request = client.post(&url).bearer_auth(&token).json(&query);
            if let Some(scope) = scope {
                request = request.header(SCOPE_PREFIX_HEADER, scope);
            }
            let response = request.send().await.unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            let body: serde_json::Value = response.json().await.unwrap();
            assert!(body.get("errors").is_none(), "{body}");
            let prefixes = body["data"]["prefixes"]["edges"]
                .as_array()
                .unwrap()
                .iter()
                .map(|edge| edge["node"]["prefix"].as_str().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(prefixes, expected);
        }
        let restricted =
            server.make_restricted_access_token(user, None, None, Some("betaCo/".to_string()));
        for (bearer, scope, status) in [
            (Some(token.as_str()), "", reqwest::StatusCode::BAD_REQUEST),
            (
                Some(restricted.as_str()),
                "acmeCo/",
                reqwest::StatusCode::BAD_REQUEST,
            ),
            (None, "acmeCo/", reqwest::StatusCode::UNAUTHORIZED),
        ] {
            let mut request = client
                .post(&url)
                .header(SCOPE_PREFIX_HEADER, scope)
                .json(&query);
            if let Some(bearer) = bearer {
                request = request.bearer_auth(bearer);
            }
            assert_eq!(request.send().await.unwrap().status(), status);
        }
        let body: serde_json::Value = client
            .post(&url)
            .bearer_auth(&token)
            .header(SCOPE_PREFIX_HEADER, "acmeCo/")
            .json(&serde_json::json!({"query": "mutation { createRefreshToken { id } }"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            body["errors"][0]["message"],
            "tokens with a prefix scope set cannot create refresh tokens."
        );

        let preflight = client
            .request(reqwest::Method::OPTIONS, &url)
            .header("Origin", server.addr.to_string())
            .header("Access-Control-Request-Method", "POST")
            .header("Access-Control-Request-Headers", SCOPE_PREFIX_HEADER)
            .send()
            .await
            .unwrap();
        assert!(preflight.status().is_success());
        assert!(
            preflight.headers()["access-control-allow-headers"]
                .to_str()
                .unwrap()
                .to_ascii_lowercase()
                .contains(SCOPE_PREFIX_HEADER)
        );
    }
}
