use std::sync::Arc;

/// MaybeControlClaims wraps an optional Verified<ControlClaims> and represents
/// the presence or absence of a verified control-plane bearer token.
#[derive(Debug)]
pub struct MaybeControlClaims(Option<tokens::jwt::Verified<crate::ControlClaims>>);

impl MaybeControlClaims {
    pub fn with_verified(verified: tokens::jwt::Verified<crate::ControlClaims>) -> Self {
        Self(Some(verified))
    }

    pub fn with_unauthenticated() -> Self {
        Self(None)
    }

    pub fn result(&self) -> tonic::Result<&crate::ControlClaims> {
        match &self.0 {
            Some(verified) => Ok(verified.claims()),
            None => Err(tonic::Status::unauthenticated(
                "This is an authenticated API but the request is missing a required Authorization: Bearer token",
            )),
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
    /// Database pool to use during request processing.
    pub pg_pool: sqlx::PgPool,
}

impl Envelope {
    /// Returns verified ControlClaims or an unauthenticated error.
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
            };

            Ok(Envelope {
                maybe_claims,
                retry_after: retry_after.unwrap_or(tokens::DateTime::UNIX_EPOCH),
                refresh: state.snapshot.token(),
                started: started.unwrap_or_else(|| tokens::now()),
                pg_pool: state.pg_pool.clone(),
                original_uri,
            })
        }
    }
}

// Empty impl allows aide to generate OpenAPI specs for handlers using this extractor.
// The extractor is an internal detail and doesn't appear in the API documentation.
impl aide::operation::OperationInput for Envelope {}
