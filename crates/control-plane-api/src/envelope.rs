use std::sync::Arc;

use crate::AuthZResult;

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
            // The request carries no verified bearer, or the policy otherwise
            // rejected it as unauthenticated. That is a property of the request
            // and not of the Snapshot: no refresh can cure it, so it must never
            // enter the provisional path below, which would revoke the Snapshot
            // and redirect an anonymous caller to retry.
            Err(status) if status.code() == tonic::Code::Unauthenticated => {
                return Err(status.into());
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

    /// Errors unless the current user holds `capability` on `prefix`.
    ///
    /// This is the hard gate for mutations and access-controlled queries: a denial
    /// becomes `permission_denied`, and a provisional denial against a stale
    /// Snapshot follows the standard refresh-and-retry path.
    ///
    /// `capability` accepts a legacy `models::Capability`, an orthogonal
    /// `models::authz::Capability` bit, or a `models::authz::CapabilitySet`.
    pub async fn verify_authorization(
        &self,
        prefix: &str,
        capability: impl Into<models::authz::CapabilitySet> + std::fmt::Display + Copy,
    ) -> Result<(), crate::ApiError> {
        let policy_result = self.evaluate_names_authorization([prefix], capability);
        let (_expiry, ()) = self.authorization_outcome(policy_result).await?;
        Ok(())
    }

    /// This does the same thing as verify authorization just for a list of
    /// prefixes instead of one.
    pub async fn verify_authorization_iter<Iter, S>(
        &self,
        prefixes: Iter,
        capability: impl Into<models::authz::CapabilitySet> + std::fmt::Display + Copy,
    ) -> Result<(), crate::ApiError>
    where
        Iter: IntoIterator<Item = S>,
        S: AsRef<str> + std::fmt::Display,
    {
        let policy_result = self.evaluate_names_authorization(prefixes, capability);
        let (_expiry, ()) = self.authorization_outcome(policy_result).await?;
        Ok(())
    }

    /// Evaluate whether the user identified by `claims` is authorized to access all
    /// of the enumerated `prefixes_or_names` with at least `min_capability`.
    /// Return a policy_result shape which fits Envelope::authorization_outcome.
    ///
    /// `min_capability` accepts any value that converts into a `CapabilitySet`:
    /// legacy `models::Capability` (mapped via `bits_for_legacy`), a single
    /// `models::authz::Capability` bit, or an explicit `CapabilitySet`.
    pub(crate) fn evaluate_names_authorization<Iter, S, C>(
        &self,
        prefixes_or_names: Iter,
        min_capability: C,
    ) -> AuthZResult<()>
    where
        Iter: IntoIterator<Item = S>,
        S: AsRef<str> + std::fmt::Display,
        C: Into<models::authz::CapabilitySet> + std::fmt::Display + Copy,
    {
        let claims = self.claims()?;
        let models::authorizations::ControlClaims {
            sub: user_id,
            email: user_email,
            ..
        } = claims;
        let user_email = user_email.as_ref().map(String::as_str).unwrap_or("user");
        let snapshot = self.snapshot();
        for prefix_or_name in prefixes_or_names.into_iter() {
            if !tables::UserGrant::is_authorized(
                &snapshot.role_grants,
                &snapshot.user_grants,
                *user_id,
                prefix_or_name.as_ref(),
                min_capability,
            ) {
                return Err(tonic::Status::permission_denied(format!(
                    "{user_email} is not authorized to access prefix or name '{prefix_or_name}' with required capability {min_capability}",
                )));
            }
        }
        Ok((None, ()))
    }

    /// Errors unless the current user may bind `data_plane_names` into a storage
    /// mapping under `catalog_prefix`. That requires two grants evaluated as one
    /// policy against a single Snapshot: the user holds Admin on the prefix, and
    /// the prefix itself holds Read on every named data plane. Both halves pass
    /// through one authorization outcome so a stale denial of either yields a
    /// single refresh-and-retry decision.
    pub async fn verify_storage_mapping_authorization(
        &self,
        catalog_prefix: &models::Prefix,
        data_plane_names: &[String],
    ) -> Result<(), crate::ApiError> {
        let policy_result =
            self.evaluate_storage_mapping_authorization(catalog_prefix, data_plane_names);
        self.authorization_outcome(policy_result).await?;
        Ok(())
    }

    /// The pure policy half of [`Self::verify_storage_mapping_authorization`].
    /// Returns a policy result shaped for [`Self::authorization_outcome`].
    fn evaluate_storage_mapping_authorization(
        &self,
        catalog_prefix: &models::Prefix,
        data_plane_names: &[String],
    ) -> crate::AuthZResult<()> {
        let models::authorizations::ControlClaims {
            sub: user_id,
            email: user_email,
            ..
        } = self.claims()?;
        let user_email = user_email.as_ref().map(String::as_str).unwrap_or("user");
        let snapshot = self.snapshot();
        // Verify the User admins `catalog_prefix`.
        if !tables::UserGrant::is_authorized(
            &snapshot.role_grants,
            &snapshot.user_grants,
            *user_id,
            catalog_prefix,
            models::Capability::Admin,
        ) {
            return Err(tonic::Status::permission_denied(format!(
                "{user_email} is not an authorized as an Admin of catalog prefix '{catalog_prefix}'",
            )));
        }

        for data_plane_name in data_plane_names {
            // Verify `catalog_prefix` is authorized to access the data-plane for Read.
            if !tables::RoleGrant::is_authorized(
                &snapshot.role_grants,
                catalog_prefix,
                data_plane_name,
                models::Capability::Read,
            ) {
                return Err(tonic::Status::permission_denied(format!(
                    "'{catalog_prefix}' is not authorized to data plane '{data_plane_name}' for Read",
                )));
            }
        }

        Ok((None, ()))
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
mod tests {
    use super::*;
    use crate::test_server::snapshot_of_grants;
    use models::Capability::{Admin, Read};

    const USER: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);

    /// Verified claims for `USER`. `Verified` can only come out of
    /// `tokens::jwt::verify`, so the claims take a sign-then-verify round trip
    /// through a throwaway secret.
    fn verified_claims() -> MaybeControlClaims {
        // The verifier skims claims as i64, so `exp` must stay within range.
        let now = tokens::now();
        let claims = models::authorizations::ControlClaims {
            iat: now.timestamp() as u64,
            exp: (now + chrono::TimeDelta::hours(1)).timestamp() as u64,
            sub: USER,
            role: "authenticated".to_string(),
            aud: "authenticated".to_string(),
            email: Some("user@example.test".to_string()),
        };
        let secret = b"envelope-test-secret";
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(secret),
        )
        .unwrap();
        let verified = tokens::jwt::verify::<crate::ControlClaims>(
            token.as_bytes(),
            0,
            &[jsonwebtoken::DecodingKey::from_secret(secret)],
        )
        .unwrap();
        MaybeControlClaims::with_verified(verified)
    }

    /// Build an Envelope over `snapshot` as a request would carry it.
    ///
    /// The Snapshot is stamped as taken now. Whether a denial is terminal or
    /// provisional then hinges on `started`: a request started before `taken`
    /// sees a terminal denial, while one started after it takes the provisional
    /// refresh-and-retry path. `retry_after` is left at the epoch so that path
    /// returns `AuthZRetry` immediately instead of awaiting a refresh which the
    /// fixed watch never delivers.
    async fn envelope_for_test(
        mut snapshot: crate::Snapshot,
        maybe_claims: MaybeControlClaims,
        started: tokens::DateTime,
    ) -> Envelope {
        snapshot.taken = tokens::now();
        let refresh = tokens::fixed(Ok(snapshot)).ready_owned().await.token();

        Envelope {
            original_uri: axum::http::Uri::from_static("/test"),
            maybe_claims,
            retry_after: tokens::DateTime::UNIX_EPOCH,
            refresh,
            started,
            started_set: false,
            pg_pool: sqlx::PgPool::connect_lazy("postgres://unused.invalid/unused").unwrap(),
            locale: Locale::EnUS,
        }
    }

    /// An authenticated Envelope whose denials are terminal.
    async fn authenticated_envelope(snapshot: crate::Snapshot) -> Envelope {
        envelope_for_test(snapshot, verified_claims(), tokens::DateTime::UNIX_EPOCH).await
    }

    /// A request which started after the Snapshot was taken, so that a denial
    /// is provisional rather than terminal.
    fn started_after_snapshot() -> tokens::DateTime {
        tokens::now() + chrono::TimeDelta::hours(1)
    }

    fn denied_message(err: crate::ApiError) -> String {
        match err {
            crate::ApiError::Status(status) => {
                assert_eq!(status.code(), tonic::Code::PermissionDenied);
                status.message().to_string()
            }
            crate::ApiError::AuthZRetry(retry) => {
                panic!("expected a terminal denial, got provisional retry {retry:?}")
            }
        }
    }

    /// The `ops/` admin gate of the /admin/* endpoints, evaluated as a bare
    /// policy result over the Snapshot's grant walk, before any outcome handling.
    #[tokio::test]
    async fn test_evaluate_ops_admin_gate() {
        // A direct admin grant to `ops/` is authorized.
        let env = authenticated_envelope(snapshot_of_grants(&[(USER, "ops/", Admin)], &[])).await;
        assert!(env.evaluate_names_authorization(["ops/"], Admin).is_ok());

        // Admin of an unrelated tenant is denied.
        let env =
            authenticated_envelope(snapshot_of_grants(&[(USER, "acmeCo/", Admin)], &[])).await;
        let status = env
            .evaluate_names_authorization(["ops/"], Admin)
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert_eq!(
            status.message(),
            "user@example.test is not authorized to access prefix or name 'ops/' with required capability admin"
        );

        // A read grant to `ops/` is not admin.
        let env = authenticated_envelope(snapshot_of_grants(&[(USER, "ops/", Read)], &[])).await;
        assert!(env.evaluate_names_authorization(["ops/"], Admin).is_err());
    }

    #[tokio::test]
    async fn test_verify_authorization_admits_admin_grant() {
        let env = authenticated_envelope(snapshot_of_grants(&[(USER, "ops/", Admin)], &[])).await;
        env.verify_authorization("ops/", Admin).await.unwrap();
    }

    #[tokio::test]
    async fn test_verify_authorization_denies_as_permission_denied() {
        let env =
            authenticated_envelope(snapshot_of_grants(&[(USER, "acmeCo/", Admin)], &[])).await;

        let err = env.verify_authorization("ops/", Admin).await.unwrap_err();
        assert_eq!(
            denied_message(err),
            "user@example.test is not authorized to access prefix or name 'ops/' with required capability admin"
        );

        // A grant below the required capability is likewise denied.
        let env = authenticated_envelope(snapshot_of_grants(&[(USER, "ops/", Read)], &[])).await;
        let err = env.verify_authorization("ops/", Admin).await.unwrap_err();
        denied_message(err);
    }

    #[tokio::test]
    async fn test_verify_authorization_iter_requires_every_name() {
        let env = authenticated_envelope(snapshot_of_grants(
            &[(USER, "acmeCo/", Read), (USER, "bobCo/", Read)],
            &[],
        ))
        .await;

        env.verify_authorization_iter(["acmeCo/foo", "bobCo/bar"], Read)
            .await
            .unwrap();

        // The first unauthorized name fails the whole set.
        let err = env
            .verify_authorization_iter(["acmeCo/foo", "carolCo/baz", "bobCo/bar"], Read)
            .await
            .unwrap_err();
        assert_eq!(
            denied_message(err),
            "user@example.test is not authorized to access prefix or name 'carolCo/baz' with required capability read"
        );
    }

    #[tokio::test]
    async fn test_unauthenticated_escapes_provisional_path() {
        // No bearer and a request started after the Snapshot: the geometry that
        // would otherwise take the provisional path.
        let env = envelope_for_test(
            snapshot_of_grants(&[(USER, "ops/", Admin)], &[]),
            MaybeControlClaims::with_unauthenticated(),
            started_after_snapshot(),
        )
        .await;

        match env.verify_authorization("ops/", Admin).await.unwrap_err() {
            crate::ApiError::Status(status) => {
                assert_eq!(status.code(), tonic::Code::Unauthenticated)
            }
            crate::ApiError::AuthZRetry(retry) => {
                panic!("unauthenticated request must not be told to retry: {retry:?}")
            }
        }
        // The dispatch must not have revoked the Snapshot: nothing about it
        // could cure a missing bearer.
        assert!(!env.snapshot().revoke.is_cancelled());
    }

    #[tokio::test]
    async fn test_denial_after_snapshot_stays_provisional() {
        // The same geometry with a real bearer: a denial may be stale, so the
        // request is redirected to retry and the Snapshot is revoked.
        let env = envelope_for_test(
            snapshot_of_grants(&[(USER, "acmeCo/", Admin)], &[]),
            verified_claims(),
            started_after_snapshot(),
        )
        .await;

        match env.verify_authorization("ops/", Admin).await.unwrap_err() {
            crate::ApiError::AuthZRetry(retry) => {
                assert_eq!(retry.status.code(), tonic::Code::PermissionDenied)
            }
            crate::ApiError::Status(status) => {
                panic!("expected a provisional retry, got terminal {status:?}")
            }
        }
        assert!(env.snapshot().revoke.is_cancelled());
    }
}
