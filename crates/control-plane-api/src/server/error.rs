/// AuthZRetry represents a provisional authorization failure that must
/// be retried by the client at a later time (after a Snapshot refresh).
#[derive(Debug)]
pub struct AuthZRetry {
    /// The original request URI.
    pub original_uri: axum::http::Uri,
    /// DateTime at which the logical request was started.
    /// This may be client-provided or initialized by this server, and is held
    /// constant throughout retries.
    pub started: tokens::DateTime,
    /// DateTime of this provisional authorization failure, as measured by this server.
    pub failed: tokens::DateTime,
    /// The DateTime after which the request can be retried by the client.
    pub retry_after: tokens::DateTime,
    /// The Status representing specifics of the authorization failure.
    pub status: tonic::Status,
}

impl AuthZRetry {
    /// Generate a 307 Temporary Redirect response for this authorization retry.
    pub fn to_response(&self) -> axum::response::Response {
        let mut builder =
            axum::response::Response::builder().status(axum::http::StatusCode::TEMPORARY_REDIRECT);
        let headers = builder.headers_mut().unwrap();

        // Build Location header, replacing any existing `started` or `retryAfter` parameters.
        let mut location = String::new();
        location.push_str(self.original_uri.path());
        location.push('?');

        let filtered_query = self
            .original_uri
            .query()
            .iter()
            .flat_map(|query| {
                query
                    .split('&')
                    .filter(|p| !p.starts_with("started=") && !p.starts_with("retryAfter="))
            })
            .collect::<Vec<_>>();

        if !filtered_query.is_empty() {
            location.push_str(&filtered_query.join("&"));
            location.push('&');
        };

        // Format `started` and `retryAfter` as RFC 3339 timestamps with millisecond precision.
        // Use 'Z' (Zulu) to mark UTC, as it's trivially URL-safe ('+00' is not).
        let started_3339 = self
            .started
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

        location.push_str("started=");
        location.push_str(&started_3339);

        if self.retry_after != tokens::DateTime::UNIX_EPOCH {
            let retry_after_3339 = self
                .retry_after
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

            location.push_str("&retryAfter=");
            location.push_str(&retry_after_3339);

            headers.insert(
                axum::http::header::RETRY_AFTER,
                self.retry_after.to_rfc2822().parse().unwrap(),
            );
        }

        headers.insert(
            axum::http::header::DATE,
            self.failed.to_rfc2822().parse().unwrap(),
        );
        headers.insert(axum::http::header::LOCATION, location.parse().unwrap());

        let body = axum::body::Body::from(format!(
            "provisional {:?} error: {}",
            self.status.code(),
            self.status.message()
        ));

        builder.body(body).unwrap()
    }
}

/// Forbidden is the structured body of a definitive capability-mask denial:
/// a `403` whose outcome is a pure function of the bearer's verified claims,
/// so no Snapshot refresh or client retry can change it. The body is
/// machine-readable so a client (such as an agent broker) can parse the
/// missing capability names and mint a token which enables them.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Forbidden {
    /// Stable machine-readable code: `missing_capabilities` when the
    /// bearer's mask does not enable required capabilities, or
    /// `unmasked_token_required` when the operation refuses masked bearers
    /// outright (which no re-mint can remedy).
    pub error: &'static str,
    /// Human-readable description of the refusal.
    pub message: String,
    /// PascalCase names of capabilities which are required but not enabled
    /// by the bearer's mask, in `Capability` declaration order. Empty for
    /// `unmasked_token_required`.
    pub missing_capabilities: Vec<String>,
}

impl Forbidden {
    /// The denial for a mask which withholds `missing` bits required to
    /// access `prefix_or_name`. The message names the withheld bits in claim
    /// vocabulary and the target, and nothing else: it must never disclose
    /// whether the user holds a grant there.
    pub fn missing_capabilities(
        missing: models::authz::CapabilitySet,
        prefix_or_name: &str,
    ) -> Self {
        let missing = missing
            .iter()
            .map(|bit| bit.to_string())
            .collect::<Vec<_>>();
        Self {
            error: "missing_capabilities",
            message: format!(
                "token does not enable capabilities [{}] required to access prefix or name '{prefix_or_name}'",
                missing.join(", "),
            ),
            missing_capabilities: missing,
        }
    }

    /// The mask-shortfall pre-check shared by every authorization policy
    /// function. A non-empty shortfall is a property of the token alone: no
    /// grant could authorize the request under this mask, so it's evaluated
    /// before the grant walk and never consults it. An empty shortfall
    /// defers to the walk, whose denial reads as it does for an unmasked
    /// bearer.
    pub fn required_covered(
        mask: models::authz::CapabilityMask,
        required: impl Into<models::authz::CapabilitySet>,
        prefix_or_name: &str,
    ) -> Result<(), Self> {
        let required: models::authz::CapabilitySet = required.into();
        let missing = required - mask.apply(required);
        if missing.is_empty() {
            return Ok(());
        }
        Err(Self::missing_capabilities(missing, prefix_or_name))
    }

    /// The masked-bearer refusal shared by every surface which demands a
    /// full-authority credential, keyed on the *presence* of the
    /// `capability_mask` claim and never its value: a mask which happens to
    /// enable everything is still a deliberately-reduced credential.
    pub fn require_unmasked(claims: &crate::ControlClaims) -> Result<(), Self> {
        if claims.capability_mask.is_some() {
            return Err(Self::unmasked_token_required());
        }
        Ok(())
    }

    pub fn unmasked_token_required() -> Self {
        Self {
            error: "unmasked_token_required",
            message: "this operation requires a full-authority token, but the bearer token carries a capability mask".to_string(),
            missing_capabilities: Vec::new(),
        }
    }
}

impl axum::response::IntoResponse for Forbidden {
    fn into_response(self) -> axum::response::Response {
        (axum::http::StatusCode::FORBIDDEN, axum::Json(self)).into_response()
    }
}

/// AuthZError is the error of an authorization policy evaluation. The
/// distinction between its variants is load-bearing for
/// [`crate::Envelope::authorization_outcome`]: a `Retriable` denial may be
/// provisional — the Snapshot may simply not yet reflect a recently-committed
/// grant — and enters the refresh-and-retry machinery, while a `Definitive`
/// denial is a pure function of the bearer's verified claims (its capability
/// mask), which no future Snapshot can change, and fails immediately with the
/// structured `403` body.
#[derive(Debug)]
pub enum AuthZError {
    Retriable(tonic::Status),
    Definitive(Forbidden),
}

impl From<tonic::Status> for AuthZError {
    fn from(status: tonic::Status) -> Self {
        Self::Retriable(status)
    }
}

impl From<Forbidden> for AuthZError {
    fn from(forbidden: Forbidden) -> Self {
        Self::Definitive(forbidden)
    }
}

#[cfg(test)]
impl AuthZError {
    /// Map to the (HTTP status, message) pair which handler test harnesses
    /// snapshot as their error shape.
    pub(crate) fn into_status_message(self) -> (u16, String) {
        match self {
            Self::Retriable(status) => (
                tokens::rest::grpc_status_code_to_http(status.code()),
                status.message().to_string(),
            ),
            Self::Definitive(forbidden) => (403, forbidden.message),
        }
    }
}

/// ApiError is the fundamental error type returned by the API.
/// It distinguishes between a terminal Status error vs a provisional
/// authorization failure that the client may retry.
#[derive(Debug)]
pub enum ApiError {
    Status(tonic::Status),
    AuthZRetry(AuthZRetry),
    /// A definitive capability-mask denial, carrying the structured `403`
    /// body. Unlike Status denials it is never provisional: it's a pure
    /// function of the bearer's verified claims, so no Snapshot refresh or
    /// client retry can change the outcome.
    Forbidden(Forbidden),
}

/// A policy error which escaped [`crate::Envelope::authorization_outcome`]
/// is terminal as-is: a Retriable status is a plain Status response.
impl From<AuthZError> for ApiError {
    fn from(error: AuthZError) -> Self {
        match error {
            AuthZError::Retriable(status) => Self::Status(status),
            AuthZError::Definitive(forbidden) => Self::Forbidden(forbidden),
        }
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(error: sqlx::Error) -> ApiError {
        tracing::error!(?error, "API responding with database error");

        ApiError::Status(tonic::Status::internal(
            "database error, please retry the request",
        ))
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        let status = match error.downcast::<tonic::Status>() {
            Ok(status) => status,
            Err(err) => tonic::Status::unknown(format!("{err:#}")),
        };
        ApiError::Status(status)
    }
}

impl From<tonic::Status> for ApiError {
    fn from(status: tonic::Status) -> Self {
        ApiError::Status(status)
    }
}

impl From<super::Rejection> for ApiError {
    fn from(value: super::Rejection) -> Self {
        let message = format!("{:#}", anyhow::Error::from(value));
        Self::Status(tonic::Status::invalid_argument(message))
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::Status(status) => crate::status_into_response(status),
            Self::AuthZRetry(retry) => retry.to_response(),
            Self::Forbidden(forbidden) => forbidden.into_response(),
        }
    }
}

impl From<ApiError> for async_graphql::Error {
    fn from(api_error: ApiError) -> Self {
        let mut err = match &api_error {
            ApiError::Status(status) => {
                Self::new(format!("{:?}: {}", status.code(), status.message()))
            }
            ApiError::AuthZRetry(retry) => Self::new(format!(
                "{:?}: {}",
                retry.status.code(),
                retry.status.message()
            )),
            // Carry the structured 403 body in error extensions, so that
            // "you need capability X" is machine-readable identically on the
            // REST and GraphQL surfaces.
            ApiError::Forbidden(forbidden) => {
                let mut err = Self::new(forbidden.message.clone());
                let mut extensions = async_graphql::ErrorExtensionValues::default();
                extensions.set("error", forbidden.error);
                extensions.set(
                    "missing_capabilities",
                    forbidden.missing_capabilities.clone(),
                );
                err.extensions = Some(extensions);
                err
            }
        };
        err.source = Some(std::sync::Arc::new(api_error));

        err
    }
}

// Required for aide OpenAPI generation - handlers returning Result<T, ApiError>
// need both T and ApiError to implement OperationOutput.
impl aide::operation::OperationOutput for ApiError {
    type Inner = ();
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// The structured 403 body, exactly as REST clients receive it: the
    /// machine-readable code, the message, and the withheld bits in claim
    /// vocabulary and `Capability` declaration order.
    #[tokio::test]
    async fn test_forbidden_response_body() {
        use models::authz::Capability::{CatalogRead, SpecEdit};

        let forbidden = Forbidden::missing_capabilities(SpecEdit | CatalogRead, "acmeCo/thing");
        let response = axum::response::IntoResponse::into_response(forbidden);
        let (parts, body) = response.into_parts();
        let body = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        insta::assert_snapshot!(
            format!(
                "{:?} {}\n{}",
                parts.status,
                parts.headers[axum::http::header::CONTENT_TYPE].to_str().unwrap(),
                String::from_utf8_lossy(&body)
            ),
            @r#"
        403 application/json
        {"error":"missing_capabilities","message":"token does not enable capabilities [CatalogRead, SpecEdit] required to access prefix or name 'acmeCo/thing'","missing_capabilities":["CatalogRead","SpecEdit"]}
        "#
        );

        insta::assert_json_snapshot!(Forbidden::unmasked_token_required(), @r#"
        {
          "error": "unmasked_token_required",
          "message": "this operation requires a full-authority token, but the bearer token carries a capability mask",
          "missing_capabilities": []
        }
        "#);
    }

    /// `required_covered` is a function of the mask alone, and "masked" is
    /// the claim's presence: an empty mask and an all-enabling mask are both
    /// refused by `require_unmasked`, while only the empty one fails coverage.
    #[test]
    fn test_forbidden_predicates() {
        use models::authz::{Capability, CapabilityBundle, CapabilityMask};

        let viewer = CapabilityMask::new(CapabilityBundle::Viewer.capabilities());
        assert_eq!(
            Forbidden::required_covered(viewer, Capability::CatalogRead, "acmeCo/"),
            Ok(())
        );
        let forbidden =
            Forbidden::required_covered(viewer, models::Capability::Admin, "acmeCo/").unwrap_err();
        assert_eq!(forbidden.error, "missing_capabilities");
        assert!(
            forbidden
                .missing_capabilities
                .contains(&"SpecEdit".to_string())
        );
        assert!(
            !forbidden
                .missing_capabilities
                .contains(&"CatalogRead".to_string())
        );
        assert_eq!(
            Forbidden::required_covered(
                CapabilityMask::ALL_CAPABILITIES,
                models::Capability::Admin,
                "acmeCo/"
            ),
            Ok(())
        );

        let claims =
            |mask| crate::test_server::control_claims(crate::test_server::ALICE, None, mask);
        assert_eq!(Forbidden::require_unmasked(&claims(None)), Ok(()));
        assert_eq!(
            Forbidden::require_unmasked(&claims(Some(vec![]))),
            Err(Forbidden::unmasked_token_required())
        );
        assert_eq!(
            Forbidden::require_unmasked(&claims(Some(vec!["Admin".to_string()]))),
            Err(Forbidden::unmasked_token_required())
        );
    }

    #[tokio::test]
    async fn test_authz_retry_to_response() {
        for (name, uri, status_code, status_msg) in [
            (
                "path_only",
                "/api/test",
                tonic::Code::PermissionDenied,
                "not allowed",
            ),
            (
                "with_query",
                "/api/test?foo=bar",
                tonic::Code::Unauthenticated,
                "bad token",
            ),
            (
                "replaces_existing_started",
                "/api/test?foo=bar&started=2023-01-01T00:00:00.000Z",
                tonic::Code::PermissionDenied,
                "retry",
            ),
            (
                "replaces_existing_retry_after",
                "/api/test?retryAfter=2023-01-01T00:00:00.000Z&baz=qux",
                tonic::Code::PermissionDenied,
                "retry",
            ),
            (
                "replaces_both_existing",
                "/api/test?started=2023-01-01T00:00:00.000Z&retryAfter=2023-01-01T00:00:00.000Z",
                tonic::Code::PermissionDenied,
                "retry",
            ),
            (
                "replaces_both_preserves_other",
                "/api/test?a=1&started=old&b=2&retryAfter=old&c=3",
                tonic::Code::PermissionDenied,
                "retry",
            ),
        ] {
            let retry = AuthZRetry {
                original_uri: uri.parse().unwrap(),
                started: chrono::Utc.with_ymd_and_hms(2024, 1, 15, 10, 0, 0).unwrap(),
                failed: chrono::Utc.with_ymd_and_hms(2024, 1, 15, 10, 0, 5).unwrap(),
                retry_after: chrono::Utc
                    .with_ymd_and_hms(2024, 1, 15, 10, 0, 10)
                    .unwrap(),
                status: tonic::Status::new(status_code, status_msg),
            };

            let response = retry.to_response();
            let (parts, body) = response.into_parts();
            let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
            let body_str = String::from_utf8_lossy(&body_bytes);

            let mut headers = parts.headers.iter().collect::<Vec<_>>();
            headers.sort_by_key(|(name, _)| name.as_str());

            insta::assert_snapshot!(
                name,
                format!(
                    "status: {:?}\nheaders: {:?}\nbody: {}",
                    parts.status, headers, body_str
                )
            );
        }
    }
}
