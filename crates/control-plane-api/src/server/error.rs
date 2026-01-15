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

/// ApiError is the fundamental error type returned by the API.
/// It distinguishes between a terminal Status error vs a provisional
/// authorization failure that the client may retry.
#[derive(Debug)]
pub enum ApiError {
    Status(tonic::Status),
    AuthZRetry(AuthZRetry),
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
        }
    }
}

impl From<ApiError> for async_graphql::Error {
    fn from(api_error: ApiError) -> Self {
        let status = match &api_error {
            ApiError::Status(status) => status,
            ApiError::AuthZRetry(retry) => &retry.status,
        };
        let message = format!("{:?}: {}", status.code(), status.message());

        let mut err = Self::new(message);
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
