use crate::{DateTime, Source, TimeDelta};

/// RestSource is a trait fulfilling Source via REST API requests and responses.
pub trait RestSource: Send + Sync {
    /// Model is the deserialized response type from the REST API.
    type Model: for<'de> serde::Deserialize<'de> + Send + Sync + 'static;
    /// Token type extracted from Model. Often the same type as Model.
    type Token: Send + Sync + 'static;

    /// Build an API request whose 200 OK response is a JSON serialization of Model.
    /// A server-side (5XX) error is logged and retried but is not client-facing.
    /// All other error statuses are mapped and surfaced as tonic::Status.
    fn build_request<'s>(
        &'s mut self,
        started: DateTime,
    ) -> impl std::future::Future<Output = Result<reqwest::RequestBuilder, tonic::Status>> + Send + 's;

    /// Extract a Token from a response Model. Returns:
    /// - Ok(Ok((token, valid_for))) if the token is ready for use and valid for the returned Duration.
    /// - Ok(Err(retry_after)) if the response model represents a server-directed client retry.
    /// - Err(status) if the response model is invalid.
    fn extract(
        response: Self::Model,
    ) -> Result<Result<(Self::Token, TimeDelta), TimeDelta>, tonic::Status>;
}

impl<R> Source for R
where
    R: RestSource + 'static,
{
    type Token = R::Token;
    type Revoke = std::future::Pending<()>;

    async fn refresh(
        &mut self,
        started: DateTime,
    ) -> tonic::Result<Result<(Self::Token, TimeDelta, Self::Revoke), TimeDelta>> {
        let request = self.build_request(started).await?;

        // If we need to retry, it will be in range 5-15 seconds (mean of 10s).
        let retry = TimeDelta::milliseconds(rand::random_range(5000..15000));

        let response = match request.send().await {
            Ok(r) => r,
            Err(err) => {
                // Network errors are logged and retried, but not returned.
                tracing::warn!(
                    ?err,
                    ?retry,
                    "REST token fetch failed to send request (will retry)"
                );
                return Ok(Err(retry));
            }
        };
        let status = response.status();

        let body = match response.bytes().await {
            Ok(b) => b,
            Err(err) => {
                tracing::warn!(
                    ?err,
                    ?retry,
                    "REST token fetch failed to read response body (will retry)"
                );
                return Ok(Err(retry));
            }
        };

        // Server errors (5XX) are logged and retried, but not returned.
        if status.is_server_error() {
            tracing::warn!(
                ?status,
                ?retry,
                body=%String::from_utf8_lossy(&body[..(body.len().min(128))]),
                "REST token fetch failed due to server error (will retry)"
            );
            return Ok(Err(retry));
        }
        // Map other non-success HTTP status codes into client-facing errors.
        let body = map_reqwest_status(status, body)?;

        let response: R::Model = serde_json::from_slice(&body).map_err(|err| {
            // We don't include `body` in the error as it may contain sensitive info.
            tonic::Status::internal(format!(
                "failed to deserialize token response (error {err}) from response body",
            ))
        })?;

        Self::extract(response).map(|r| r.map(|(token, dur)| (token, dur, std::future::pending())))
    }
}

fn map_reqwest_status(
    status: reqwest::StatusCode,
    body: bytes::Bytes,
) -> tonic::Result<bytes::Bytes> {
    if status.is_success() {
        return Ok(body);
    }

    let code = if status.as_u16() == 401 {
        tonic::Code::Unauthenticated
    } else if status.as_u16() == 403 {
        tonic::Code::PermissionDenied
    } else if status.is_client_error() {
        tonic::Code::InvalidArgument
    } else {
        tonic::Code::Unknown
    };

    Err(tonic::Status::new(
        code,
        format!(
            "HTTP status {status}: {}",
            String::from_utf8_lossy(&body[..(body.len().min(512))])
        ),
    ))
}
