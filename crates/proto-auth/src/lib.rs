pub mod token;
pub use token::{TokenSource, TokenStream};

mod bearer;
pub use bearer::BearerToken;

pub mod jwt;

/// Extract the bearer token from a tonic MetadataMap.
///
/// Returns `Ok(token)` if a valid "Bearer <token>" authorization header is present,
/// or an appropriate `tonic::Status` error otherwise.
pub fn extract_bearer_token(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<&[u8], tonic::Status> {
    let token = metadata
        .get("authorization")
        .ok_or_else(|| tonic::Status::unauthenticated("missing authorization header"))?
        .as_bytes();

    let token = token.strip_prefix(b"Bearer ").ok_or_else(|| {
        tonic::Status::unauthenticated("authorization header missing Bearer prefix")
    })?;

    Ok(token)
}

pub fn reqwest_error_to_tonic_status(err: reqwest::Error) -> tonic::Status {
    let code = if err.is_connect() || err.is_timeout() {
        tonic::Code::Unavailable
    } else if let Some(status) = err.status() {
        if status.as_u16() == 401 {
            tonic::Code::Unauthenticated
        } else if status.as_u16() == 403 {
            tonic::Code::PermissionDenied
        } else if status.is_client_error() {
            tonic::Code::InvalidArgument
        } else {
            tonic::Code::Unknown
        }
    } else {
        tonic::Code::Internal
    };

    tonic::Status::new(code, format!("{err}"))
}

const MINUTE: std::time::Duration = std::time::Duration::from_secs(60);
