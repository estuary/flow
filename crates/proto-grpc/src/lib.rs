pub mod capture;
pub mod consumer;
pub mod derive;
pub mod flow;
pub mod materialize;
mod protocol;
pub mod runtime;
pub mod shuffle;

// The `protocol` package is publicly exported as `broker`.
#[cfg(any(feature = "broker_client", feature = "broker_server"))]
pub mod broker {
    pub use crate::protocol::*;
}

/// Metadata is a tonic Interceptor that adds metadata to gRPC requests.
#[derive(Clone, Debug, Default)]
pub struct Metadata(pub tonic::metadata::MetadataMap);

impl Metadata {
    pub fn new() -> Self {
        Self(tonic::metadata::MetadataMap::new())
    }

    pub fn with_bearer_token(mut self, token: &str) -> tonic::Result<Self> {
        let mut token = format!("Bearer {token}")
            .parse::<tonic::metadata::AsciiMetadataValue>()
            .map_err(|e: tonic::metadata::errors::InvalidMetadataValue| {
                tonic::Status::invalid_argument(e.to_string())
            })?;

        token.set_sensitive(true);
        self.0.insert("authorization", token);

        Ok(self)
    }
}

impl tonic::service::Interceptor for Metadata {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, tonic::Status> {
        let out = request.metadata_mut();
        out.reserve(self.0.len());

        for entry in self.0.iter() {
            match entry {
                tonic::metadata::KeyAndValueRef::Ascii(key, value) => {
                    _ = out.insert(key, value.clone());
                }
                tonic::metadata::KeyAndValueRef::Binary(key, value) => {
                    _ = out.insert_bin(key, value.clone());
                }
            }
        }
        Ok(request)
    }
}

/// Extract a bearer token from a tonic MetadataMap.
///
/// Returns `Ok(token)` if a valid "Bearer <token>" authorization header is present,
/// or an appropriate `tonic::Status` error otherwise.
pub fn extract_bearer(metadata: &tonic::metadata::MetadataMap) -> Result<&[u8], tonic::Status> {
    let token = metadata
        .get("authorization")
        .ok_or_else(|| tonic::Status::unauthenticated("missing authorization header"))?
        .as_bytes();

    let token = token.strip_prefix(b"Bearer ").ok_or_else(|| {
        tonic::Status::unauthenticated("authorization header missing Bearer prefix")
    })?;

    Ok(token)
}
