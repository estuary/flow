/// BearerToken is a tonic Interceptor that adds a Bearer token to gRPC metadata.
#[derive(Clone, Debug)]
pub struct BearerToken(tonic::metadata::AsciiMetadataValue);

impl BearerToken {
    /// Create a new BearerToken from a raw ASCII token.
    pub fn new(token: &str) -> Result<Self, tonic::Status> {
        let mut token = format!("Bearer {token}")
            .parse::<tonic::metadata::AsciiMetadataValue>()
            .map_err(|e: tonic::metadata::errors::InvalidMetadataValue| {
                tonic::Status::internal(e.to_string())
            })?;

        token.set_sensitive(true);
        Ok(Self(token))
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl tonic::service::Interceptor for BearerToken {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, tonic::Status> {
        request
            .metadata_mut()
            .insert("authorization", self.0.clone());
        Ok(request)
    }
}
