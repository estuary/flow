#[derive(Clone)]
pub struct Interceptor(Option<tonic::metadata::AsciiMetadataValue>);

impl Interceptor {
    pub fn new(
        bearer_token: Option<String>,
    ) -> Result<Self, tonic::metadata::errors::InvalidMetadataValue> {
        let auth_header = if let Some(token) = bearer_token {
            Some(format!("Bearer {}", &token).parse()?)
        } else {
            None
        };

        Ok(Self(auth_header))
    }
}

impl tonic::service::Interceptor for Interceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(bearer) = self.0.as_ref() {
            request
                .metadata_mut()
                .insert("authorization", bearer.clone());
        }
        Ok(request)
    }
}
