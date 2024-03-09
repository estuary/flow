#[derive(Clone)]
pub struct Auth(Option<tonic::metadata::AsciiMetadataValue>);

impl Auth {
    pub fn new(bearer_token: Option<String>) -> crate::Result<Self> {
        let auth_header = if let Some(token) = bearer_token {
            Some(
                format!("Bearer {}", &token)
                    .parse()
                    .map_err(crate::Error::BearerToken)?,
            )
        } else {
            None
        };

        Ok(Self(auth_header))
    }
}

impl tonic::service::Interceptor for Auth {
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
