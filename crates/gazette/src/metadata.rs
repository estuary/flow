use std::ops::{Deref, DerefMut};

#[derive(Clone, Debug, Default)]
pub struct Metadata(pub tonic::metadata::MetadataMap);

impl Metadata {
    pub fn new() -> Self {
        Self(tonic::metadata::MetadataMap::new())
    }

    /// Attaches an Authorization: Bearer token to the Metadata.
    pub fn with_bearer_token(mut self, token: &str) -> crate::Result<Self> {
        self.0.insert(
            "authorization",
            format!("Bearer {token}")
                .parse()
                .map_err(crate::Error::BearerToken)?,
        );
        Ok(self)
    }

    /// Sign claims into an JWT suited for use as an Authorization: Bearer token.
    pub fn with_signed_claims<S: AsRef<str>>(
        self,
        capability: u32,
        data_plane_fqdn: &str,
        duration: std::time::Duration,
        hmac_keys: &[S],
        selector: proto_gazette::broker::LabelSelector,
        subject: &str,
    ) -> crate::Result<Self> {
        let unix_ts = jsonwebtoken::get_current_timestamp();

        let claims = proto_gazette::Claims {
            sel: selector,
            cap: capability,
            sub: subject.to_string(),
            iat: unix_ts,
            exp: unix_ts + duration.as_secs(),
            iss: data_plane_fqdn.to_string(),
        };
        let Some(hmac_key) = hmac_keys.first() else {
            return Err(crate::Error::Protocol(
                "HMAC keys for signing claims cannot be empty",
            ));
        };

        let hmac_key = jsonwebtoken::EncodingKey::from_base64_secret(hmac_key.as_ref())?;
        let token = jsonwebtoken::encode(&jsonwebtoken::Header::default(), &claims, &hmac_key)?;

        self.with_bearer_token(&token)
    }
}

impl Deref for Metadata {
    type Target = tonic::metadata::MetadataMap;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for Metadata {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl tonic::service::Interceptor for Metadata {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        for entry in self.0.iter() {
            match entry {
                tonic::metadata::KeyAndValueRef::Ascii(key, value) => {
                    _ = request.metadata_mut().insert(key, value.clone());
                }
                tonic::metadata::KeyAndValueRef::Binary(key, value) => {
                    _ = request.metadata_mut().insert_bin(key, value.clone());
                }
            }
        }
        Ok(request)
    }
}
