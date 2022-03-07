use base64::display::Base64Display;
use base64::DecodeError;
use chrono::serde::ts_seconds;
use chrono::{DateTime, Duration, SubsecRound, Utc};
use hmac::digest::MacError;
use serde_json::value::RawValue;

use crate::services::signatures::MessageVerifier;

static ENCODING_CONFIG: base64::Config = base64::URL_SAFE_NO_PAD;

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("bad encoding")]
    BadEncoding(#[from] DecodeError),
    #[error("expired session")]
    Expired,
    #[error("invalid session")]
    InvalidMac(#[from] MacError),
    #[error("malformed token")]
    MalformedToken(#[from] serde_json::Error),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Token {
    credential_token: String,
    #[serde(with = "ts_seconds")]
    expires_at: DateTime<Utc>,
}

impl Token {
    pub fn new(credential_token: impl Into<String>) -> Self {
        // Duration functions are not const, otherwise this would be.
        let session_length = Duration::hours(12);

        Self {
            credential_token: credential_token.into(),
            expires_at: (Utc::now() + session_length).trunc_subsecs(0),
        }
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at < Utc::now()
    }

    pub fn expires_at(&self) -> &DateTime<Utc> {
        &self.expires_at
    }

    pub fn credential_token(&self) -> &str {
        &self.credential_token
    }
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
pub struct SignedToken {
    token: Box<RawValue>,
    #[serde_as(as = "serde_with::base64::Base64")]
    mac: Vec<u8>,
}

impl SignedToken {
    pub fn encode(&self) -> Result<String, SessionError> {
        let json = serde_json::to_vec(&self)?;
        Ok(Base64Display::with_config(&json, ENCODING_CONFIG).to_string())
    }

    pub fn decode(encoded: &str) -> Result<SignedToken, SessionError> {
        let bytes = base64::decode_config(encoded, ENCODING_CONFIG)?;
        let signed_token = serde_json::from_slice(&bytes)?;
        Ok(signed_token)
    }
}

impl MessageVerifier<Token> {
    pub fn sign_token(&self, token: &Token) -> Result<SignedToken, SessionError> {
        let json_token = serde_json::to_string(&token)?;
        let mac = self.sign(&json_token.as_bytes());

        Ok(SignedToken {
            token: RawValue::from_string(json_token)?,
            mac,
        })
    }

    pub fn verify_token(&self, signed_token: &SignedToken) -> Result<Token, SessionError> {
        self.verify(&signed_token.token.get().as_bytes(), &signed_token.mac)?;

        let token: Token = serde_json::from_str(signed_token.token.get())?;
        if token.is_expired() {
            return Err(SessionError::Expired);
        } else {
            Ok(token)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_tokens() {
        let token = Token::new("12345");
        let verifier = MessageVerifier::with_key_base(b"not very secure");

        let encoded = verifier.sign_token(&token).unwrap().encode().unwrap();
        let decoded = verifier
            .verify_token(&SignedToken::decode(&encoded).unwrap())
            .unwrap();

        assert_eq!(token, decoded);
    }

    #[test]
    fn verification_keys() {
        let token = Token::new("12345");
        let verifier1 = MessageVerifier::with_key_base(b"not very secure");
        let verifier2 = MessageVerifier::with_key_base(b"different secret key base");

        let encoded1 = verifier1.sign_token(&token).unwrap().encode().unwrap();
        let encoded2 = verifier2.sign_token(&token).unwrap().encode().unwrap();
        assert_ne!(encoded1, encoded2);

        let decoded1 = verifier1
            .verify_token(&SignedToken::decode(&encoded1).unwrap())
            .unwrap();
        let decoded2 = verifier2
            .verify_token(&SignedToken::decode(&encoded2).unwrap())
            .unwrap();

        assert_eq!(token, decoded1);
        assert_eq!(token, decoded2);

        assert!(verifier1
            .verify_token(&SignedToken::decode(&encoded2).unwrap())
            .is_err());
        assert!(verifier2
            .verify_token(&SignedToken::decode(&encoded1).unwrap())
            .is_err());
    }

    #[test]
    fn session_expiration() {
        let mut token = Token::new("12345");
        assert!(!token.is_expired());

        token.expires_at = Utc::now() - Duration::seconds(1);
        assert!(token.is_expired());
    }
}
