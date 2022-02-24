use std::fmt::Display;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use strum::EnumIter;

use crate::models::Id;

#[derive(Debug, Deserialize, Serialize)]
pub struct NewSession {
    pub auth_token: String,
}

#[derive(Debug, Serialize)]
pub struct Session {
    pub account_id: Id,
    pub token: String,
    pub expires_at: DateTime<Utc>,
}

#[serde_as]
#[derive(Debug, thiserror::Error)]
pub enum LoginError {
    #[error("idp not supported: {0}")]
    UnknownIDP(String),
}

#[derive(Debug, DeserializeFromStr, EnumIter)]
pub enum IdentityProvider {
    Local,
    // Google,
    // Microsoft,
    // etc.
}

impl IdentityProvider {
    pub fn as_str(&self) -> &'static str {
        match self {
            IdentityProvider::Local => "local",
        }
    }
}

impl Display for IdentityProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for IdentityProvider {
    type Err = LoginError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        match name {
            "local" => Ok(IdentityProvider::Local),
            other => Err(LoginError::UnknownIDP(other.to_owned())),
        }
    }
}
