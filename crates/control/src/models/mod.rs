use std::convert::TryInto;
use std::fmt::Display;
use std::str::FromStr;

use base64::display::Base64Display;

pub mod accounts;
pub mod connector_images;
pub mod connectors;
pub mod credentials;

#[derive(Debug, thiserror::Error)]
pub enum MalformedIdError {
    #[error("id does not appear to be valid")]
    Decode,
    #[error("id with an unexpected length")]
    Length,
}

static ENCODING_CONFIG: base64::Config = base64::URL_SAFE_NO_PAD;

/// `Id` is a transparent wrapper around our non-sequential Postgres keys. We
/// wish to expose these Id values as base64 encoded values. This wrapper
/// handles all these serialization and deserialization concerns.
///
/// When creating a new Postgres-backed record or relationship, it is expected
/// to use `Id` for primary and foreign keys.
#[serde_as]
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    DeserializeFromStr,
    SerializeDisplay,
    sqlx::Type,
)]
#[sqlx(transparent)]
pub struct Id(i64);

impl Id {
    pub const fn new(inner: i64) -> Self {
        Self(inner)
    }

    /// Retrieves the raw value of the Id. Not to be directly exposed to
    /// customers, but not a secret.
    pub fn to_i64(&self) -> i64 {
        self.0
    }

    /// Converts the Id value to bytes. **Must** be the inverse operation of `from_bytes`.
    pub(crate) fn bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    /// Converts bytes into an Id value. **Must** be the inverse operation of `bytes`.
    fn from_bytes(bytes: [u8; 8]) -> Self {
        Id::new(i64::from_be_bytes(bytes))
    }

    /// An Id that does not correspond to a persisted resource, but rather is a
    /// one-time use identifier. Nonces must not be reused.
    pub fn nonce() -> Self {
        Id::new(rand::random())
    }
}

impl Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            Base64Display::with_config(&self.bytes(), ENCODING_CONFIG)
        )
    }
}

impl FromStr for Id {
    type Err = MalformedIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes =
            base64::decode_config(s, ENCODING_CONFIG).map_err(|_| MalformedIdError::Decode)?;
        let bytes = bytes.try_into().map_err(|_| MalformedIdError::Length)?;
        Ok(Id::from_bytes(bytes))
    }
}

impl From<Id> for String {
    fn from(id: Id) -> Self {
        id.to_string()
    }
}
