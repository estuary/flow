use std::convert::TryInto;
use std::fmt::Display;
use std::str::FromStr;

use base64::display::Base64Display;

pub mod connector_images;
pub mod connectors;

#[derive(Debug, thiserror::Error)]
pub enum MalformedIdError {
    #[error("does not appear to be valid")]
    Decode,
    #[error("unexpected length")]
    Length,
}

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
    pub fn new(inner: i64) -> Self {
        Self(inner)
    }
}

impl Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            Base64Display::with_config(&self.0.to_le_bytes(), base64::URL_SAFE_NO_PAD)
        )
    }
}

impl FromStr for Id {
    type Err = MalformedIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = base64::decode_config(s, base64::URL_SAFE_NO_PAD)
            .map_err(|_| MalformedIdError::Decode)?;
        let bytes: [u8; 8] = bytes.try_into().map_err(|_| MalformedIdError::Length)?;
        Ok(Id::new(i64::from_le_bytes(bytes)))
    }
}
