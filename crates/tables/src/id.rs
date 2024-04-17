use std::{fmt::Debug, str::FromStr};

use crate::Column;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id([u8; 8]);

impl Id {
    pub fn is_zero(&self) -> bool {
        self.0 == [0u8; 8]
    }
    pub fn new(b: [u8; 8]) -> Self {
        Self(b)
    }
    pub fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, hex::FromHexError> {
        let vec_bytes = hex::decode(hex)?;
        let exact: [u8; 8] = vec_bytes
            .as_slice()
            .try_into()
            .map_err(|_| hex::FromHexError::InvalidStringLength)?;

        Ok(Id(exact))
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl FromStr for Id {
    type Err = hex::FromHexError;

    // TODO: from_hex should probably also accept strings with colons
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let no_colons = s.replace(':', "");
        Id::from_hex(&no_colons)
    }
}

#[cfg(feature = "persist")]
impl crate::SqlColumn for Id {
    fn sql_type() -> &'static str {
        "TEXT"
    }

    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        //<String as rusqlite::types::ToSql>::to_sql(&self.to_string())
        Ok(rusqlite::types::ToSqlOutput::Owned(
            rusqlite::types::Value::Text(self.to_string()),
        ))
    }

    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let s = value.as_str()?;
        Id::from_hex(s).map_err(|err| rusqlite::types::FromSqlError::Other(Box::new(err)))
    }
}

impl Column for Id {
    fn column_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
    }
}

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", i64::from_be_bytes(self.0))
    }
}
impl Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
    }
}
impl serde::Serialize for Id {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        format!("{self}").serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Id {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        let str_val = std::borrow::Cow::<'de, str>::deserialize(deserializer)?;
        Id::from_hex(str_val.as_ref()).map_err(|err| D::Error::custom(format!("invalid id: {err}")))
    }
}
