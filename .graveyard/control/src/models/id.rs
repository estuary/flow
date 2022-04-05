use std::fmt::Display;
use std::marker::PhantomData;
use std::str::FromStr;

use base64::display::Base64Display;

#[derive(Debug, thiserror::Error)]
pub enum MalformedIdError {
    #[error("id does not appear to be valid")]
    Decode,
    #[error("id with an unexpected length")]
    Length,
}

pub static ENCODING_CONFIG: base64::Config = base64::URL_SAFE_NO_PAD;

/// `Id` is a transparent wrapper around our non-sequential Postgres keys. We
/// wish to expose these Id values as base64 encoded values. This wrapper
/// handles all these serialization and deserialization concerns.
///
/// When creating a new Postgres-backed record or relationship, it is expected
/// to use `Id` for primary and foreign keys.
#[serde_as]
#[derive(Debug)]
pub struct Id<T> {
    value: i64,
    _type: PhantomData<T>,
}

impl<T> Id<T> {
    pub const fn new(inner: i64) -> Self {
        Self {
            value: inner,
            _type: PhantomData,
        }
    }

    /// Retrieves the raw value of the Id. Not to be directly exposed to
    /// customers, but not a secret.
    pub fn to_i64(&self) -> i64 {
        self.value
    }

    /// Converts the Id value to bytes. **Must** be the inverse operation of `from_bytes`.
    pub fn bytes(&self) -> [u8; 8] {
        self.value.to_be_bytes()
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

impl<T> Display for Id<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            Base64Display::with_config(&self.bytes(), ENCODING_CONFIG)
        )
    }
}

impl<T> FromStr for Id<T> {
    type Err = MalformedIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes =
            base64::decode_config(s, ENCODING_CONFIG).map_err(|_| MalformedIdError::Decode)?;
        let bytes = bytes.try_into().map_err(|_| MalformedIdError::Length)?;
        Ok(Id::from_bytes(bytes))
    }
}

impl<T> From<Id<T>> for String {
    fn from(id: Id<T>) -> Self {
        id.to_string()
    }
}

impl<T> serde::Serialize for Id<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de, T> serde::Deserialize<'de> for Id<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        pub struct IdVisitor<T>(PhantomData<T>);
        impl<'v, T> serde::de::Visitor<'v> for IdVisitor<T> {
            type Value = Id<T>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an integer or a formatted Id string")
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Id::new(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                value.parse::<Id<T>>().map_err(serde::de::Error::custom)
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                std::str::from_utf8(value)
                    .map_err(serde::de::Error::custom)?
                    .parse::<Id<T>>()
                    .map_err(serde::de::Error::custom)
            }
        }

        deserializer.deserialize_str(IdVisitor(Default::default()))
    }
}

impl<'q, T, DB: ::sqlx::Database> ::sqlx::encode::Encode<'q, DB> for Id<T>
where
    i64: ::sqlx::encode::Encode<'q, DB>,
{
    fn encode_by_ref(
        &self,
        buf: &mut <DB as ::sqlx::database::HasArguments<'q>>::ArgumentBuffer,
    ) -> ::sqlx::encode::IsNull {
        <i64 as ::sqlx::encode::Encode<'q, DB>>::encode_by_ref(&self.value, buf)
    }
    fn produces(&self) -> Option<DB::TypeInfo> {
        <i64 as ::sqlx::encode::Encode<'q, DB>>::produces(&self.value)
    }
    fn size_hint(&self) -> usize {
        <i64 as ::sqlx::encode::Encode<'q, DB>>::size_hint(&self.value)
    }
}

impl<'r, T, DB: ::sqlx::Database> ::sqlx::decode::Decode<'r, DB> for Id<T>
where
    i64: ::sqlx::decode::Decode<'r, DB>,
{
    fn decode(
        value: <DB as ::sqlx::database::HasValueRef<'r>>::ValueRef,
    ) -> ::std::result::Result<
        Self,
        ::std::boxed::Box<
            dyn ::std::error::Error + 'static + ::std::marker::Send + ::std::marker::Sync,
        >,
    > {
        <i64 as ::sqlx::decode::Decode<'r, DB>>::decode(value).map(Self::new)
    }
}

impl<T, DB: ::sqlx::Database> ::sqlx::Type<DB> for Id<T>
where
    i64: ::sqlx::Type<DB>,
{
    fn type_info() -> DB::TypeInfo {
        <i64 as ::sqlx::Type<DB>>::type_info()
    }
    fn compatible(ty: &DB::TypeInfo) -> ::std::primitive::bool {
        <i64 as ::sqlx::Type<DB>>::compatible(ty)
    }
}

impl<T> Clone for Id<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            _type: PhantomData,
        }
    }
}

impl<T> Copy for Id<T> {}

impl<T> PartialEq for Id<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T> Eq for Id<T> {}

impl<T> PartialOrd for Id<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.value.partial_cmp(&other.value)
    }
}

impl<T> Ord for Id<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value.cmp(&other.value)
    }
}

impl<T> std::hash::Hash for Id<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}
