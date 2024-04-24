use sqlx::{postgres, Decode, Encode, Type, TypeInfo};

/// Id is the Rust equivalent of the Postgres `flowid` type domain.
/// It's a fixed 8-byte payload which is represented in hexadecimal notation.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id([u8; 8]);

impl From<models::Id> for Id {
    fn from(value: models::Id) -> Self {
        Id(value.as_array())
    }
}
impl Into<models::Id> for Id {
    fn into(self) -> models::Id {
        models::Id::new(self.0)
    }
}

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
}

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", i64::from_be_bytes(self.0))
    }
}
impl std::fmt::Debug for Id {
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

impl Type<postgres::Postgres> for Id {
    fn type_info() -> postgres::PgTypeInfo {
        postgres::PgTypeInfo::with_name("flowid")
    }
    fn compatible(ty: &postgres::PgTypeInfo) -> bool {
        *ty == Self::type_info() || ty.name() == "MACADDR8"
    }
}

impl sqlx::postgres::PgHasArrayType for Id {
    fn array_type_info() -> postgres::PgTypeInfo {
        postgres::PgTypeInfo::with_name("_flowid")
    }
}

impl Encode<'_, postgres::Postgres> for Id {
    fn encode_by_ref(&self, buf: &mut postgres::PgArgumentBuffer) -> sqlx::encode::IsNull {
        buf.extend_from_slice(&self.0);
        sqlx::encode::IsNull::No
    }
}

// TODO(johnny): This works fine for postgres binary format, but breaks for text format.
// Fix with a proper decoder once blocking issue is resolved:
//  https://github.com/launchbadge/sqlx/issues/1758
impl Decode<'_, postgres::Postgres> for Id {
    fn decode(value: postgres::PgValueRef<'_>) -> Result<Self, sqlx::error::BoxDynError> {
        <i64 as Decode<'_, postgres::Postgres>>::decode(value).map(|i| Self(i.to_be_bytes()))
    }
}
