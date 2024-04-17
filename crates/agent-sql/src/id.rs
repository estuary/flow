use sqlx::{postgres, Decode, Encode, Type, TypeInfo};

/// Id is the Rust equivalent of the Postgres `flowid` type domain.
/// It's a fixed 8-byte payload which is represented in hexadecimal notation.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id(tables::Id);

impl Into<tables::Id> for Id {
    fn into(self) -> tables::Id {
        self.0
    }
}

impl Id {
    pub fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
    pub fn new(b: [u8; 8]) -> Self {
        Self(tables::Id::new(b))
    }
    pub fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, hex::FromHexError> {
        let id = tables::Id::from_hex(hex)?;
        Ok(Id(id))
    }
}

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}
impl std::fmt::Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
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
        buf.extend_from_slice(self.0.as_slice());
        sqlx::encode::IsNull::No
    }
}

// TODO(johnny): This works fine for postgres binary format, but breaks for text format.
// Fix with a proper decoder once blocking issue is resolved:
//  https://github.com/launchbadge/sqlx/issues/1758
impl Decode<'_, postgres::Postgres> for Id {
    fn decode(value: postgres::PgValueRef<'_>) -> Result<Self, sqlx::error::BoxDynError> {
        <i64 as Decode<'_, postgres::Postgres>>::decode(value)
            .map(|i| Self(tables::Id::new(i.to_be_bytes())))
    }
}
