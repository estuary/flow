#[cfg(feature = "sqlx-support")]
use sqlx::Decode;

/// RawValue is like serde_json::value::RawValue, but removes newlines to ensure
/// values can safely be used in newline-delimited contexts.
///
/// As it uses serde_json::RawValue, it MUST be deserialized using serde_json
/// and not serde_yaml or some other Deserializer. This may require first
/// transcoding to serde_json::Value and then using serde_json::from_value().
#[derive(serde::Serialize, Clone)]
pub struct RawValue(Box<serde_json::value::RawValue>);

// RawValues are only equal if they are byte-for-byte identical,
// except for leading and trailing whitespace.
impl std::cmp::PartialEq<RawValue> for RawValue {
    fn eq(&self, other: &RawValue) -> bool {
        self.get().trim() == other.get().trim()
    }
}

impl RawValue {
    pub fn is_null(&self) -> bool {
        return self.get() == "null";
    }
    pub fn from_str(s: &str) -> serde_json::Result<Self> {
        Self::from_string(s.to_owned())
    }
    pub fn from_string(mut s: String) -> serde_json::Result<Self> {
        s.retain(|c| c != '\n'); // Strip newlines.
        let value = serde_json::value::RawValue::from_string(s)?;
        Ok(Self(value))
    }
    pub fn from_value(value: &serde_json::Value) -> Self {
        Self::from_string(value.to_string()).unwrap()
    }
    pub fn to_value(&self) -> serde_json::Value {
        serde_json::from_str(self.get()).unwrap()
    }
}

impl<'de> serde::Deserialize<'de> for RawValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let inner = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
        Ok(inner.into())
    }
}

impl Default for RawValue {
    fn default() -> Self {
        Self(serde_json::value::RawValue::from_string("null".to_string()).unwrap())
    }
}

impl From<Box<serde_json::value::RawValue>> for RawValue {
    fn from(value: Box<serde_json::value::RawValue>) -> Self {
        if value.get().contains('\n') {
            let s: Box<str> = value.into();
            Self::from_string(s.into()).unwrap()
        } else {
            Self(value)
        }
    }
}

impl From<RawValue> for Box<serde_json::value::RawValue> {
    fn from(RawValue(inner): RawValue) -> Self {
        inner
    }
}

impl From<RawValue> for String {
    fn from(value: RawValue) -> Self {
        let s: Box<str> = value.0.into();
        s.into()
    }
}

#[cfg(feature = "sqlx-support")]
impl sqlx::Type<sqlx::postgres::Postgres> for RawValue {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("json")
    }
    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        *ty == Self::type_info()
    }
}

#[cfg(feature = "sqlx-support")]
impl sqlx::Encode<'_, sqlx::postgres::Postgres> for RawValue {
    fn encode_by_ref(&self, buf: &mut sqlx::postgres::PgArgumentBuffer) -> sqlx::encode::IsNull {
        buf.extend_from_slice(self.0.get().as_bytes());
        sqlx::encode::IsNull::No
    }
}

#[cfg(feature = "sqlx-support")]
impl sqlx::Decode<'_, sqlx::postgres::Postgres> for RawValue {
    fn decode(value: sqlx::postgres::PgValueRef<'_>) -> Result<Self, sqlx::error::BoxDynError> {
        <&[u8] as Decode<'_, sqlx::postgres::Postgres>>::decode(value)
            .and_then(|i| serde_json::from_slice(i).map_err(|e| e.into()))
            .map(|i| Self(i))
    }
}

impl std::ops::Deref for RawValue {
    type Target = serde_json::value::RawValue;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for RawValue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl std::fmt::Debug for RawValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl schemars::JsonSchema for RawValue {
    fn schema_name() -> String {
        "Value".to_string()
    }
    fn is_referenceable() -> bool {
        false
    }
    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        serde_json::Value::json_schema(gen)
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn test_newlines_are_removed() {
        let fixture = serde_json::to_string_pretty(&serde_json::json!({
            "one": 2,
            "three": [4, 5]
        }))
        .unwrap();

        let v = serde_json::value::RawValue::from_string(fixture).unwrap();
        assert!(v.get().contains('\n'));
        let v = super::RawValue::from(v);
        assert!(!v.get().contains('\n'));
    }
}
