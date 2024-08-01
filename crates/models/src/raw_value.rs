#[derive(serde::Serialize, serde::Deserialize, Clone)]
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
        serde_json::value::RawValue::from_string(s.to_owned()).map(Into::into)
    }
    pub fn from_string(s: String) -> serde_json::Result<Self> {
        serde_json::value::RawValue::from_string(s).map(Into::into)
    }
    pub fn from_value(value: &serde_json::Value) -> Self {
        Self::from_string(value.to_string()).unwrap()
    }
    pub fn to_value(&self) -> serde_json::Value {
        serde_json::from_str(self.get()).unwrap()
    }
}

impl Default for RawValue {
    fn default() -> Self {
        Self(serde_json::value::RawValue::from_string("null".to_string()).unwrap())
    }
}

impl From<Box<serde_json::value::RawValue>> for RawValue {
    fn from(value: Box<serde_json::value::RawValue>) -> Self {
        Self(value)
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
