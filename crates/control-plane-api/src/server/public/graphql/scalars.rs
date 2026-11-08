//! Custom GraphQL scalars shared across the API.

/// A secret the API returns to the caller — a bearer credential or similar
/// one-time secret. It serializes as a plain string so the caller receives the
/// real value, but is a distinct scalar in the schema so client tooling can
/// recognize and redact it: in logs and UIs, and above all before any value is
/// handed to a language model.
///
/// Its `Debug` impl never prints the secret, so wrapping a field in `Sensitive`
/// also keeps the value out of server logs, traces, and error messages.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Sensitive(pub String);

impl Sensitive {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl std::fmt::Debug for Sensitive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Sensitive(<redacted>)")
    }
}

async_graphql::scalar!(
    Sensitive,
    "Sensitive",
    "A secret returned by the API, such as a bearer credential. The value is \
     serialized as a string, but clients must treat it as sensitive: redact it \
     from logs and UIs, and never pass it to a language model."
);

/// A 64-bit unsigned integer, serialized as a decimal string.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct UInt64(pub u64);

impl std::fmt::Debug for UInt64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl From<u64> for UInt64 {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

/// A 64-bit unsigned integer, serialized as a decimal string.
#[async_graphql::Scalar(name = "UInt64")]
impl async_graphql::ScalarType for UInt64 {
    fn parse(value: async_graphql::Value) -> async_graphql::InputValueResult<Self> {
        match value {
            async_graphql::Value::String(s) => s
                .parse::<u64>()
                .map(UInt64)
                .map_err(async_graphql::InputValueError::custom),
            async_graphql::Value::Number(n) => n.as_u64().map(UInt64).ok_or_else(|| {
                async_graphql::InputValueError::custom(
                    "expected a non-negative integer representable in 64 bits",
                )
            }),
            other => Err(async_graphql::InputValueError::expected_type(other)),
        }
    }

    fn is_valid(value: &async_graphql::Value) -> bool {
        match value {
            async_graphql::Value::String(s) => s.parse::<u64>().is_ok(),
            async_graphql::Value::Number(n) => n.is_u64(),
            _ => false,
        }
    }

    fn to_value(&self) -> async_graphql::Value {
        async_graphql::Value::String(self.0.to_string())
    }
}
