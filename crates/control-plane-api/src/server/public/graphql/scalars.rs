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
