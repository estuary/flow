use std::collections::BTreeMap;

use axum::response::Response;
use serde_json::Value as JsonValue;

/// Applies more sweeping redactions than insta provides by default. Redactor
/// specializes in redacting across many fields of a response at once. The prime
/// example being `id`s embedded within link text.
///
/// Insta redactions are still preferred in the general case, as they can act as
/// assertions as well.
#[derive(Debug, Default)]
pub struct Redactor(BTreeMap<String, String>);

impl Redactor {
    /// Registers a pattern to be redacted later. Each redaction can have a
    /// separate redaction text, which is useful for ensuring that values are
    /// not swapped.
    pub fn redact(mut self, pattern: impl Into<String>, replacement: &str) -> Self {
        self.0.insert(pattern.into(), format!("[{}]", replacement));
        self
    }

    /// Applies all registered redactions to the text.
    pub fn apply(&self, response: impl Into<String>) -> String {
        let mut result = response.into();

        for (id, replacement) in self.0.iter() {
            result = result.replace(id, replacement);
        }

        result
    }

    /// Applies all redactions to the body of a response, then parses the result
    /// as json.
    pub async fn response_json(&self, response: &mut Response) -> anyhow::Result<JsonValue> {
        let body = hyper::body::to_bytes(response.body_mut())
            .await
            .expect("a response body");
        let redacted = &self.apply(std::str::from_utf8(body.as_ref())?);
        let json = serde_json::from_str::<JsonValue>(redacted)?;
        Ok(json)
    }
}
