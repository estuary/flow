use super::{redact, reduce};
use json::schema;
use serde_json::Value;

/// Enumeration of JSON-Schema annotations allowed in document schema contexts.
#[derive(Debug)]
pub enum Annotation {
    /// Delegate all annotations of the core JSON-Schema spec.
    Core(schema::CoreAnnotation),
    /// "reduce" annotation keyword.
    Reduce(reduce::Strategy),
    /// "redact" annotation keyword.
    Redact(redact::Strategy),
    /// It has become a sort of convention in JSONSchema and OpenAPI to use X- prefixed fields
    /// for custom behavior. We parse these fields to avoid breaking on such custom fields.
    X(String, Value),
}

impl schema::Annotation for Annotation {
    type KeywordError = serde_json::Error;

    fn keyword(&self) -> &str {
        match self {
            Annotation::Core(core) => core.keyword(),
            Annotation::Reduce(_) => "reduce",
            Annotation::Redact(_) => "redact",
            Annotation::X(key, _) => key.as_str(),
        }
    }

    fn uses_keyword(keyword: &str) -> bool {
        match keyword {
            "reduce" | "redact" => true,
            key if key.starts_with("x-") || key.starts_with("X-") => true,
            _ => schema::CoreAnnotation::uses_keyword(keyword),
        }
    }

    fn from_keyword(keyword: &str, value: &serde_json::Value) -> Result<Self, Self::KeywordError> {
        use schema::CoreAnnotation as Core;

        match keyword {
            "reduce" => Ok(Annotation::Reduce(reduce::Strategy::try_from(value)?)),
            "redact" => Ok(Annotation::Redact(redact::Strategy::try_from(value)?)),
            key if key.starts_with("x-") || key.starts_with("X-") => {
                Ok(Annotation::X(key.to_string(), value.clone()))
            }
            _ => Ok(Annotation::Core(Core::from_keyword(keyword, value)?)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::Annotation;
    use json::schema::build::build_schema;
    use serde_json::json;
    use url::Url;

    #[test]
    fn test_x_fields() {
        let schema = json!({
            "properties": {
                "str": {
                    "type": "integer",
                    "x-value": "test"
                },
                "num": {
                    "type": "number",
                    "x-test": 2
                },
                "obj": {
                    "type": "number",
                    "x-another": { "hello": "world" }
                },
                "arr": {
                    "type": "number",
                    "x-arr": [1, 2, "test"]
                }
            },
        });

        let curi = Url::parse("https://example/schema").unwrap();
        build_schema::<Annotation>(&curi, &schema).unwrap();
    }
}
