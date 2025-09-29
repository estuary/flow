use super::{redact, reduce};
use json::schema;
use serde::Deserialize;
use serde_json::Value;

/// Enumeration of JSON-Schema associated annotations understood by Estuary.
#[derive(Debug)]
pub enum Annotation {
    /// Delegate all annotations of the core JSON-Schema spec.
    Core(schema::CoreAnnotation),
    /// "reduce" annotation keyword.
    Reduce(reduce::Strategy),
    /// "redact" annotation keyword.
    Redact(redact::Strategy),
    /// "secret" or "airbyte_secret" annotation keyword.
    Secret(bool),
    /// "multiline" annotation keyword marks fields that should have a multiline text input in the
    /// UI. This is from the airbyte spec.
    Multiline(bool),
    /// Marks a location as being an advanced configuration section, which should be collapsed by
    /// default. This annotation is intended to be applied only when the type is `object`, and it
    /// applies to all of the objects properties.
    Advanced(bool),
    /// "order" annotation keyword, indicates the desired presentation order of fields in the UI.
    /// This is from the airbyte spec.
    Order(i32),
    /// It has become a sort of convention in JSONSchema and OpenAPI to use X- prefixed fields
    /// for custom behavior. We parse these fields to avoid breaking on such custom fields.
    X(String, Value),
    /// Discriminator is an annotation from the [openapi spec](https://spec.openapis.org/oas/latest.html#discriminator-object),
    /// which is used by the Estuary UI when rendering forms containing `oneOf`s.
    Discriminator(Value),
}

impl schema::Annotation for Annotation {
    type KeywordError = serde_json::Error;

    fn keyword(&self) -> &str {
        match self {
            Annotation::Core(core) => core.keyword(),
            Annotation::Reduce(_) => "reduce",
            Annotation::Redact(_) => "redact",
            Annotation::Secret(_) => "secret",
            Annotation::Multiline(_) => "multiline",
            Annotation::Advanced(_) => "advanced",
            Annotation::Order(_) => "order",
            Annotation::X(key, _) => key.as_str(),
            Annotation::Discriminator(_) => "discriminator",
        }
    }

    fn uses_keyword(keyword: &str) -> bool {
        match keyword {
            "reduce" | "redact" | "secret" | "airbyte_secret" | "multiline" | "advanced"
            | "order" | "discriminator" => true,
            key if key.starts_with("x-") || key.starts_with("X-") => true,
            _ => schema::CoreAnnotation::uses_keyword(keyword),
        }
    }

    fn from_keyword(keyword: &str, value: &serde_json::Value) -> Result<Self, Self::KeywordError> {
        use schema::CoreAnnotation as Core;

        match keyword {
            "reduce" => Ok(Annotation::Reduce(reduce::Strategy::try_from(value)?)),
            "redact" => Ok(Annotation::Redact(redact::Strategy::try_from(value)?)),
            "order" => Ok(Annotation::Order(i32::deserialize(value)?)),
            "secret" | "airbyte_secret" => Ok(Annotation::Secret(bool::deserialize(value)?)),
            "multiline" => Ok(Annotation::Multiline(bool::deserialize(value)?)),
            "advanced" => Ok(Annotation::Advanced(bool::deserialize(value)?)),
            "discriminator" => Ok(Annotation::Discriminator(value.clone())),
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
    fn build_with_advanced_annotation() {
        let schema = json!({
            "type": "object",
            "advanced": true,
            "properties": {
                "advanced_foo": {"type": "integer"}
            },
        });

        let curi = Url::parse("https://example/schema").unwrap();
        build_schema::<Annotation>(&curi, &schema).unwrap();
    }

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
