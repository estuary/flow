use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};

use super::{Object, RelativeUrl};

/// A schema is a draft 2020-12 JSON Schema which validates Flow documents.
/// Schemas also provide annotations at document locations, such as reduction
/// strategies for combining one document into another.
///
/// Schemas may be defined inline to the catalog, or given as a relative
/// or absolute URI. URIs may optionally include a JSON fragment pointer that
/// locates a specific sub-schema therein.
///
/// For example, "schemas/marketing.yaml#/$defs/campaign" would reference the schema
/// at location {"$defs": {"campaign": ...}} within ./schemas/marketing.yaml.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(untagged)]
#[schemars(example = "Schema::example_absolute")]
#[schemars(example = "Schema::example_relative")]
#[schemars(example = "Schema::example_inline_basic")]
#[schemars(example = "Schema::example_inline_counter")]
pub enum Schema {
    /// Relative URL to a schema file.
    Url(RelativeUrl),
    /// Inline schema document.
    Object(Object),
    /// Inline schema document (alternate boolean form).
    Bool(bool),
}

impl Schema {
    pub fn example_absolute() -> Self {
        from_value(json!("http://example/schema#/$defs/subPath")).unwrap()
    }
    pub fn example_relative() -> Self {
        from_value(json!("../path/to/schema#/$defs/subPath")).unwrap()
    }
    pub fn example_inline_basic() -> Self {
        from_value(json!({
            "type": "object",
            "properties": {
                "foo": { "type": "integer" },
                "bar": { "const": 42 }
            }
        }))
        .unwrap()
    }
    pub fn example_inline_counter() -> Self {
        from_value(json!({
            "type": "object",
            "reduce": {"strategy": "merge"},
            "properties": {
                "foo_count": {
                    "type": "integer",
                    "reduce": {"strategy": "sum"},
                }
            }
        }))
        .unwrap()
    }
}
