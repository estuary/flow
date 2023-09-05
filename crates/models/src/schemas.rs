use super::RawValue;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};

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
#[schemars(example = "Schema::example_absolute")]
#[schemars(example = "Schema::example_relative")]
#[schemars(example = "Schema::example_inline_basic")]
pub struct Schema(RawValue);

impl std::ops::Deref for Schema {
    type Target = RawValue;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for Schema {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Schema {
    pub fn new(v: RawValue) -> Self {
        Self(v)
    }

    // URL for referencing the inferred schema of a collection, which may be used within a read schema.
    pub const REF_INFERRED_SCHEMA_URL: &str = "flow://inferred-schema";
    // URL for referencing the write schema of a collection, which may be used within a read schema.
    pub const REF_WRITE_SCHEMA_URL: &str = "flow://write-schema";

    /// Returns true if this Schema references the canonical inferred schema URL.
    pub fn references_inferred_schema(&self) -> bool {
        REF_INFERRED_SCHEMA_RE.is_match(self.get())
    }
    /// Returns true if this Schema references the canonical write schema URL.
    pub fn references_write_schema(&self) -> bool {
        REF_WRITE_SCHEMA_RE.is_match(self.get())
    }

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
}

// These patterns let us cheaply detect if a collection schema references the
// canonical inferred schema or write schema of its corresponding collection.
// Assuming an otherwise well-formed JSON schema, they can neither false-positive
// nor false-negative:
// * It must detect an actual property. A contained representation within a JSON
//   string must be quote-escaped and would not match the pattern.
// * It must be a schema keyword ($ref cannot be, say, a property) because
//   "flow://inferred-schema" is not a valid JSON schema and would error at build time.
lazy_static::lazy_static! {
    static ref REF_INFERRED_SCHEMA_RE: regex::Regex = regex::Regex::new(
        &[r#""\$ref"\p{Z}*:\p{Z}*""#, &regex::escape(Schema::REF_INFERRED_SCHEMA_URL), "\""].concat()
    ).unwrap();
    static ref REF_WRITE_SCHEMA_RE: regex::Regex = regex::Regex::new(
        &[r#""\$ref"\p{Z}*:\p{Z}*""#, &regex::escape(Schema::REF_WRITE_SCHEMA_URL), "\""].concat()
    ).unwrap();
}

#[cfg(test)]
mod test {
    use super::{RawValue, Schema};
    use serde_json::json;

    #[test]
    fn test_ref_patterns() {
        let fixture = json!({
            "allOf": [
                {"$ref": Schema::REF_INFERRED_SCHEMA_URL},
                {"$ref": Schema::REF_WRITE_SCHEMA_URL},
            ]
        });

        assert!(Schema::new(RawValue::from_value(&fixture)).references_inferred_schema());
        assert!(Schema::new(RawValue::from_value(&fixture)).references_write_schema());

        let fixture = Schema::new(
            RawValue::from_str(&serde_json::to_string_pretty(&fixture).unwrap()).unwrap(),
        );

        assert!(fixture.references_inferred_schema());
        assert!(fixture.references_write_schema());

        let fixture = Schema::new(RawValue::from_value(&json!({
            "does": "not match",
            "nested-quoted-string": fixture.get(),
        })));

        assert!(!fixture.references_inferred_schema());
        assert!(!fixture.references_write_schema());
    }
}
