use super::RawValue;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use std::collections::BTreeMap;

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
    pub fn into_inner(self) -> RawValue {
        self.0
    }

    pub fn to_value(&self) -> serde_json::Value {
        self.0.to_value()
    }

    // URL for referencing the inferred schema of a collection, which may be used within a read schema.
    pub const REF_INFERRED_SCHEMA_URL: &'static str = "flow://inferred-schema";
    // URL for referencing the write schema of a collection, which may be used within a read schema.
    pub const REF_WRITE_SCHEMA_URL: &'static str = "flow://write-schema";

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

    pub fn default_inferred_read_schema() -> Self {
        let read_schema = serde_json::json!({
            "allOf": [
                {"$ref": "flow://write-schema"},
                {"$ref": "flow://inferred-schema"}
            ],
        });
        let read_bundle = Self(crate::RawValue::from_value(&read_schema));
        Self::extend_read_bundle(&read_bundle, None, None)
    }

    /// Extend a bundled Flow read schema, which may include references to the
    /// canonical collection write schema URI and inferred schema URI,
    /// with inline definitions that fully resolve these references.
    /// If an inferred schema is not available then `{}` is used.
    pub fn extend_read_bundle(
        read_bundle: &Self,
        write_bundle: Option<&Self>,
        inferred_bundle: Option<&Self>,
    ) -> Self {
        const KEYWORD_DEF: &str = "$defs";
        const KEYWORD_ID: &str = "$id";

        use serde_json::{value::to_raw_value, Value};
        type Skim = BTreeMap<String, RawValue>;

        let mut read_schema: Skim = serde_json::from_str(read_bundle.get()).unwrap();
        let mut read_defs: Skim = read_schema
            .get(KEYWORD_DEF)
            .map(|d| serde_json::from_str(d.get()).unwrap())
            .unwrap_or_default();

        // Add a definition for the write schema if it's referenced.
        // We cannot add it in all cases because the existing `read_bundle` and
        // `write_bundle` may have a common sub-schema defined, and naively adding
        // it would result in an indexing error due to the duplicate definition.
        // So, we treat $ref: flow://write-schema as a user assertion that there is
        // no such conflicting definition (and we may produce an indexing error
        // later if they're wrong).
        if let Some(write_schema_json) =
            write_bundle.filter(|_| read_bundle.references_write_schema())
        {
            let mut write_schema: Skim = serde_json::from_str(write_schema_json.get()).unwrap();

            // Set $id to "flow://write-schema".
            _ = write_schema.insert(
                KEYWORD_ID.to_string(),
                RawValue::from_value(&Value::String(Self::REF_WRITE_SCHEMA_URL.to_string())),
            );
            // Add as a definition within the read schema.
            read_defs.insert(
                Self::REF_WRITE_SCHEMA_URL.to_string(),
                to_raw_value(&write_schema).unwrap().into(),
            );
        }

        // Add a definition for the inferred schema if it's referenced.
        if read_bundle.references_inferred_schema() {
            // Prefer the actual inferred schema, or fall back to a sentinel schema
            // which allows for validations but fails on the first document.
            let inferred_bundle = inferred_bundle.map(|s| s.get()).unwrap_or(
                r###"
            {
                "properties": {
                    "_meta": {
                        "properties": {
                            "inferredSchemaIsNotAvailable": {
                                "const": true,
                                "description": "An inferred schema is not yet available because no documents have been written to this collection.\nThis place-holder causes document validations to fail at read time, so that the task can be updated once an inferred schema is ready."
                            }
                        },
                        "required": ["inferredSchemaIsNotAvailable"]
                    }
                },
                "required": ["_meta"]
            }
            "###,
            );
            // We don't use `Skim` here because we want the serde round trip to
            // transform the sentinel schema from pretty-printed to dense. This
            // is important because newlines in the schema could otherwise break
            // connectors using the airbyte protocol.
            let mut inferred_schema: BTreeMap<String, serde_json::Value> =
                serde_json::from_str(inferred_bundle).unwrap();

            // Set $id to "flow://inferred-schema".
            _ = inferred_schema.insert(
                KEYWORD_ID.to_string(),
                Value::String(Self::REF_INFERRED_SCHEMA_URL.to_string()),
            );
            // Add as a definition within the read schema.
            read_defs.insert(
                Self::REF_INFERRED_SCHEMA_URL.to_string(),
                to_raw_value(&inferred_schema).unwrap().into(),
            );
        }

        // Re-serialize the updated definitions of the read schema.
        _ = read_schema.insert(
            KEYWORD_DEF.to_string(),
            serde_json::value::to_raw_value(&read_defs).unwrap().into(),
        );
        Self(to_raw_value(&read_schema).unwrap().into())
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

    #[test]
    fn test_extend_read_schema() {
        let read_schema = Schema::new(RawValue::from_value(&json!({
            "$defs": {
                "existing://def": {"type": "array"},
            },
            "maxProperties": 10,
            "allOf": [
                {"$ref": "flow://inferred-schema"},
                {"$ref": "flow://write-schema"},
            ]
        })));
        let write_schema = Schema::new(RawValue::from_value(&json!({
            "$id": "old://value",
            "required": ["a_key"],
        })));
        let inferred_schema = Schema::new(RawValue::from_value(&json!({
            "$id": "old://value",
            "minProperties": 5,
        })));

        insta::assert_json_snapshot!(Schema::extend_read_bundle(&read_schema, Some(&write_schema), Some(&inferred_schema)).to_value(), @r###"
        {
          "$defs": {
            "existing://def": {
              "type": "array"
            },
            "flow://inferred-schema": {
              "$id": "flow://inferred-schema",
              "minProperties": 5
            },
            "flow://write-schema": {
              "$id": "flow://write-schema",
              "required": [
                "a_key"
              ]
            }
          },
          "allOf": [
            {
              "$ref": "flow://inferred-schema"
            },
            {
              "$ref": "flow://write-schema"
            }
          ],
          "maxProperties": 10
        }
        "###);

        // Case: no inferred schema is available.
        insta::assert_json_snapshot!(Schema::extend_read_bundle(&read_schema, Some(&write_schema), None).to_value(), @r###"
        {
          "$defs": {
            "existing://def": {
              "type": "array"
            },
            "flow://inferred-schema": {
              "$id": "flow://inferred-schema",
              "properties": {
                "_meta": {
                  "properties": {
                    "inferredSchemaIsNotAvailable": {
                      "const": true,
                      "description": "An inferred schema is not yet available because no documents have been written to this collection.\nThis place-holder causes document validations to fail at read time, so that the task can be updated once an inferred schema is ready."
                    }
                  },
                  "required": [
                    "inferredSchemaIsNotAvailable"
                  ]
                }
              },
              "required": [
                "_meta"
              ]
            },
            "flow://write-schema": {
              "$id": "flow://write-schema",
              "required": [
                "a_key"
              ]
            }
          },
          "allOf": [
            {
              "$ref": "flow://inferred-schema"
            },
            {
              "$ref": "flow://write-schema"
            }
          ],
          "maxProperties": 10
        }
        "###);

        // Case: pass `write_schema` which has no references.
        insta::assert_json_snapshot!(Schema::extend_read_bundle(&write_schema, Some(&write_schema), None).to_value(), @r###"
        {
          "$defs": {},
          "$id": "old://value",
          "required": [
            "a_key"
          ]
        }
        "###);

        // Case: don't include `write_schema`
        insta::assert_json_snapshot!(Schema::extend_read_bundle(&read_schema, None, Some(&inferred_schema)).to_value(), @r###"
        {
          "$defs": {
            "existing://def": {
              "type": "array"
            },
            "flow://inferred-schema": {
              "$id": "flow://inferred-schema",
              "minProperties": 5
            }
          },
          "allOf": [
            {
              "$ref": "flow://inferred-schema"
            },
            {
              "$ref": "flow://write-schema"
            }
          ],
          "maxProperties": 10
        }
        "###);
    }
}
