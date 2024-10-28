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
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
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

type Skim = BTreeMap<String, RawValue>;
const KEYWORD_DEF: &str = "$defs";
const KEYWORD_ID: &str = "$id";

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
        let mut defs = Vec::new();

        // Add a definition for the write schema if it's referenced.
        // We cannot add it in all cases because the existing `read_bundle` and
        // `write_bundle` may have a common sub-schema defined, and naively adding
        // it would result in an indexing error due to the duplicate definition.
        // So, we treat $ref: flow://write-schema as a user assertion that there is
        // no such conflicting definition (and we may produce an indexing error
        // later if they're wrong).
        if let Some(write) = write_bundle.filter(|_| read_bundle.references_write_schema()) {
            defs.push(AddDef {
                id: Schema::REF_WRITE_SCHEMA_URL,
                schema: write,
                overwrite: true, // always overwrite the write schema definition
            });
        }

        if read_bundle.references_inferred_schema() {
            let inferred = inferred_bundle.unwrap_or(&INFERRED_SCHEMA_PLACEHOLDER);
            defs.push(AddDef {
                id: Schema::REF_INFERRED_SCHEMA_URL,
                schema: inferred,
                overwrite: true,
            });
        }

        Schema::add_defs(read_bundle, &defs)
    }

    pub fn build_read_schema_bundle(read_schema: &Schema, write_schema: &Schema) -> Schema {
        let mut defs = Vec::new();
        if read_schema.references_write_schema() {
            defs.push(AddDef {
                id: Schema::REF_WRITE_SCHEMA_URL,
                schema: write_schema,
                overwrite: true, // always overwrite the write schema definition
            });
        }
        if read_schema.references_inferred_schema() {
            // The control plane will keep the inferred schema definition up to date,
            // so we only ever add the placeholder here if it doesn't already exist.
            defs.push(AddDef {
                id: Schema::REF_INFERRED_SCHEMA_URL,
                schema: &INFERRED_SCHEMA_PLACEHOLDER,
                overwrite: false,
            });
        }
        Schema::add_defs(read_schema, &defs)
    }

    fn add_id(id: &str, schema: &Schema) -> RawValue {
        let mut skim: Skim = serde_json::from_str(schema.get()).unwrap();

        _ = skim.insert(
            KEYWORD_ID.to_string(),
            RawValue::from_value(&serde_json::Value::String(id.to_string())),
        );
        serde_json::value::to_raw_value(&skim).unwrap().into()
    }

    fn add_defs(target: &Schema, defs: &[AddDef]) -> Schema {
        use serde_json::value::to_raw_value;

        let mut read_schema: Skim = serde_json::from_str(target.get()).unwrap();
        let mut read_defs: Skim = read_schema
            .remove(KEYWORD_DEF)
            .map(|d| serde_json::from_str(d.get()).unwrap())
            .unwrap_or_default();

        for AddDef {
            id,
            schema,
            overwrite,
        } in defs
        {
            if !overwrite && read_defs.contains_key(*id) {
                continue;
            }
            let with_id = Schema::add_id(id, schema);
            read_defs.insert(id.to_string(), with_id);
        }

        // Skip adding defs if they are empty (which means `defs` was empty and there were no
        // pre-existing `$defs` in the schema).
        if !read_defs.is_empty() {
            // Re-serialize the updated definitions of the read schema.
            _ = read_schema.insert(
                KEYWORD_DEF.to_string(),
                serde_json::value::to_raw_value(&read_defs).unwrap().into(),
            );
        }
        Self(to_raw_value(&read_schema).unwrap().into())
    }

    /// Removes the bundled write schema from the `$defs` of `self`, returning
    /// a new schema with the value removed, and a boolean indicating whether the write
    /// schema def was actually present. We used to bundle the write schema as part of the
    /// read schema, just like the inferred schema. We're no longer doing that because it's
    /// confusing to users, so this function removes the bundled write schema. This function
    /// should only be needed for long enough to update all the inferred schemas, and can then
    /// be safely removed.
    pub fn remove_bundled_write_schema(&self) -> (bool, Self) {
        use serde_json::value::to_raw_value;

        let mut read_schema: Skim = serde_json::from_str(self.0.get()).unwrap();
        let mut read_defs: Skim = read_schema
            .get(KEYWORD_DEF)
            .map(|d| serde_json::from_str(d.get()).unwrap())
            .unwrap_or_default();
        let had_write_schema = read_defs.remove(Schema::REF_WRITE_SCHEMA_URL).is_some();
        read_schema.insert(
            KEYWORD_DEF.to_string(),
            to_raw_value(&read_defs).unwrap().into(),
        );
        (
            had_write_schema,
            Self(to_raw_value(&read_schema).unwrap().into()),
        )
    }
}

struct AddDef<'a> {
    id: &'a str,
    schema: &'a Schema,
    overwrite: bool,
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

    /// Placeholder used to resolve the `flow://inferred-schema` reference when the actual schema
    /// is not yet known.
    static ref INFERRED_SCHEMA_PLACEHOLDER: Schema =  Schema(RawValue::from_value(&serde_json::json!({
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
    } )));
}

#[cfg(test)]
mod test {
    use super::{RawValue, Schema};
    use serde_json::json;

    macro_rules! schema {
        ($json:tt) => {
            Schema::new(RawValue::from_value(&serde_json::json!($json)))
        };
    }

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
    fn test_build_read_schema_bundle() {
        let write_schema = schema!({
            "type": "object",
            "properties": {
                "a": { "type": "integer" },
                "b": { "type": "string" }
            }
        });

        // Assert that inferred schema placeholder gets added if needed
        let read_schema = schema!({
            "$defs": {
                "existing": { "properties": { "f": { "type": "string" }}}
            },
            "allOf": [
                {"$ref": "flow://inferred-schema"},
                {"$ref": "flow://write-schema"},
            ]
        });
        let result = Schema::build_read_schema_bundle(&read_schema, &write_schema);
        insta::assert_json_snapshot!(result.to_value(), @r###"
        {
          "$defs": {
            "existing": {
              "properties": {
                "f": {
                  "type": "string"
                }
              }
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
              "properties": {
                "a": {
                  "type": "integer"
                },
                "b": {
                  "type": "string"
                }
              },
              "type": "object"
            }
          },
          "allOf": [
            {
              "$ref": "flow://inferred-schema"
            },
            {
              "$ref": "flow://write-schema"
            }
          ]
        }
        "###);

        // Assert that existing defs are unchanged when read schema does not ref anything
        let read_schema = schema!({
            "$defs": {
                "existing": { "properties": { "f": { "type": "string" }}}
            },
            "type": "object",
            "properties": {
                "c": { "type": "integer" },
                "d": { "type": "string" }
            }
        });
        let result = Schema::build_read_schema_bundle(&read_schema, &write_schema);
        insta::assert_json_snapshot!(result.to_value(), @r###"
        {
          "$defs": {
            "existing": {
              "properties": {
                "f": {
                  "type": "string"
                }
              }
            }
          },
          "properties": {
            "c": {
              "type": "integer"
            },
            "d": {
              "type": "string"
            }
          },
          "type": "object"
        }
        "###);

        // Assert that no defs are added when read schema does not ref anything
        let read_schema = schema!({
            "type": "object",
            "properties": {
                "c": { "type": "integer" },
                "d": { "type": "string" }
            }
        });
        let result = Schema::build_read_schema_bundle(&read_schema, &write_schema);
        insta::assert_json_snapshot!(result.to_value(), @r###"
        {
          "properties": {
            "c": {
              "type": "integer"
            },
            "d": {
              "type": "string"
            }
          },
          "type": "object"
        }
        "###);

        // Assert that existing inferred schema def is not overwritten, but that
        // the write schema def is. Note that in practice any
        // `flow://write-schema` defs are removed by the publisher prior to
        // validation, so it should generally not exist when this function is
        // called. But there's still some collection specs out there that have
        // inlined write schema defs. Not overwriting the inferred schema def is
        // necessary in order to avoid replacing an existing
        // `flow://inferred-schema` def with the placeholder.
        let read_schema = schema!({
            "$defs": {
                "flow://inferred-schema": {
                    "type": "object",
                    "properties": {
                        "c": { "type": "integer" }
                    }
                },
                "flow://write-schema": {
                    "properties": {
                        "c": { "const": "should be overwritten" }
                    }
                },
            },
            "allOf": [
                {"$ref": "flow://inferred-schema"},
                {"$ref": "flow://write-schema"},
            ]
        });
        let result = Schema::build_read_schema_bundle(&read_schema, &write_schema);
        insta::assert_json_snapshot!(result.to_value(), @r###"
        {
          "$defs": {
            "flow://inferred-schema": {
              "properties": {
                "c": {
                  "type": "integer"
                }
              },
              "type": "object"
            },
            "flow://write-schema": {
              "$id": "flow://write-schema",
              "properties": {
                "a": {
                  "type": "integer"
                },
                "b": {
                  "type": "string"
                }
              },
              "type": "object"
            }
          },
          "allOf": [
            {
              "$ref": "flow://inferred-schema"
            },
            {
              "$ref": "flow://write-schema"
            }
          ]
        }
        "###);
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

    #[test]
    fn test_removing_bundled_write_schema() {
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

        let bundle =
            Schema::extend_read_bundle(&read_schema, Some(&write_schema), Some(&inferred_schema));
        assert_eq!(
            3,
            bundle.get().matches(Schema::REF_WRITE_SCHEMA_URL).count(),
            "schema should contain 'flow://write-schema' 3 times, for $ref, $defs key, and $id"
        );
        let (was_removed, new_bundle) = bundle.remove_bundled_write_schema();
        assert!(was_removed);
        insta::assert_json_snapshot!(new_bundle.to_value(), @r###"
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

        let (was_removed, _) = new_bundle.remove_bundled_write_schema();
        assert!(
            !was_removed,
            "expected write schema to have already been removed"
        );
    }
}
