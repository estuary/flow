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
///
// Schema wraps RawValue: see its notes on deserialization, which must use serde_json.
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
    // URL for referencing the write schema of a collection, which may be used within a read schema.
    pub const REF_RELAXED_WRITE_SCHEMA_URL: &'static str = "flow://relaxed-write-schema";

    /// Returns true if this Schema references the canonical inferred schema URL.
    pub fn references_inferred_schema(&self) -> bool {
        REF_INFERRED_SCHEMA_RE.is_match(self.get())
    }
    /// Returns true if this Schema references the canonical write schema URL.
    pub fn references_write_schema(&self) -> bool {
        REF_WRITE_SCHEMA_RE.is_match(self.get())
    }
    /// Returns true if this Schema references the canonical write schema URL.
    pub fn references_relaxed_write_schema(&self) -> bool {
        REF_RELAXED_WRITE_SCHEMA_RE.is_match(self.get())
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

    /// Default collection `readSchema`,
    /// used when schema inference is desired.
    pub fn default_inferred_read_schema() -> Self {
        let read_schema = serde_json::json!({
            "allOf": [
                {"$ref": Self::REF_RELAXED_WRITE_SCHEMA_URL},
                {"$ref": Self::REF_INFERRED_SCHEMA_URL},
            ],
        });
        Self(crate::RawValue::from_value(&read_schema))
    }

    /// Placeholder definition for `flow://inferred-schema`,
    /// used when an actual inferred schema is not yet available.
    pub fn inferred_schema_placeholder() -> &'static Self {
        &INFERRED_SCHEMA_PLACEHOLDER
    }

    /// Transform this Schema into a relaxed form which removes all `type`,
    /// `require`, and `format` keywords of most (but not all) recursive
    /// sub-schemas, while preserving other keywords.
    ///
    /// The primary purpose of relaxed schemas is to transform a collection
    /// write-schema into a relaxation which is likely to function well when
    /// intersected with the collection's inferred schema.
    pub fn to_relaxed_schema(&self) -> serde_json::Result<Self> {
        let relaxed = serde_json::from_str::<RelaxedSchema>(self.get())?;
        Ok(Self(serde_json::value::to_raw_value(&relaxed)?.into()))
    }

    /// TODO(johnny): This is deprecated and will be removed.
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
            .ok()
            .unwrap_or_else(|| read_bundle.clone())
    }

    /// TODO(johnny): This is deprecated and will be removed.
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
            .ok()
            .unwrap_or_else(|| read_schema.clone())
    }

    /// Extend this Schema with added $defs definitions.
    /// The $id keyword of definition is set to its id, and its id is also
    /// used to key the property of $defs under which the sub-schema lives.
    pub fn add_defs(&self, defs: &[AddDef]) -> serde_json::Result<Schema> {
        use serde_json::value::to_raw_value;

        type Skim = BTreeMap<String, RawValue>;
        const KEYWORD_DEF: &str = "$defs";
        const KEYWORD_ID: &str = "$id";

        let mut schema = serde_json::from_str::<Skim>(self.get())?;
        let mut schema_defs = schema
            .remove(KEYWORD_DEF)
            .map(|d| serde_json::from_str::<Skim>(d.get()))
            .transpose()?
            .unwrap_or_default();

        for AddDef {
            id,
            schema: sub_schema,
            overwrite,
        } in defs
        {
            if !overwrite && schema_defs.contains_key(*id) {
                continue;
            }
            let mut sub_schema = serde_json::from_str::<Skim>(sub_schema.get())?;
            _ = sub_schema.insert(
                KEYWORD_ID.to_string(),
                RawValue::from_value(&serde_json::Value::String(id.to_string())),
            );
            schema_defs.insert(id.to_string(), to_raw_value(&sub_schema)?.into());
        }

        // Skip adding defs if they are empty (which means `defs` was empty and there were no
        // pre-existing `$defs` in the schema).
        if !schema_defs.is_empty() {
            _ = schema.insert(
                KEYWORD_DEF.to_string(),
                serde_json::value::to_raw_value(&schema_defs)?.into(),
            );
        }

        Ok(Self(to_raw_value(&schema)?.into()))
    }
}

// AddDef instances parameterize sub-schemas to be added via Schema::add_defs().
pub struct AddDef<'a> {
    // Canonical $id of this schema, which also keys the schema in $defs.
    pub id: &'a str,
    // Sub-schema to be inlined into $defs.
    pub schema: &'a Schema,
    // Should this definition overwrite one that's already present?
    pub overwrite: bool,
}

/// RelaxedSchema is an opinionated relaxation of a JSON-Schema, which removes
/// the `type`, `required`, and `format` keywords from most (but not all)
/// recursive sub-schemas. It's purpose is to transform collection write schemas
/// into relaxed forms which are likely to function well when intersected with
/// an inferred schema.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
enum RelaxedSchema {
    Bool(bool),
    Obj(RelaxedSchemaObj),
    Vec(Vec<RelaxedSchema>),
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RelaxedSchemaObj {
    #[serde(
        rename = "$defs",
        alias = "definitions",
        default,
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    defs: BTreeMap<String, RelaxedSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    all_of: Option<Box<RelaxedSchema>>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    properties: BTreeMap<String, RelaxedSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    items: Option<Box<RelaxedSchema>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    additional_items: Option<Box<RelaxedSchema>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    additional_properties: Option<Box<RelaxedSchema>>,

    // Keywords which are removed from a relaxed schema.
    #[serde(rename = "type", skip_serializing)]
    _type: Option<serde_json::Value>,
    #[serde(rename = "required", default, skip_serializing)]
    _required: Vec<String>,
    #[serde(rename = "format", default, skip_serializing)]
    _format: String,

    // Other keywords are passed-through.
    #[serde(flatten)]
    pass_through: BTreeMap<String, serde_json::Value>,
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
    static ref REF_RELAXED_WRITE_SCHEMA_RE: regex::Regex = regex::Regex::new(
        &[r#""\$ref"\p{Z}*:\p{Z}*""#, &regex::escape(Schema::REF_RELAXED_WRITE_SCHEMA_URL), "\""].concat()
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
    use super::{AddDef, RawValue, Schema};
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
    fn test_references_and_add_defs() {
        let sub_schema = schema!({
            "type": "object",
            "properties": {
                "a": { "type": "integer" },
                "b": { "type": "string" }
            }
        });

        let schema = schema!({
            "$defs": {
                "replaced": { "properties": { "f": { "type": "string" }}},
                "not_overwritten": { "properties": { "f": { "type": "string" }}},
                "extra": { "properties": { "f": { "type": "string" }}},
            },
            "allOf": [
                {"$ref": "flow://inferred-schema"},
                {"$ref": "flow://write-schema"},
            ]
        });

        assert!(!schema.references_relaxed_write_schema());
        assert!(schema.references_inferred_schema());
        assert!(schema.references_write_schema());

        let outcome = schema.add_defs(&[
            AddDef {
                id: Schema::REF_WRITE_SCHEMA_URL,
                schema: &sub_schema,
                overwrite: true,
            },
            AddDef {
                id: Schema::REF_INFERRED_SCHEMA_URL,
                schema: &sub_schema,
                overwrite: true,
            },
            AddDef {
                id: "replaced",
                schema: &sub_schema,
                overwrite: true,
            },
            AddDef {
                id: "not_overwritten",
                schema: &sub_schema,
                overwrite: false,
            },
        ]);

        insta::assert_json_snapshot!(outcome.unwrap().to_value());

        let no_ref_schema = schema!({
            "$defs": {
                "existing": { "properties": { "f": { "type": "string" }}}
            },
            "type": "object",
            "properties": {
                "c": { "type": "integer" },
                "d": { "type": "string" }
            }
        });
        assert!(!no_ref_schema.references_inferred_schema());
        assert!(!no_ref_schema.references_relaxed_write_schema());
        assert!(!no_ref_schema.references_write_schema());
    }

    #[test]
    fn test_relaxation() {
        let fixture = Schema::new(RawValue::from_str(include_str!("fixture.schema.json")).unwrap());
        insta::assert_json_snapshot!(fixture.to_relaxed_schema().unwrap().to_value())
    }
}
