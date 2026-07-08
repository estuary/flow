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
#[schemars(example = Schema::example_absolute())]
#[schemars(example = Schema::example_relative())]
#[schemars(example = Schema::example_inline_basic())]
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
    // URL for referencing the connector schema of a collection, which may be used within a write schema.
    pub const REF_CONNECTOR_SCHEMA_URL: &'static str = "flow://connector-schema";

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

    /// Return a copy of this read-schema bundle in which `date`, `date-time`,
    /// and `time` `format` keywords are removed from the inlined inferred
    /// schema — the `$defs` entry keyed by [`Self::REF_INFERRED_SCHEMA_URL`].
    /// Every other keyword, every other `$defs` entry, and the remainder of the
    /// bundle are left exactly as-is. If the bundle does not inline an inferred
    /// schema (e.g. a single-schema collection), this is a no-op copy.
    ///
    /// This is deliberately much narrower than [`Self::to_relaxed_schema`]: it
    /// touches only `format`, only for the three formats whose validator
    /// delegates to the `time` crate (whose RFC3339 interpretation has drifted
    /// over time), and only within the inferred schema. It exists so read-side
    /// (materialize / derive) document validation does not retroactively reject
    /// historical documents whose date-time values predate a tightening of the
    /// `format` validator, while leaving capture-time write-schema enforcement
    /// and schema inference strict. See estuary/flow#3133.
    pub fn relax_inferred_datetime_formats(&self) -> serde_json::Result<Self> {
        let mut bundle = serde_json::from_str::<serde_json::Value>(self.get())?;

        if let Some(inferred) = bundle
            .get_mut("$defs")
            .and_then(|defs| defs.get_mut(Self::REF_INFERRED_SCHEMA_URL))
        {
            relax_datetime_formats(inferred);
        }
        Ok(Self(RawValue::from_value(&bundle)))
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

/// `format` values whose read-side enforcement is relaxed by
/// [`Schema::relax_inferred_datetime_formats`]. These are exactly the formats
/// whose validator delegates to the `time` crate, whose interpretation of
/// RFC3339 has drifted over time (see estuary/flow#3133, #3108, #3116).
const RELAXED_DATETIME_FORMATS: [&str; 3] = ["date", "date-time", "time"];

/// Recursively remove `date`/`date-time`/`time` `format` keywords from `node`
/// and its sub-schemas. It descends *only* through schema-bearing keywords, so
/// a `format` string appearing within a data-valued keyword (`enum`, `const`,
/// `default`, ...) is never touched — only genuine `format` schema keywords are
/// removed.
fn relax_datetime_formats(node: &mut serde_json::Value) {
    let serde_json::Value::Object(obj) = node else {
        return;
    };

    if let Some(serde_json::Value::String(format)) = obj.get("format") {
        if RELAXED_DATETIME_FORMATS.contains(&format.as_str()) {
            _ = obj.remove("format");
        }
    }

    // Keywords holding a map of named sub-schemas.
    for keyword in ["properties", "patternProperties", "$defs", "definitions"] {
        if let Some(serde_json::Value::Object(map)) = obj.get_mut(keyword) {
            map.values_mut().for_each(relax_datetime_formats);
        }
    }
    // Keywords holding an array of sub-schemas.
    for keyword in ["allOf", "anyOf", "oneOf", "prefixItems"] {
        if let Some(serde_json::Value::Array(arr)) = obj.get_mut(keyword) {
            arr.iter_mut().for_each(relax_datetime_formats);
        }
    }
    // Keywords holding a single sub-schema. `items` may hold either a single
    // sub-schema or (for tuple validation) an array of sub-schemas.
    for keyword in ["additionalProperties", "additionalItems", "items", "not"] {
        match obj.get_mut(keyword) {
            Some(serde_json::Value::Array(arr)) => arr.iter_mut().for_each(relax_datetime_formats),
            Some(sub) => relax_datetime_formats(sub),
            None => {}
        }
    }
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
    #[serde(
        default,
        deserialize_with = "deserialize_additional_properties",
        skip_serializing_if = "Option::is_none"
    )]
    additional_properties: Option<Box<RelaxedSchema>>,

    // Keywords which are removed from a relaxed schema.
    #[serde(rename = "type", default, skip_serializing)]
    _type: Option<serde_json::Value>,
    #[serde(rename = "required", default, skip_serializing)]
    _required: Vec<String>,
    #[serde(rename = "format", default, skip_serializing)]
    _format: String,
    #[serde(rename = "const", default, skip_serializing)]
    _const: String,
    #[serde(rename = "enum", default, skip_serializing)]
    _enum: Vec<serde_json::Value>,
    #[serde(rename = "minLength", default, skip_serializing)]
    _min_length: Option<serde_json::Value>,
    #[serde(rename = "maxLength", default, skip_serializing)]
    _max_length: Option<serde_json::Value>,
    #[serde(rename = "redact", default, skip_serializing)]
    _redact: Option<serde_json::Value>,
    #[serde(rename = "x-str-minimum", default, skip_serializing)]
    _x_str_minimum: Option<serde_json::Value>,
    #[serde(rename = "x-str-maximum", default, skip_serializing)]
    _x_str_maximum: Option<serde_json::Value>,

    // Other keywords are passed-through.
    #[serde(flatten)]
    pass_through: BTreeMap<String, serde_json::Value>,
}

fn deserialize_additional_properties<'de, D>(
    deserializer: D,
) -> Result<Option<Box<RelaxedSchema>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = Option::<RelaxedSchema>::deserialize(deserializer)?;

    Ok(match v {
        // Drop closed-object constraint in relaxed schemas
        Some(RelaxedSchema::Bool(false)) => None,

        // Preserve true or schema-valued additionalProperties
        Some(other) => Some(Box::new(other)),

        None => None,
    })
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

    #[test]
    fn test_relaxation_drops_min_max_length() {
        let schema = schema!({
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 255,
                    "description": "A name field"
                }
            }
        });

        let relaxed = schema.to_relaxed_schema().unwrap().to_value();

        assert_eq!(
            relaxed,
            json!({
                "properties": {
                    "name": {
                        "description": "A name field"
                    }
                }
            })
        );
    }

    #[test]
    fn test_relaxation_drops_x_str_bounds() {
        // x-str-minimum/maximum are enforced validations reflecting the source's
        // *current* column type, and earlier collection data may predate them.
        // For example: a once-text column captured values like "n/a" alongside
        // "10.5", and was later altered to NUMERIC(4,2) — at which point the
        // capture's write schema could reasonably declare bounds of ±99.99.
        // Those bounds are a lie about the older data (an x-str bound alone
        // rejects any non-numeric string), so the relaxed write schema must not
        // carry them into its intersection with the inferred schema.
        let schema = schema!({
            "type": "object",
            "properties": {
                "price": {
                    "type": "string",
                    "format": "number",
                    "x-str-minimum": "-99.99",
                    "x-str-maximum": "99.99",
                    "description": "A numeric-string field"
                }
            }
        });

        let relaxed = schema.to_relaxed_schema().unwrap().to_value();

        assert_eq!(
            relaxed,
            json!({
                "properties": {
                    "price": {
                        "description": "A numeric-string field"
                    }
                }
            })
        );
    }

    #[test]
    fn test_relaxation_drops_redact() {
        let schema = schema!({
            "type": "object",
            "properties": {
                "email": {
                    "type": "string",
                    "redact": { "strategy": "sha256" },
                    "description": "Sensitive key component"
                }
            }
        });

        let relaxed = schema.to_relaxed_schema().unwrap().to_value();

        assert_eq!(
            relaxed,
            json!({
                "properties": {
                    "email": {
                        "description": "Sensitive key component"
                    }
                }
            })
        );
    }

    #[test]
    fn test_relaxation_drops_additional_properties_false() {
        let schema = schema!({
            "type": "object",
            "properties": {
                "known": { "type": "string" }
            },
            "additionalProperties": false
        });

        let relaxed = schema.to_relaxed_schema().unwrap().to_value();

        // `type` is removed, and `additionalProperties: false` must be dropped
        // so the relaxed schema permits unknown fields.
        assert_eq!(
            relaxed,
            json!({
                "properties": {
                    "known": {}
                }
            })
        );
    }

    #[test]
    fn test_relaxation_preserves_additional_properties_true() {
        let schema = schema!({
            "type": "object",
            "properties": {
                "known": { "type": "string" }
            },
            "additionalProperties": true
        });

        let relaxed = schema.to_relaxed_schema().unwrap().to_value();

        // We only drop the closed-object constraint (false). True should remain.
        assert_eq!(
            relaxed,
            json!({
                "properties": {
                    "known": {}
                },
                "additionalProperties": true
            })
        );
    }

    #[test]
    fn test_relaxation_preserves_schema_valued_additional_properties() {
        let schema = schema!({
            "type": "object",
            "properties": {
                "known": { "type": "string" }
            },
            "additionalProperties": { "type": "integer" }
        });

        let relaxed = schema.to_relaxed_schema().unwrap().to_value();

        // Schema-valued `additionalProperties` should be preserved, but its own
        // `type` should be relaxed away.
        assert_eq!(
            relaxed,
            json!({
                "properties": {
                    "known": {}
                },
                "additionalProperties": {}
            })
        );
    }

    #[test]
    fn test_relax_inferred_datetime_only_touches_inferred_defs_formats() {
        // A read-schema bundle shaped like one assembled by the control plane:
        // the inferred schema is inlined as a `$defs` entry keyed by its URL,
        // alongside the (relaxed) write schema, with a top-level `allOf`.
        let bundle = schema!({
            "$defs": {
                "flow://inferred-schema": {
                    "$id": "flow://inferred-schema",
                    "type": "object",
                    "required": ["ts"],
                    "properties": {
                        // date-time formats: relaxed away.
                        "ts": { "type": "string", "format": "date-time", "minLength": 1 },
                        "d": { "type": "string", "format": "date" },
                        "t": { "type": "string", "format": "time" },
                        // Non-time formats: preserved.
                        "email": { "type": "string", "format": "email" },
                        "ip": { "type": "string", "format": "ipv4" },
                        // A property literally named "format" holding a
                        // sub-schema must not be mistaken for a keyword.
                        "format": { "type": "string" },
                        // Nested containers are descended into.
                        "nested": {
                            "type": "object",
                            "properties": {
                                "inner_ts": { "type": "string", "format": "date-time" }
                            },
                            "additionalProperties": { "type": "string", "format": "time" }
                        },
                        "list": {
                            "type": "array",
                            "items": { "type": "string", "format": "date-time" }
                        },
                        // A `date-time` string appearing as *data* (enum/const/
                        // default) is never touched.
                        "kind": { "type": "string", "enum": ["date-time", "date"] },
                        "constish": { "const": { "format": "date-time" } }
                    }
                },
                "flow://write-schema": {
                    "$id": "flow://write-schema",
                    "properties": {
                        // Author-declared format outside the inferred schema is
                        // left strictly enforced.
                        "authored": { "type": "string", "format": "date-time" }
                    }
                }
            },
            "allOf": [
                {"$ref": "flow://write-schema"},
                {"$ref": "flow://inferred-schema"}
            ]
        });

        let relaxed = bundle.relax_inferred_datetime_formats().unwrap().to_value();

        assert_eq!(
            relaxed,
            json!({
                "$defs": {
                    "flow://inferred-schema": {
                        "$id": "flow://inferred-schema",
                        "type": "object",
                        "required": ["ts"],
                        "properties": {
                            "ts": { "type": "string", "minLength": 1 },
                            "d": { "type": "string" },
                            "t": { "type": "string" },
                            "email": { "type": "string", "format": "email" },
                            "ip": { "type": "string", "format": "ipv4" },
                            "format": { "type": "string" },
                            "nested": {
                                "type": "object",
                                "properties": {
                                    "inner_ts": { "type": "string" }
                                },
                                "additionalProperties": { "type": "string" }
                            },
                            "list": {
                                "type": "array",
                                "items": { "type": "string" }
                            },
                            "kind": { "type": "string", "enum": ["date-time", "date"] },
                            "constish": { "const": { "format": "date-time" } }
                        }
                    },
                    // Untouched: only the inferred `$def` is relaxed.
                    "flow://write-schema": {
                        "$id": "flow://write-schema",
                        "properties": {
                            "authored": { "type": "string", "format": "date-time" }
                        }
                    }
                },
                "allOf": [
                    {"$ref": "flow://write-schema"},
                    {"$ref": "flow://inferred-schema"}
                ]
            })
        );
    }

    #[test]
    fn test_relax_inferred_datetime_is_noop_without_inferred_defs() {
        // A single-schema collection (no inlined inferred schema) is unchanged,
        // including any authored date-time formats.
        let bundle = schema!({
            "type": "object",
            "properties": {
                "ts": { "type": "string", "format": "date-time" }
            }
        });
        let relaxed = bundle.relax_inferred_datetime_formats().unwrap().to_value();
        assert_eq!(relaxed, bundle.to_value());
    }
}
