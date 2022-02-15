//! Types for reasoning about projections of tabular data into potentially nested JSON.
use crate::config::ParseConfig;
use caseless::Caseless;
use doc::inference::{Exists, Shape};
use doc::{Pointer, Schema, SchemaIndexBuilder};
use json::schema::build::Error as SchemaBuildError;
use json::schema::index::Error as SchemaIndexError;
use json::schema::types;
use serde_json::Value;
use std::collections::BTreeMap;
use std::rc::Rc;
use unicode_normalization::UnicodeNormalization;

/// Information known about a specific location within a JSON document. The type information here
/// is a simplified and stripped down version of what's included in `Shape`. It might make sense to
/// simply embed the entire `Shape` here in the future if there's a reason for parsers to desire
/// richer type information.
#[derive(Debug, Clone, PartialEq)]
pub struct Projection {
    /// The possible JSON types for this location, if any type information could be inferred.
    pub possible_types: Option<types::Set>,
    /// True only if this location must exist in order for a JSON document to validate against the
    /// schema. Otherwise, false.
    pub must_exist: bool,
    /// The specific sublocation within JSON documents to which this type information applies.
    pub target_location: Pointer,
}

impl Projection {
    fn from_shape(target_location: Pointer, shape: &Shape, exists: Exists) -> Projection {
        Projection {
            target_location,
            must_exist: exists == Exists::Must,
            possible_types: Some(shape.type_),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("failed to parse json schema: {0}")]
    InvalidSchema(#[from] SchemaBuildError),
    #[error("cannot process json schema: {0}")]
    SchemaIndex(#[from] SchemaIndexError),
}

/// Projections is a helper for constructing potentially nested JSON documents from flat tabular
/// input.
#[derive(Debug)]
pub struct Projections {
    /// Type inferrence information from the root JSON schema.
    shape: Shape,
    /// Projections stored with field names exactly as they are in the config/schema.
    case_sensitive: BTreeMap<String, Rc<Pointer>>,
    /// Projections stored with field names that have been normalized and converted to lowercase,
    /// to allow for case-insensitive matching against field names. These are stored in a separate
    /// map mostly to make the lookup code easier to read.
    case_insensitive: BTreeMap<String, Rc<Pointer>>,
}

impl Projections {
    /// Returns a projection for the given column name. This involves two steps:
    ///
    /// First is to resolve the given `column_name` to a JSON pointer that identifies it's desired
    /// location within JSON documents. This uses both user-provided and automatically inferred
    /// projections, with precedence of:
    ///
    /// 1. Exact match of user-provided projection
    /// 2. Exact match of inferred projection
    /// 3. Case-insensitive match of user-provided projection
    /// 4. Case-insensitive match of inferred projection
    /// 5. Convert the raw `column_name` into a JSON pointer by prefixing it with a `/` (without
    ///    escaping internal `/` characters that may be included in the name)
    ///
    /// The second step is to consult the JSON schema for type information about the location that
    /// was resolved in step one.
    /// The resolved pointer and type information are returned as part of the `Projection`.
    pub fn lookup(&self, field_name: &str) -> Projection {
        let ptr = self
            .case_sensitive
            .get(field_name)
            .or_else(|| {
                let case_i_name = lowercase(field_name);
                self.case_insensitive.get(&case_i_name)
            })
            .map(|ptr| (&**ptr).clone())
            .unwrap_or_else(|| {
                let field_as_ptr = String::from("/") + field_name;
                Pointer::from(&field_as_ptr)
            });

        if let Some((shape, exists)) = self.shape.locate(&ptr) {
            // If the inferrence says that this location cannot exist, then we'll log a warning as
            // a kindness to the user, since they will soon be engaged in debugging.
            if let Exists::Cannot = exists {
                tracing::warn!(
                    ?shape,
                    ?exists,
                    field = field_name,
                    pointer = ?ptr,
                    "inferred type information says that this field cannot exist within a valid document"
                );
            };
            tracing::debug!(
                ?shape,
                ?exists,
                field = field_name,
                pointer = ?ptr,
                "located field within schema"
            );
            Projection::from_shape(ptr, shape, exists)
        } else {
            // This isn't an error because there may be files that we can still parse correctly
            // even without knowing the types from the schema. For example, this could be a
            // location that simply allows any valid JSON, and the projection may be only for the
            // sake of putting things into the right shape. But we definitely want to log here
            // because this may be very helpful if the parser starts producing documents that fail
            // validation.
            tracing::info!(
                pointer = ?ptr,
                field = field_name,
                "could not locate projection within schema"
            );
            Projection {
                possible_types: None,
                must_exist: false,
                target_location: ptr,
            }
        }
    }
}

/// Resolves a map of possible column names to associated type information. This uses both the
/// `projections` and the `schema` from the `config`. Runs an inferrence on the JSON schema to
/// extract type information about known locations. The returned map will contain several possible
/// aliases for each inferred location, but projections from the config will always take
/// precedence.
///
/// Parsers can use the returned map to lookup type information based on parsed column names.
#[tracing::instrument(skip(config))]
pub fn build_projections(config: &ParseConfig) -> Result<Projections, BuildError> {
    let schema_uri = url::Url::parse("whatever://placeholder").unwrap();
    let schema_json = if config.schema.is_null() {
        &Value::Bool(true)
    } else {
        &config.schema
    };
    let schema: Schema = json::schema::build::build_schema(schema_uri.clone(), &schema_json)?;
    let mut builder = SchemaIndexBuilder::new();
    builder.add(&schema)?;
    let index = builder.into_index();
    let shape = Shape::infer(&schema, &index);

    let mut results = BTreeMap::new();
    let mut case_i_results = BTreeMap::new();

    for (pointer, _, _) in shape.locations() {
        let target_location = Rc::new(Pointer::from_str(pointer.as_str()));
        for resolved_field in derive_field_names(pointer.as_str()) {
            case_i_results.insert(lowercase(&resolved_field), target_location.clone());
            results.insert(resolved_field, target_location.clone());
        }
    }

    // projections from the configuration always take precedence over those we infer from the
    // schema.
    for (field, pointer) in config.projections.iter() {
        let pointer = Rc::new(Pointer::from(pointer.as_ref()));
        case_i_results.insert(lowercase(field), pointer.clone());
        results.insert(field.to_string(), pointer);
    }

    Ok(Projections {
        shape,
        case_sensitive: results,
        case_insensitive: case_i_results,
    })
}

/// Maps each codepoint in `s` to its collated form,
/// which ignores casing and is unicode-normalized.
/// This follows the conformance guidelines in:
/// http://www.unicode.org/versions/Unicode13.0.0/ch03.pdf
/// in Section 3.13 - "Default Caseless Matching" (all the way at the bottom).
fn lowercase(s: &str) -> String {
    s.chars().nfd().default_case_fold().nfkc().collect()
}

/// Returns a possibly 0-length collection of field names derived from the given JSON pointer.
/// The field names will represent a variety of possible mappings from fields to the location,
/// which will be used to make a best-effort lookup of columns from a tabular data file.
fn derive_field_names(pointer: &str) -> Vec<String> {
    use doc::ptr::Token;

    if pointer.is_empty() {
        return Vec::new();
    }

    let split = Pointer::from(pointer)
        .iter()
        .map(|t| match t {
            Token::Index(i) => i.to_string(),
            Token::Property(p) => p.to_string(),
            Token::NextIndex => "-".to_string(),
        })
        .collect::<Vec<_>>();

    let with_underscores = split.iter().fold(String::new(), |mut acc, r| {
        if !acc.is_empty() {
            acc.push('_');
        }
        acc.push_str(r.as_str());
        acc
    });
    let with_spaces = split.iter().fold(String::new(), |mut acc, r| {
        if !acc.is_empty() {
            acc.push(' ');
        }
        acc.push_str(r.as_str());
        acc
    });
    let with_no_delim = split.iter().fold(String::new(), |mut acc, r| {
        acc.push_str(r.as_str());
        acc
    });

    let mut variants = vec![
        with_underscores,
        with_spaces,
        with_no_delim,
        pointer.to_string(),
        // pointer with the first `/` removed matches the projections generated during Flow catalog builds
        pointer[1..].to_string(),
    ];

    // For properties of the document root (only) having underscores,
    // allow the property to also match a space-delimited variant
    // of its constituent parts.
    if split.len() == 1 {
        variants.push(split[0].replace("_", " "))
    }

    variants.sort();
    variants.dedup();
    variants
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    macro_rules! map_of{
        ($($key:expr => $val:expr),*) => {{
            let mut m = BTreeMap::new();
            $(
                m.insert($key.into(), $val.into());
             )*
            m
        }}
    }

    #[test]
    fn test_derived_names() {
        assert_eq!(
            derive_field_names("/foo_bar"),
            vec!["/foo_bar", "foo bar", "foo_bar"]
        );

        assert_eq!(
            derive_field_names("/foo_bar/baz"),
            vec![
                "/foo_bar/baz",
                "foo_bar baz",
                "foo_bar/baz",
                "foo_bar_baz",
                "foo_barbaz",
                // Note that "foo bar baz" is not included.
            ]
        );
    }

    #[test]
    fn test_projection_lookup() {
        let config = ParseConfig {
            projections: map_of!(
                "fieldA" => "/locationa",
                "fieldB" => "/b/loc",
                // Ensure that this projection takes precedence over the generated one.
                "BeeLoc" => "/locationa",
                "notReal" => "/cannot/exist"
            ),
            schema: json!({
                "type": "object",
                "properties": {
                    "locationa": {
                        "type": "integer"
                    },
                    "LOCATIONA": {
                        "type": "string"
                    },
                    "bee": {
                        "type": "object",
                        "properties": {
                            "loc": { "type": "string" },
                            "rock": {
                                "type": "object",
                                "properties": {
                                    "flower": { "type": "boolean" }
                                }
                            }
                        }
                    }
                },
                "patternProperties": {
                    "bool.*": {"type": "boolean"}
                },
                "additionalProperties": {
                    "type": "number"
                },
                "required": ["locationa", "bee"],

            }),
            ..Default::default()
        };
        let projections = build_projections(&config).expect("failed to build projections");

        fn projection(ty: Option<types::Set>, must_exist: bool, ptr: &str) -> Projection {
            Projection {
                possible_types: ty,
                must_exist,
                target_location: Pointer::from_str(ptr),
            }
        }

        let expected: &[(&[&str], Projection)] = &[
            (
                &[
                    "fieldA",
                    "BeeLoc",
                    "BEELOC",
                    "fielda",
                    "FIELDA",
                    "locationa",
                    "/locationa",
                ],
                projection(Some(types::INTEGER), true, "/locationa"),
            ),
            (
                // An exact case-sensitive match should always take precedence over a case-insensitive match
                &["LOCATIONA"],
                projection(Some(types::STRING), false, "/LOCATIONA"),
            ),
            (
                &["BEE", "bee", "Bee"],
                projection(Some(types::OBJECT), true, "/bee"),
            ),
            (
                // Type information accounts for patternProperties.
                &["booleananananana"],
                projection(Some(types::BOOLEAN), false, "/booleananananana"),
            ),
            (
                // Type information accounts for additionalProperties.
                &["extra"],
                projection(Some(types::INT_OR_FRAC), false, "/extra"),
            ),
            (
                // Test different separators
                &[
                    "BEE_rocK_flOWER",
                    "/bee/rock/flower",
                    "BEE/ROCK/FLOWER",
                    "bEE rOcK FLOwer",
                ],
                projection(Some(types::BOOLEAN), false, "/bee/rock/flower"),
            ),
            (
                // This should not return type information, but the location pointer should match
                // the one provided in the projections of the config.
                &["notReal", "NOTREAL"],
                projection(None, false, "/cannot/exist"),
            ),
        ];
        for (fields, type_info) in expected {
            for field in *fields {
                let actual = projections.lookup(field);
                assert_eq!(
                    type_info, &actual,
                    "mismatch for field: {:?}, expected: {:?}, actual: {:?}",
                    field, expected, actual
                );
            }
        }
    }
}
