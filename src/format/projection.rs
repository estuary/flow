//! Types for reasoning about projections of tabular data into potentially nested JSON.
use crate::config::ParseConfig;
use doc::inference::{Exists, Shape};
use doc::{Pointer, Schema, SchemaIndex};
use json::schema::build::Error as SchemaBuildError;
use json::schema::index::Error as SchemaIndexError;
use json::schema::types;
use serde_json::Value;
use std::collections::BTreeMap;

/// Information known about a specific location within a JSON document.
#[derive(Debug, Clone, PartialEq)]
pub struct TypeInfo {
    /// The possible JSON types for this location, if any type information could be inferred.
    pub possible_types: Option<types::Set>,
    pub must_exist: bool,
    pub target_location: Pointer,
}

impl TypeInfo {
    fn from_shape(target_location: Pointer, shape: &Shape, exists: Exists) -> TypeInfo {
        TypeInfo {
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

/// Resolves a map of possible column names to associated type information. This uses both the
/// `projections` and the `schema` from the `config`. Runs an inferrence on the JSON schema to
/// extract type information about known locations. The returned map will contain several possible
/// aliases for each inferred location, but projections from the config will always take
/// precedence.
///
/// Parsers can use the returned map to lookup type information based on parsed column names.
#[tracing::instrument(skip(config))]
pub fn build_projections(config: &ParseConfig) -> Result<BTreeMap<String, TypeInfo>, BuildError> {
    let schema_uri = url::Url::parse("whatever://placeholder").unwrap();
    let schema_json = if config.schema.is_null() {
        &Value::Bool(true)
    } else {
        &config.schema
    };
    let schema: Schema = json::schema::build::build_schema(schema_uri.clone(), &schema_json)?;
    let mut index = SchemaIndex::new();
    index.add(&schema)?;
    let shape = Shape::infer(&schema, &index);

    let mut results = BTreeMap::new();

    for (pointer, shape, exists) in shape.locations() {
        let target_location = Pointer::from_str(pointer.as_str());
        for resolved_field in derive_field_names(pointer.as_str()) {
            let projection = TypeInfo::from_shape(target_location.clone(), shape, exists);
            results.insert(resolved_field, projection);
        }
    }

    // projections from the configuration always take precedence over those we infer from the
    // schema.
    for (field, pointer) in config.projections.iter() {
        let target_location = Pointer::from_str(pointer.as_ref());
        let projection = if let Some((shape, exists)) = shape.locate(&target_location) {
            TypeInfo::from_shape(target_location, shape, exists)
        } else {
            // This isn't a hard error because there may be files that we can still parse correctly
            // even without knowing the types from the schema. For example, this could be a
            // location that simply allows any valid JSON, and the projection may be only for the
            // sake of putting things into the right shape.
            tracing::warn!(
                field = field.as_str(),
                pointer = pointer.as_ref(),
                "could not locate projection within schema"
            );
            TypeInfo {
                target_location,
                must_exist: false,
                possible_types: None,
            }
        };
        results.insert(field.clone(), projection);
    }

    Ok(results)
}

/// Returns a possibly 0-length collection of field names derived from the given JSON pointer.
/// The field names will represent a variety of possible mappings from fields to the location,
/// which will be used to make a best-effort lookup of columns from a tabular data file.
fn derive_field_names(pointer: &str) -> Vec<String> {
    use doc::ptr::Token;
    if !pointer.is_empty() {
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
        let camel_case = split.iter().fold(String::new(), |mut acc, r| {
            if acc.is_empty() {
                acc.push_str(r.as_str());
            } else {
                if let Some(c) = r.chars().next() {
                    acc.push(c.to_ascii_uppercase());
                    acc.extend(r.chars().skip(1));
                }
            }
            acc
        });

        let mut title_case = camel_case
            .chars()
            .take(1)
            .map(|c| c.to_ascii_uppercase())
            .collect::<String>();
        title_case.extend(camel_case.chars().skip(1));

        vec![
            with_underscores,
            camel_case,
            title_case,
            pointer.to_string(),
            pointer[1..].to_string(),
        ]
    } else {
        Vec::new()
    }
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
    fn projections_are_built() {
        let config = ParseConfig {
            projections: map_of!(
                "fieldA" => "/locationa",
                "fieldB" => "/b/loc",
                // Ensure that this projection takes precedence over the generated on
                "BeeLoc" => "/locationa"
            ),
            schema: json!({
                "type": "object",
                "properties": {
                    "locationa": {
                        "type": "integer"
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
                "required": ["locationa", "bee"]
            }),
            ..Default::default()
        };
        let result = build_projections(&config).expect("failed to build projectsions");
        insta::assert_debug_snapshot!(result);
    }
}
