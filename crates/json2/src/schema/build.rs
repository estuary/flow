use crate::schema::Keyword;
use crate::schema::{self, keywords, types};
use crate::scope::Scope;
use itertools::Itertools;
use serde::Deserialize;

#[derive(thiserror::Error, Debug)]
pub enum Error<A: schema::Annotation + 'static> {
    #[error("expected an array")]
    ExpectedArray,
    #[error("expected a boolean")]
    ExpectedBool,
    #[error("unexpected JSON Schema keyword")]
    ExpectedKeyword,
    #[error("expected a number")]
    ExpectedNumber,
    #[error("expected an object")]
    ExpectedObject,
    #[error("expected a JSON Schema")]
    ExpectedSchema,
    #[error("expected a string")]
    ExpectedString,
    #[error("expected an unsigned integer")]
    ExpectedUnsigned,
    #[error("expected a URL")]
    ExpectedURL,
    #[error("unexpected URL fragment")]
    UnexpectedURLFragment,

    #[error(transparent)]
    Annotation(A::KeywordError),
    #[error(transparent)]
    Json(serde_json::Error),
    #[error(transparent)]
    Regex(#[from] regex::Error),
    #[error(transparent)]
    URL(#[from] url::ParseError),
}

#[derive(thiserror::Error, Debug)]
#[error("invalid schema at '{scope}'")]
pub struct ScopedError<A: schema::Annotation + 'static> {
    pub scope: url::Url,
    #[source]
    pub inner: Error<A>,
}
pub type Errors<A> = Vec<ScopedError<A>>;

/// Build a JSON Schema from a serde_json::Value having the canonical URI.
/// If any errors are encountered, they are collected and returned.
pub fn build_schema<'l, 's, A>(
    curi: &'l url::Url,
    value: &'s serde_json::Value,
) -> Result<schema::Schema<A>, Errors<A>>
where
    A: schema::Annotation,
{
    let scope = Scope::new(curi);
    let mut errors = Errors::new();
    let schema = build(scope, value, &mut errors);

    if errors.is_empty() {
        Ok(schema)
    } else {
        Err(errors)
    }
}

impl<A: schema::Annotation> Error<A> {
    fn push<'l>(self, scope: Scope<'l>, errors: &mut Vec<ScopedError<A>>) {
        errors.push(ScopedError {
            scope: scope.flatten(),
            inner: self,
        });
    }
}

fn build<'l, 's, A>(
    scope: Scope<'l>,
    value: &'s serde_json::Value,
    errors: &mut Errors<A>,
) -> schema::Schema<A>
where
    A: schema::Annotation,
{
    let kw = match value {
        serde_json::Value::Object(m) => build_object_keywords::<A>(scope, m, errors),
        serde_json::Value::Bool(b) => {
            let id = Keyword::Id {
                curi: Into::<String>::into(scope.flatten()).into(),
                explicit: false,
            };

            if *b {
                vec![id] // Match anything.
            } else {
                vec![id, Keyword::False] // Match nothing.
            }
        }
        _ => {
            Error::ExpectedSchema.push(scope, errors);
            vec![]
        }
    };
    schema::Schema { kw: kw.into() }
}

fn build_object_keywords<'l, 's, A>(
    scope: Scope<'l>,
    map: &'s serde_json::Map<String, serde_json::Value>,
    errors: &mut Errors<A>,
) -> Vec<Keyword<A>>
where
    A: schema::Annotation,
{
    let maybe_id: Option<url::Url>;
    let mut keywords: Vec<Keyword<A>> = Vec::new();

    // First extract $id, as it changes the Scope's base resource.
    let scope = if let Some(id) = map.get(keywords::ID) {
        maybe_id = Some(expect_url(scope.push_prop(keywords::ID), id, errors));
        let id = maybe_id.as_ref().unwrap();

        keywords.push(Keyword::Id {
            curi: id.to_string().into(),
            explicit: true,
        });
        scope.push_resource(id)
    } else {
        keywords.push(Keyword::Id {
            curi: Into::<String>::into(scope.flatten()).into(),
            explicit: false,
        });
        scope
    };

    let mut properties = None;
    let mut required = None;
    let mut nullable = false;

    for (keyword, value) in map {
        let scope = scope.push_prop(keyword);
        let mut unknown = false;

        match keyword.as_str() {
            // Note: Annotation and False are not keywords, they're handled elsewhere.
            // $id is already handled outside of this match.
            keywords::ADDITIONAL_ITEMS => {
                if map.get(keywords::ITEMS).filter(|v| v.is_array()).is_none() {
                    // 2019-09 "additionalItems" is ignored if "items" is not present,
                    // or is present and is not an array.
                } else {
                    let items = Box::new(build::<A>(scope, value, errors));
                    keywords.push(Keyword::Items { items });
                }
            }
            keywords::ADDITIONAL_PROPERTIES => {
                keywords.push(Keyword::AdditionalProperties {
                    additional_properties: Box::new(build::<A>(scope, value, errors)),
                });
            }
            keywords::ALL_OF => {
                let all_of = build_schema_array(scope, value, errors).into();
                keywords.push(Keyword::AllOf { all_of });
            }
            keywords::ANCHOR => {
                let anchor = expect_relative_url(scope, true, value, errors)
                    .to_string()
                    .into();
                keywords.push(Keyword::Anchor { anchor });
            }
            keywords::ANY_OF => {
                let any_of = build_schema_array(scope, value, errors).into();
                keywords.push(Keyword::AnyOf { any_of });
            }
            keywords::COMMENT => {
                let comment = expect_str(scope, value, errors).to_string().into();
                keywords.push(Keyword::Comment { comment });
            }
            keywords::CONST => {
                let r#const = Box::new(value.clone());
                keywords.push(Keyword::Const { r#const });
            }
            keywords::CONTAINS => {
                let contains = Box::new(build::<A>(scope, value, errors));
                keywords.push(Keyword::Contains { contains });

                // The presence of "contains" implies "minContains: 1" if
                // it's not explicitly specified.
                if !map.contains_key(keywords::MIN_CONTAINS) {
                    keywords.push(Keyword::MinContains { min_contains: 1 });
                }
            }
            keywords::DEFS => {
                let defs = build_frozen_schema_map(scope, value, errors);
                keywords.push(Keyword::Defs { defs });
            }
            keywords::DEFINITIONS => {
                let definitions = build_frozen_schema_map(scope, value, errors);
                keywords.push(Keyword::Definitions { definitions });
            }
            keywords::DEPENDENT_REQUIRED => {
                // Map `dependentRequired` into its equivalent `dependentSchemas`.
                let dependent_schemas = expect_map(scope, value, errors)
                    .iter()
                    .map(|(prop, value)| {
                        let schema = build::<A>(
                            scope.push_prop(prop),
                            &serde_json::json!({
                                "required": value,
                            }),
                            errors,
                        );
                        (prop.to_string().into(), schema)
                    })
                    .collect::<Vec<_>>()
                    .into();

                keywords.push(Keyword::DependentSchemas { dependent_schemas });
            }
            keywords::DEPENDENT_SCHEMAS => {
                let dependent_schemas = build_frozen_schema_map(scope, value, errors);
                keywords.push(Keyword::DependentSchemas { dependent_schemas });
            }
            keywords::DYNAMIC_ANCHOR => {
                let dynamic_anchor = expect_relative_url(scope, true, value, errors)
                    .to_string()
                    .into();
                keywords.push(Keyword::DynamicAnchor { dynamic_anchor });
            }
            keywords::DYNAMIC_REF | keywords::RECURSIVE_REF => {
                // Unlike $ref, we do NOT canonicalize $dynamicRef.
                // That happens at validation time, where we walk the dynamic
                // scope to determine base URL(s) to join and query against.
                let dynamic_ref = expect_str(scope, value, errors).to_string().into();
                keywords.push(Keyword::DynamicRef { dynamic_ref });
            }
            keywords::ELSE => {
                if map.get(keywords::IF).is_some() {
                    let r#else = Box::new(build::<A>(scope, value, errors));
                    keywords.push(Keyword::Else { r#else });
                }
            }
            keywords::ENUM => {
                let r#enum = expect_array(scope, value, errors).to_vec().into();
                keywords.push(Keyword::Enum { r#enum });
            }
            keywords::EXCLUSIVE_MAXIMUM => {
                let exclusive_maximum = expect_number(scope, value, errors);
                keywords.push(Keyword::ExclusiveMaximum { exclusive_maximum });
            }
            keywords::EXCLUSIVE_MINIMUM => {
                let exclusive_minimum = expect_number(scope, value, errors);
                keywords.push(Keyword::ExclusiveMinimum { exclusive_minimum });
            }
            keywords::FORMAT => match serde_json::from_value(value.clone()).map_err(Error::Json) {
                Ok(format) => {
                    keywords.push(Keyword::Format { format });
                }
                Err(err) => {
                    err.push(scope, errors);
                }
            },
            keywords::ID => (), // Already handled.
            keywords::IF => {
                let r#if = Box::new(build::<A>(scope, value, errors));
                keywords.push(Keyword::If { r#if });
            }
            keywords::ITEMS if value.is_array() => {
                // 2019-09 "items" as array is equivalent to "prefixItems".
                let prefix_items = build_schema_array(scope, value, errors).into();
                keywords.push(Keyword::PrefixItems { prefix_items });
            }
            keywords::ITEMS => {
                let items = Box::new(build::<A>(scope, value, errors));
                keywords.push(Keyword::Items { items });
            }
            keywords::MAXIMUM => {
                let maximum = expect_number(scope, value, errors);
                keywords.push(Keyword::Maximum { maximum });
            }
            keywords::MAX_CONTAINS => {
                let max_contains = expect_unsigned(scope, value, errors);
                keywords.push(Keyword::MaxContains { max_contains });
            }
            keywords::MAX_ITEMS => {
                let max_items = expect_unsigned(scope, value, errors);
                keywords.push(Keyword::MaxItems { max_items });
            }
            keywords::MAX_LENGTH => {
                let max_length = expect_unsigned(scope, value, errors);
                keywords.push(Keyword::MaxLength { max_length });
            }
            keywords::MAX_PROPERTIES => {
                let max_properties = expect_unsigned(scope, value, errors);
                keywords.push(Keyword::MaxProperties { max_properties });
            }
            keywords::MINIMUM => {
                let minimum = expect_number(scope, value, errors);
                keywords.push(Keyword::Minimum { minimum });
            }
            keywords::MIN_CONTAINS => {
                if map.contains_key(keywords::CONTAINS) {
                    let min_contains = expect_unsigned(scope, value, errors);
                    keywords.push(Keyword::MinContains { min_contains });
                }
            }
            keywords::MIN_ITEMS => {
                let min_items = expect_unsigned(scope, value, errors);
                keywords.push(Keyword::MinItems { min_items });
            }
            keywords::MIN_LENGTH => {
                let min_length = expect_unsigned(scope, value, errors);
                keywords.push(Keyword::MinLength { min_length });
            }
            keywords::MIN_PROPERTIES => {
                let min_properties = expect_unsigned(scope, value, errors);
                keywords.push(Keyword::MinProperties { min_properties });
            }
            keywords::MULTIPLE_OF => {
                let multiple_of = expect_number(scope, value, errors);
                keywords.push(Keyword::MultipleOf { multiple_of });
            }
            keywords::NOT => {
                let not = Box::new(build::<A>(scope, value, errors));
                keywords.push(Keyword::Not { not });
            }
            keywords::NULLABLE => {
                // Support OpenAPI versions prior to 3.1, by merging `nullable` with `type`.
                nullable = expect_bool(scope, value, errors);
            }
            keywords::ONE_OF => {
                let one_of = build_schema_array(scope, value, errors).into();
                keywords.push(Keyword::OneOf { one_of });
            }
            keywords::PATTERN => {
                let pattern = expect_str(scope, value, errors);
                let pattern = Box::new(match regex::Regex::new(pattern) {
                    Ok(re) => re,
                    Err(err) => {
                        Error::Regex(err).push(scope, errors);
                        regex::Regex::new("placeholder").unwrap()
                    }
                });
                keywords.push(Keyword::Pattern { pattern });
            }
            keywords::PATTERN_PROPERTIES => {
                let pattern_properties = build_schema_map(scope, value, errors)
                    .into_iter()
                    .map(|(pattern, schema)| {
                        let pattern = match regex::Regex::new(pattern) {
                            Ok(re) => re,
                            Err(err) => {
                                Error::Regex(err).push(scope.push_prop(pattern), errors);
                                regex::Regex::new("placeholder").unwrap()
                            }
                        };
                        (pattern, schema)
                    })
                    .collect::<Vec<_>>()
                    .into();
                keywords.push(Keyword::PatternProperties { pattern_properties });
            }
            keywords::PREFIX_ITEMS => {
                let prefix_items = build_schema_array(scope, value, errors).into();
                keywords.push(Keyword::PrefixItems { prefix_items });
            }
            keywords::PROPERTIES => {
                properties = Some(build_schema_map(scope, value, errors));
                // We'll post-process with `required` after walking schema keywords.
            }
            keywords::PROPERTY_NAMES => {
                let property_names = Box::new(build::<A>(scope, value, errors));
                keywords.push(Keyword::PropertyNames { property_names });
            }
            keywords::RECURSIVE_ANCHOR => {
                // Legacy name for $dynamicAnchor.
                if expect_bool(scope, value, errors) {
                    let value = serde_json::Value::String("#legacy-recursive-anchor".to_string());
                    let dynamic_anchor = expect_relative_url(scope, true, &value, errors)
                        .to_string()
                        .into();
                    keywords.push(Keyword::DynamicAnchor { dynamic_anchor });
                }
            }
            keywords::REF => {
                // A relative $ref is projected into its canonical and absolute URL.
                let r#ref = expect_relative_url(scope, false, value, errors)
                    .to_string()
                    .into();
                keywords.push(Keyword::Ref { r#ref });
            }
            keywords::REQUIRED => {
                let mut r = expect_array(scope, value, errors)
                    .iter()
                    .enumerate()
                    .map(|(i, value)| expect_str(scope.push_item(i), value, errors))
                    .collect::<Vec<_>>();
                r.sort();
                r.dedup();
                required = Some(r); // We'll post-process after walking keywords.
            }
            keywords::SCHEMA => {} // No-op.
            keywords::THEN => {
                if map.get(keywords::IF).is_some() {
                    let then = Box::new(build::<A>(scope, value, errors));
                    keywords.push(Keyword::Then { then });
                }
            }
            keywords::TYPE => {
                // As a support crutch for OpenAPI versions prior to 3.1,
                // merge a "nullable" keyword into the "type" keyword.
                let actual = match types::Set::deserialize(value) {
                    Ok(actual) => actual,
                    Err(err) => {
                        Error::Json(err).push(scope, errors);
                        types::INVALID
                    }
                };
                keywords.push(Keyword::Type {
                    r#type: actual
                        | if nullable {
                            types::NULL
                        } else {
                            types::INVALID
                        },
                });
            }
            keywords::UNEVALUATED_ITEMS => {
                keywords.push(Keyword::UnevaluatedItems {
                    unevaluated_items: Box::new(build::<A>(scope, value, errors)),
                });
            }
            keywords::UNEVALUATED_PROPERTIES => {
                keywords.push(Keyword::UnevaluatedProperties {
                    unevaluated_properties: Box::new(build::<A>(scope, value, errors)),
                });
            }
            keywords::UNIQUE_ITEMS => {
                if expect_bool(scope, value, errors) {
                    keywords.push(Keyword::UniqueItems {});
                }
            }
            keywords::VOCABULARY => {} // No-op.

            keyword if keyword.starts_with("x-") => (), // Ignore extension keywords.

            _ => {
                unknown = true;
            }
        }

        if A::uses_keyword(keyword) {
            match A::from_keyword(keyword, value) {
                Ok(annotation) => {
                    keywords.push(Keyword::Annotation {
                        annotation: Box::new(annotation),
                    });
                }
                Err(err) => {
                    Error::Annotation(err).push(scope, errors);
                }
            }
            unknown = false;
        }

        if unknown {
            Error::ExpectedKeyword.push(scope, errors)
        }
    } // End loop over schema schema object map.

    if properties.is_some() || required.is_some() {
        // `properties` and `required` are already sorted on ascending property.
        let properties = properties.unwrap_or_default();
        let required = required.unwrap_or_default();

        // Note we're walking properties in ascending order,
        // allocating them into FrozenString and using concat() to ensure exact
        // capacities are requested. This maximizes the likelihood of strings
        // being packed and ordered in shared cache lines.

        let properties = (properties.into_iter())
            .merge_join_by(required.into_iter(), |(l, _), r| l.cmp(r))
            .map(|eob| match eob {
                itertools::EitherOrBoth::Left((prop, schema)) => {
                    (["?", prop].concat().into(), schema) // Optional property.
                }
                itertools::EitherOrBoth::Both((prop, schema), _req) => {
                    (["!", prop].concat().into(), schema) // Required property.
                }
                itertools::EitherOrBoth::Right(prop) => {
                    let id = Keyword::Id {
                        curi: Into::<String>::into(
                            scope.push_prop("require").push_prop(prop).flatten(),
                        )
                        .into(),
                        explicit: false,
                    };
                    let schema = schema::Schema {
                        kw: vec![id].into(),
                    };
                    (["+", prop].concat().into(), schema) // Only in `required`, not `properties`.
                }
            })
            .collect::<Vec<_>>()
            .into();

        keywords.push(Keyword::Properties { properties });
    }

    keywords.sort_by_key(|kw| -> u32 {
        match kw {
            Keyword::Id { .. } => 0, // Always first.

            // Properties / PatternProperties must appear before
            // AdditionalProperties or UnevaluatedProperties.
            Keyword::Properties { .. } => 10,
            Keyword::PatternProperties { .. } => 11,
            Keyword::AdditionalProperties { .. } => 12,
            Keyword::UnevaluatedProperties { .. } => 13,

            // PrefixItems conditions whether AdditionalItems is applied.
            // Contains is always applied, but evaluates before UnevaluatedItems.
            Keyword::PrefixItems { .. } => 20,
            Keyword::Items { .. } => 21,
            Keyword::Contains { .. } => 22,
            Keyword::UnevaluatedItems { .. } => 23,

            // When unwinding frames, we want to know which branch was taken before
            // we examine branch results.
            Keyword::Else { .. } => 30,
            Keyword::Then { .. } => 31,
            Keyword::If { .. } => 32,

            Keyword::Annotation { .. } => u32::MAX,

            _ => u32::MAX - 1,
        }
    });

    keywords
}

fn build_schema_array<'l, 's, A>(
    scope: Scope<'l>,
    value: &'s serde_json::Value,
    errors: &mut Errors<A>,
) -> Vec<schema::Schema<A>>
where
    A: schema::Annotation,
{
    expect_array(scope, value, errors)
        .iter()
        .enumerate()
        .map(|(i, value)| build::<A>(scope.push_item(i), value, errors))
        .collect::<Vec<_>>()
}

fn build_schema_map<'l, 's, A>(
    scope: Scope<'l>,
    value: &'s serde_json::Value,
    errors: &mut Errors<A>,
) -> Vec<(&'s str, schema::Schema<A>)>
where
    A: schema::Annotation,
{
    expect_map(scope, value, errors)
        .iter()
        .map(|(property, value)| {
            (
                property.as_str(),
                build::<A>(scope.push_prop(property), value, errors),
            )
        })
        .collect::<Vec<_>>()
}

fn build_frozen_schema_map<'l, 's, A>(
    scope: Scope<'l>,
    value: &'s serde_json::Value,
    errors: &mut Errors<A>,
) -> super::FrozenSlice<(super::FrozenString, schema::Schema<A>)>
where
    A: schema::Annotation,
{
    build_schema_map(scope, value, errors)
        .into_iter()
        .map(|(k, v)| (k.to_string().into(), v))
        .collect::<Vec<_>>()
        .into()
}

fn expect_unsigned<'l, A: schema::Annotation>(
    scope: Scope<'l>,
    v: &serde_json::Value,
    errors: &mut Errors<A>,
) -> usize {
    if let Some(v) = v.as_u64() {
        v as usize
    } else {
        Error::ExpectedUnsigned.push(scope, errors);
        0
    }
}

fn expect_str<'l, 's, A: schema::Annotation>(
    scope: Scope<'l>,
    v: &'s serde_json::Value,
    errors: &mut Errors<A>,
) -> &'s str {
    if let Some(v) = v.as_str() {
        v
    } else {
        Error::ExpectedString.push(scope, errors);
        ""
    }
}

fn expect_array<'l, 's, A: schema::Annotation>(
    scope: Scope<'l>,
    v: &'s serde_json::Value,
    errors: &mut Errors<A>,
) -> &'s [serde_json::Value] {
    if let Some(v) = v.as_array() {
        v
    } else {
        Error::ExpectedArray.push(scope, errors);
        &[]
    }
}

fn expect_map<'l, 's, A: schema::Annotation>(
    scope: Scope<'l>,
    v: &'s serde_json::Value,
    errors: &mut Errors<A>,
) -> &'s serde_json::Map<String, serde_json::Value> {
    if let Some(v) = v.as_object() {
        v
    } else {
        Error::ExpectedObject.push(scope, errors);
        &EMPTY_MAP
    }
}

fn expect_bool<'l, A: schema::Annotation>(
    scope: Scope<'l>,
    v: &serde_json::Value,
    errors: &mut Errors<A>,
) -> bool {
    if let Some(v) = v.as_bool() {
        v
    } else {
        Error::ExpectedBool.push(scope, errors);
        false
    }
}

fn expect_number<'l, A: schema::Annotation>(
    scope: Scope<'l>,
    v: &serde_json::Value,
    errors: &mut Errors<A>,
) -> crate::Number {
    if let Some(num) = crate::Number::from_node(v) {
        num
    } else {
        Error::ExpectedNumber.push(scope, errors);
        crate::Number::PosInt(0)
    }
}

fn expect_url<'l, A: schema::Annotation>(
    scope: Scope<'l>,
    v: &serde_json::Value,
    errors: &mut Errors<A>,
) -> url::Url {
    match v.as_str().map(|s| scope.resource().join(s)) {
        None => {
            Error::ExpectedURL.push(scope, errors);
        }
        Some(Err(err)) => {
            Error::URL(err).push(scope, errors);
        }
        Some(Ok(url)) if url.fragment().is_some() => {
            Error::UnexpectedURLFragment.push(scope, errors);
        }
        Some(Ok(url)) => {
            return url;
        }
    }
    url::Url::parse("https://placeholder.invalid").unwrap()
}

fn expect_relative_url<'l, A: schema::Annotation>(
    scope: Scope<'l>,
    anchor: bool,
    v: &serde_json::Value,
    errors: &mut Errors<A>,
) -> url::Url {
    match v.as_str().map(str::to_string).map(|s| {
        scope
            .resource()
            .join(&if anchor { format!("#{s}") } else { s })
    }) {
        None => {
            Error::ExpectedURL.push(scope, errors);
        }
        Some(Err(err)) => {
            Error::URL(err).push(scope, errors);
        }
        Some(Ok(mut url)) => {
            if url.fragment() == Some("") {
                url.set_fragment(None); // Normalize empty fragment to no fragment.
            }
            return url;
        }
    }
    url::Url::parse("https://placeholder.invalid").unwrap()
}

lazy_static::lazy_static! {
    static ref EMPTY_MAP: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
}

#[cfg(test)]
mod tests {
    use super::schema;

    #[test]
    fn it_works() {
        let schema: serde_json::Value = serde_json::from_str(include_str!(
            "../../../json/tests/official/test-schema.json"
        ))
        .unwrap();
        let curi = url::Url::parse("https://example.com/test-schema.json").unwrap();

        let schema = super::build_schema::<schema::CoreAnnotation>(&curi, &schema).unwrap();
        insta::assert_debug_snapshot!(schema);
    }
}
