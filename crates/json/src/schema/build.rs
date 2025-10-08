use crate::schema::{self, Keyword, keywords, types};
use crate::scope::Scope;
use itertools::Itertools;
use serde::Deserialize;

#[derive(thiserror::Error, Debug)]
pub enum Error<A: schema::Annotation> {
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
    #[error("expected a schema object or boolean")]
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
#[error("at {scope}")]
pub struct ScopedError<A: schema::Annotation> {
    pub scope: url::Url,
    #[source]
    pub inner: Error<A>,
}

#[derive(thiserror::Error, Debug)]
#[error("{}", .0.iter().map(|ScopedError{scope, inner}| format!("at {scope}: {inner}")).join("\n"))]
pub struct Errors<A: schema::Annotation>(pub Vec<ScopedError<A>>);

/// Build a JSON Schema having canonical URI `curi` from the given serde_json::Value.
/// If any errors are encountered, they are collected and returned.
pub fn build_schema<'l, 's, A>(
    curi: &'l url::Url,
    value: &'s serde_json::Value,
) -> Result<schema::Schema<A>, Errors<A>>
where
    A: schema::Annotation,
{
    let scope = Scope::new(curi);
    let mut errors = Vec::new();
    let schema = build(scope, value, &mut errors);

    if errors.is_empty() {
        Ok(schema)
    } else {
        Err(Errors(errors))
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
    errors: &mut Vec<ScopedError<A>>,
) -> schema::Schema<A>
where
    A: schema::Annotation,
{
    let keywords = match value {
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
    schema::Schema {
        keywords: keywords.into(),
    }
}

fn build_object_keywords<'l, 's, A>(
    scope: Scope<'l>,
    map: &'s serde_json::Map<String, serde_json::Value>,
    errors: &mut Vec<ScopedError<A>>,
) -> Vec<Keyword<A>>
where
    A: schema::Annotation,
{
    let maybe_id: Option<url::Url>;
    let mut keywords: Vec<Keyword<A>> = Vec::new();

    // We always include the implicit URI which lead to this schema.
    // This may be over-ridden by an explicit canonical URI ($id),
    // which is used to scope all keywords under this schema object.
    let implicit_curi = scope.flatten();
    keywords.push(Keyword::Id {
        curi: implicit_curi.to_string().into(),
        explicit: false,
    });

    // $id, if present, changes the Scope's base resource.
    let scope = if let Some(id) = map.get(keywords::ID) {
        maybe_id = Some(expect_url(
            scope.push_prop(keywords::ID),
            false,
            true,
            id,
            errors,
        ));
        let id = maybe_id.as_ref().unwrap();

        if id == &implicit_curi {
            keywords.pop(); // Remove identical implicit Keyword::Id.
        }
        keywords.push(Keyword::Id {
            curi: id.to_string().into(),
            explicit: true,
        });
        scope.push_resource(id)
    } else {
        scope
    };

    let mut properties = None;
    let mut required = None;
    let mut r#type = types::INVALID;

    for (keyword, value) in map {
        let scope = scope.push_prop(keyword);
        let mut unknown = false;

        match keyword.as_str() {
            keywords::ADDITIONAL_ITEMS => {
                // Ignore "additionalItems" if "items" is present and not an array.
                // Otherwise interpret it as draft2020-12 "items".
                //
                // This is a deliberate departure from the draft-2019-09 spec,
                // which says "additionalItems" is ignored if "items" isn't also present.
                // However it matches the historical behavior of our validator.
                if !matches!(map.get(keywords::ITEMS), Some(items) if !items.is_array()) {
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
                let anchor = expect_url(scope, true, false, value, errors)
                    .to_string()
                    .into();
                keywords.push(Keyword::Anchor { anchor });
            }
            keywords::ANY_OF => {
                let any_of = build_schema_array(scope, value, errors).into();
                keywords.push(Keyword::AnyOf { any_of });
            }
            keywords::CONST => {
                let r#const = Box::new(value.clone());
                keywords.push(Keyword::Const { r#const });
            }
            keywords::CONTAINS => {
                let contains = Box::new(build::<A>(scope, value, errors));
                keywords.push(Keyword::Contains { contains });

                // The presence of "contains" implies "minContains: 1".
                if !map.contains_key(keywords::MIN_CONTAINS) {
                    keywords.push(Keyword::MinContains { min_contains: 1 });
                }
            }
            keywords::DEFS => {
                let defs = build_packed_schema_map(scope, value, errors);
                keywords.push(Keyword::Defs { defs });
            }
            keywords::DEFINITIONS => {
                let definitions = build_packed_schema_map(scope, value, errors);
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
                let dependent_schemas = build_packed_schema_map(scope, value, errors);
                keywords.push(Keyword::DependentSchemas { dependent_schemas });
            }
            keywords::DYNAMIC_ANCHOR => {
                let dynamic_anchor = expect_url(scope, true, false, value, errors)
                    .to_string()
                    .into();
                keywords.push(Keyword::DynamicAnchor { dynamic_anchor });
            }
            keywords::DYNAMIC_REF => {
                // A relative $dynamicRef is projected into its canonical and
                // absolute URL in its current resource. When indexing, we expect
                // there's a corresponding $dynamicAnchor of the same resource
                // (the "bookending requirement"), but at evaluation time we'll
                // look for earlier base resources of the dynamic scope that also
                // define the same $dynamicAnchor.
                let dynamic_ref = expect_url(scope, false, false, value, errors)
                    .to_string()
                    .into();
                keywords.push(Keyword::DynamicRef { dynamic_ref });
            }
            keywords::ELSE => {
                let r#else = Box::new(build::<A>(scope, value, errors));
                keywords.push(Keyword::Else { r#else });
            }
            keywords::ENUM => {
                let mut r#enum = expect_array(scope, value, errors).to_vec();
                r#enum.sort_by(crate::node::compare);
                keywords.push(Keyword::Enum {
                    r#enum: r#enum.into(),
                });
            }
            keywords::EXCLUSIVE_MAXIMUM => {
                expect_number(scope, value, errors, &mut keywords, |n| match n {
                    NumberType::PosInt(v) => Keyword::ExclusiveMaximumPosInt {
                        exclusive_maximum: v,
                    },
                    NumberType::NegInt(v) => Keyword::ExclusiveMaximumNegInt {
                        exclusive_maximum: v,
                    },
                    NumberType::Float(v) => Keyword::ExclusiveMaximumFloat {
                        exclusive_maximum: v,
                    },
                });
            }
            keywords::EXCLUSIVE_MINIMUM => {
                expect_number(scope, value, errors, &mut keywords, |n| match n {
                    NumberType::PosInt(v) => Keyword::ExclusiveMinimumPosInt {
                        exclusive_minimum: v,
                    },
                    NumberType::NegInt(v) => Keyword::ExclusiveMinimumNegInt {
                        exclusive_minimum: v,
                    },
                    NumberType::Float(v) => Keyword::ExclusiveMinimumFloat {
                        exclusive_minimum: v,
                    },
                });
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
                expect_number(scope, value, errors, &mut keywords, |n| match n {
                    NumberType::PosInt(v) => Keyword::MaximumPosInt { maximum: v },
                    NumberType::NegInt(v) => Keyword::MaximumNegInt { maximum: v },
                    NumberType::Float(v) => Keyword::MaximumFloat { maximum: v },
                });
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
                expect_number(scope, value, errors, &mut keywords, |n| match n {
                    NumberType::PosInt(v) => Keyword::MinimumPosInt { minimum: v },
                    NumberType::NegInt(v) => Keyword::MinimumNegInt { minimum: v },
                    NumberType::Float(v) => Keyword::MinimumFloat { minimum: v },
                });
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
                expect_number(scope, value, errors, &mut keywords, |n| match n {
                    NumberType::PosInt(v) => Keyword::MultipleOfPosInt { multiple_of: v },
                    NumberType::NegInt(v) => Keyword::MultipleOfNegInt { multiple_of: v },
                    NumberType::Float(v) => Keyword::MultipleOfFloat { multiple_of: v },
                });
            }
            keywords::NOT => {
                let not = Box::new(build::<A>(scope, value, errors));
                keywords.push(Keyword::Not { not });
            }
            keywords::NULLABLE => {
                // Support OpenAPI versions prior to 3.1, by merging `nullable` with `type`.
                if expect_bool(scope, value, errors) {
                    r#type = types::NULL;
                }
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
            keywords::RECURSIVE_ANCHOR => {} // Ignored.
            keywords::RECURSIVE_REF => {}    // Ignored.
            keywords::REF => {
                // A relative $ref is projected into its canonical and absolute URL.
                let r#ref = expect_url(scope, false, false, value, errors)
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
                let then = Box::new(build::<A>(scope, value, errors));
                keywords.push(Keyword::Then { then });
            }
            keywords::TYPE => {
                // As a support crutch for OpenAPI versions prior to 3.1,
                // merge a "nullable" keyword into the "type" keyword.
                r#type = r#type
                    | match types::Set::deserialize(value) {
                        Ok(r#type) => r#type,
                        Err(err) => {
                            Error::Json(err).push(scope, errors);
                            types::INVALID
                        }
                    };
                keywords.push(Keyword::Type { r#type });
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
            keywords::URL => {}        // No-op.
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
        // allocating them into PackedStr and using concat() to ensure exact
        // capacities are requested. This maximizes the likelihood of strings
        // being packed and ordered within common cache lines.

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
                        keywords: vec![id].into(),
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
            Keyword::Id { explicit: true, .. } => 0,
            Keyword::Id {
                explicit: false, ..
            } => 1,

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

            _ => u32::MAX,
        }
    });

    keywords
}

fn build_schema_array<'l, 's, A>(
    scope: Scope<'l>,
    value: &'s serde_json::Value,
    errors: &mut Vec<ScopedError<A>>,
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
    errors: &mut Vec<ScopedError<A>>,
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

fn build_packed_schema_map<'l, 's, A>(
    scope: Scope<'l>,
    value: &'s serde_json::Value,
    errors: &mut Vec<ScopedError<A>>,
) -> super::PackedSlice<(super::PackedStr, schema::Schema<A>)>
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
    errors: &mut Vec<ScopedError<A>>,
) -> usize {
    if let Some(v) = v.as_u64() {
        return v as usize;
    } else if let Some(v) = v.as_f64() {
        if v >= 0.0 && v.fract() == 0.0 {
            return v as usize;
        }
    }

    Error::ExpectedUnsigned.push(scope, errors);
    0
}

fn expect_str<'l, 's, A: schema::Annotation>(
    scope: Scope<'l>,
    v: &'s serde_json::Value,
    errors: &mut Vec<ScopedError<A>>,
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
    errors: &mut Vec<ScopedError<A>>,
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
    errors: &mut Vec<ScopedError<A>>,
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
    errors: &mut Vec<ScopedError<A>>,
) -> bool {
    if let Some(v) = v.as_bool() {
        v
    } else {
        Error::ExpectedBool.push(scope, errors);
        false
    }
}

enum NumberType {
    PosInt(u64),
    NegInt(i64),
    Float(f64),
}

fn expect_number<'l, A, F>(
    scope: Scope<'l>,
    v: &serde_json::Value,
    errors: &mut Vec<ScopedError<A>>,
    keywords: &mut Vec<Keyword<A>>,
    build_keyword: F,
) where
    A: schema::Annotation,
    F: FnOnce(NumberType) -> Keyword<A>,
{
    let number_type = match v {
        serde_json::Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                Some(NumberType::PosInt(u))
            } else if let Some(i) = n.as_i64() {
                Some(NumberType::NegInt(i))
            } else if let Some(f) = n.as_f64() {
                Some(NumberType::Float(f))
            } else {
                None
            }
        }
        _ => None,
    };

    if let Some(num_type) = number_type {
        keywords.push(build_keyword(num_type));
    } else {
        Error::ExpectedNumber.push(scope, errors);
    }
}

fn expect_url<'l, A: schema::Annotation>(
    scope: Scope<'l>,
    is_anchor: bool,
    is_id: bool,
    v: &serde_json::Value,
    errors: &mut Vec<ScopedError<A>>,
) -> url::Url {
    match v.as_str().map(str::to_string).map(|s| {
        scope
            .resource()
            .join(&if is_anchor { format!("#{s}") } else { s })
    }) {
        None => {
            Error::ExpectedURL.push(scope, errors);
        }
        Some(Err(err)) => {
            Error::URL(err).push(scope, errors);
        }
        Some(Ok(url)) if is_id && url.fragment().is_some() => {
            Error::UnexpectedURLFragment.push(scope, errors);
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
    fn build_official_schema() {
        let schema: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/official/test-schema.json")).unwrap();
        let curi = url::Url::parse("https://example.com/schema.json").unwrap();

        let schema = super::build_schema::<schema::CoreAnnotation>(&curi, &schema).unwrap();
        insta::assert_debug_snapshot!(schema);
    }
}
