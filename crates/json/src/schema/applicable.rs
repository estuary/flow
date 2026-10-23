//! Conservative resolution of the subschemas which may apply at a document location.
//! Branch success is never established: every branch of a combinator or conditional
//! is a candidate, as if any of them could be the one which applies.

use super::{Annotation, Index, Keyword, Schema};

/// Expand `candidates` through in-place applicators into the distinct subschemas which
/// may apply at their document location. References of `index` must be verified.
pub fn expand<'s, A: Annotation>(
    mut candidates: Vec<&'s Schema<A>>,
    index: &Index<'s, A>,
) -> Vec<&'s Schema<A>> {
    // Deduplicate within this document location, not across locations: recursive
    // schemas must be revisited as we descend through a finite document.
    let mut seen = std::collections::HashSet::new();
    let mut expanded = Vec::new();

    while let Some(schema) = candidates.pop() {
        if !seen.insert(schema as *const Schema<A>) {
            continue;
        }
        expanded.push(schema);

        for kw in schema.keywords.iter() {
            match kw {
                Keyword::Ref { r#ref } => {
                    candidates.push(index.fetch(r#ref).expect("verified reference").0);
                }
                Keyword::DynamicRef { dynamic_ref } => {
                    let (target, dynamic) = index.fetch(dynamic_ref).expect("verified reference");
                    candidates.push(target);
                    // Any matching dynamic anchor is an over-approximation of
                    // the possible dynamic scopes, including recursive ones.
                    // A $dynamicRef without a fragment behaves as a plain $ref.
                    if let (true, Some((_, fragment))) = (dynamic, dynamic_ref.rsplit_once('#')) {
                        candidates.extend(index.iter().filter_map(|(uri, dynamic, schema)| {
                            (dynamic && uri.rsplit_once('#').map(|(_, f)| f) == Some(fragment))
                                .then_some(schema)
                        }));
                    }
                }
                Keyword::AllOf { all_of: schemas }
                | Keyword::AnyOf { any_of: schemas }
                | Keyword::OneOf { one_of: schemas } => candidates.extend(schemas.iter()),
                Keyword::If { r#if: schema }
                | Keyword::Then { then: schema }
                | Keyword::Else { r#else: schema } => candidates.push(schema),
                Keyword::DependentSchemas { dependent_schemas } => {
                    candidates.extend(dependent_schemas.iter().map(|(_, schema)| schema));
                }
                // `not` discards annotations; propertyNames describes keys, not
                // values. Definitions apply only when referenced.
                _ => {}
            }
        }
    }
    expanded
}

/// The subschemas which may apply to property `name` of an object, given the
/// `expanded` subschemas which may apply to the object itself.
pub fn property<'s, A: Annotation>(expanded: &[&'s Schema<A>], name: &str) -> Vec<&'s Schema<A>> {
    let mut schemas = Vec::new();
    for schema in expanded {
        let mut matched = false;
        let mut additional = None;
        for kw in schema.keywords.iter() {
            match kw {
                Keyword::Properties { properties } => {
                    for (property, schema) in properties.iter() {
                        // '+' entries represent required-only properties,
                        // which do not exclude additionalProperties.
                        if property.as_bytes()[0] != b'+' && &property[1..] == name {
                            matched = true;
                            schemas.push(schema);
                        }
                    }
                }
                Keyword::PatternProperties { pattern_properties } => {
                    for (pattern, schema) in pattern_properties.iter() {
                        if pattern.is_match(name) {
                            matched = true;
                            schemas.push(schema);
                        }
                    }
                }
                Keyword::AdditionalProperties {
                    additional_properties,
                } => additional = Some(&**additional_properties),
                // Evaluation depends on branch success, which we do
                // not establish. Include these for every child.
                Keyword::UnevaluatedProperties {
                    unevaluated_properties,
                } => schemas.push(unevaluated_properties),
                _ => {}
            }
        }
        if !matched {
            schemas.extend(additional);
        }
    }
    schemas
}

/// The subschemas which may apply to item `position` of an array, given the
/// `expanded` subschemas which may apply to the array itself.
pub fn item<'s, A: Annotation>(expanded: &[&'s Schema<A>], position: usize) -> Vec<&'s Schema<A>> {
    let mut schemas = Vec::new();
    for schema in expanded {
        let mut prefix = false;
        let mut additional = None;
        for kw in schema.keywords.iter() {
            match kw {
                Keyword::PrefixItems { prefix_items } => {
                    if let Some(schema) = prefix_items.get(position) {
                        prefix = true;
                        schemas.push(schema);
                    }
                }
                Keyword::Items { items } => additional = Some(&**items),
                Keyword::Contains { contains: schema }
                | Keyword::UnevaluatedItems {
                    unevaluated_items: schema,
                } => schemas.push(schema),
                _ => {}
            }
        }
        if !prefix {
            schemas.extend(additional);
        }
    }
    schemas
}
