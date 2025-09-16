use crate::schema::{Annotation, Keyword, Schema};
use std::collections::{BTreeMap, BTreeSet};
use thiserror;

#[derive(thiserror::Error, Debug, serde::Serialize)]
pub enum Error {
    #[error("duplicate canonical URI: '{0}'")]
    DuplicateCanonicalURI(String),
    #[error("duplicate anchor URI: '{0}'")]
    DuplicateAnchorURI(String),
    #[error("schema $ref '{ruri}', referenced by '{curi}', was not found")]
    InvalidReference { ruri: String, curi: String },
    #[error("schema '{uri}' was not found")]
    NotFound { uri: String },
}

/// IndexBuilder builds an index of Schemas indexed on their
/// canonical and alternative anchor-form URIs.
/// Once built, an IndexBuilder is converted into a packed Index
/// for fast query lookups.
pub struct IndexBuilder<'s, A>(BTreeMap<&'s str, &'s Schema<A>>)
where
    A: Annotation;

impl<'s, A> IndexBuilder<'s, A>
where
    A: Annotation,
{
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    // Index a schema and all sub-schemas by their canonical URIs.
    pub fn add(&mut self, schema: &'s Schema<A>) -> Result<(), Error> {
        // Extract the canonical URI from the Id keyword, which must be first.
        let curi = match schema.kw.first() {
            Some(Keyword::Id { curi, .. }) => curi.as_ref(),
            _ => panic!("schema must have an Id keyword as its first keyword"),
        };

        // Index this schema's canonical URI.
        if let Some(_) = self.0.insert(curi, schema) {
            return Err(Error::DuplicateCanonicalURI(curi.to_string()));
        }

        // Process all keywords for sub-schemas and anchors.
        for kw in schema.kw.iter() {
            match kw {
                // Index an alternative, anchor-form canonical URI.
                Keyword::Anchor { anchor } => {
                    if let Some(_) = self.0.insert(anchor.as_ref(), schema) {
                        return Err(Error::DuplicateAnchorURI(anchor.to_string()));
                    }
                }
                // Recurse to index subordinate schemas.
                Keyword::AdditionalProperties {
                    additional_properties,
                } => self.add(additional_properties)?,
                Keyword::AllOf { all_of } => {
                    for child in all_of.iter() {
                        self.add(child)?;
                    }
                }
                Keyword::AnyOf { any_of } => {
                    for child in any_of.iter() {
                        self.add(child)?;
                    }
                }
                Keyword::Contains { contains } => self.add(contains)?,
                Keyword::Defs { defs } => {
                    for (_, child) in defs.iter() {
                        self.add(child)?;
                    }
                }
                Keyword::Definitions { definitions } => {
                    for (_, child) in definitions.iter() {
                        self.add(child)?;
                    }
                }
                Keyword::DependentSchemas { dependent_schemas } => {
                    for (_, child) in dependent_schemas.iter() {
                        self.add(child)?;
                    }
                }
                Keyword::Else { r#else } => self.add(r#else)?,
                Keyword::If { r#if } => self.add(r#if)?,
                Keyword::Items { items } => self.add(items)?,
                Keyword::Not { not } => self.add(not)?,
                Keyword::OneOf { one_of } => {
                    for child in one_of.iter() {
                        self.add(child)?;
                    }
                }
                Keyword::PatternProperties { pattern_properties } => {
                    for (_, child) in pattern_properties.iter() {
                        self.add(child)?;
                    }
                }
                Keyword::PrefixItems { prefix_items } => {
                    for child in prefix_items.iter() {
                        self.add(child)?;
                    }
                }
                Keyword::Properties { properties } => {
                    for (_, child) in properties.iter() {
                        self.add(child)?;
                    }
                }
                Keyword::PropertyNames { property_names } => self.add(property_names)?,
                Keyword::Then { then } => self.add(then)?,
                Keyword::UnevaluatedItems { unevaluated_items } => self.add(unevaluated_items)?,
                Keyword::UnevaluatedProperties {
                    unevaluated_properties,
                } => self.add(unevaluated_properties)?,
                // Referenced schemas are just links, not new schemas to index.
                Keyword::Ref { .. } => {}
                // All other keywords don't contain sub-schemas.
                _ => {}
            }
        }
        Ok(())
    }

    pub fn verify_references(&self) -> Result<(), Error> {
        for (referrer, referrent) in self.references() {
            if !self.0.contains_key(referrent) {
                return Err(Error::InvalidReference {
                    ruri: referrent.to_string(),
                    curi: referrer.to_string(),
                });
            }
        }
        Ok(())
    }

    pub fn into_index(self) -> Index<'s, A> {
        let referents: BTreeSet<_> = self.references().map(|(_, r)| r).collect();

        let (fast, slow): (Vec<_>, Vec<_>) = self
            .0
            .into_iter()
            .partition(|(uri, _)| referents.contains(uri));

        // Re-allocate the `fast` partition so that its keys are likely to
        // be naturally ordered and packed in memory (cache lines).
        let fast = fast
            .into_iter()
            .map(|(u, s)| (u.to_string().into(), s))
            .collect();

        Index { fast, slow }
    }

    fn references(&self) -> impl Iterator<Item = (&'s str, &'s str)> + '_ {
        self.0.iter().flat_map(|(referrer, schema)| {
            schema.kw.iter().filter_map(move |kw| match kw {
                Keyword::Ref { r#ref } => Some((*referrer, r#ref.as_ref())),
                Keyword::DynamicRef { dynamic_ref } => Some((*referrer, dynamic_ref.as_ref())),
                _ => None,
            })
        })
    }
}

/// Index is a packed, sorted index over Schema references.
/// It provides lookups over Schema canonical and anchor-form URIs.
pub struct Index<'s, A>
where
    A: Annotation,
{
    // Store is subdivided between a `fast` and `slow` index.
    // `fast` items are statically known to be referenced, and there are fewer of them.
    // `slow` items may still be referenced, and there are more of them.
    fast: Vec<(super::FrozenString, &'s Schema<A>)>,
    slow: Vec<(&'s str, &'s Schema<A>)>,
}

impl<'s, A> Index<'s, A>
where
    A: Annotation,
{
    pub fn fetch(&self, uri: &str) -> Option<&'s Schema<A>> {
        if let Ok(ind) = self.fast.binary_search_by_key(&uri, |(u, _)| u) {
            Some(self.fast[ind].1)
        } else if let Ok(ind) = self.slow.binary_search_by_key(&uri, |(u, _)| u) {
            Some(self.slow[ind].1)
        } else {
            None
        }
    }

    pub fn must_fetch(&self, uri: &str) -> Result<&'s Schema<A>, Error> {
        match self.fetch(uri) {
            None => Err(Error::NotFound {
                uri: uri.to_string(),
            }),
            Some(scm) => Ok(scm),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{super::build::build_schema, super::CoreAnnotation, IndexBuilder};
    use serde_json::json;

    #[test]
    fn test_indexing() {
        let schema = json!({
            "$defs": {
                "one": {
                    "const": 1,
                },
                "two": {
                    "$anchor": "Two",
                    "const": 2,
                },
                "three": {
                    "$id": "http://other",
                    "$anchor": "Three",
                    "properties": {
                        "hello": {
                            "$id": "http://yet-another",
                            "const": 3,
                        },
                    },
                },
                "other": { "$ref": "http://other" },
            },
            "$ref": "#Two",
        });

        let curi = url::Url::parse("http://example/schema").unwrap();
        let schema = build_schema::<CoreAnnotation>(&curi, &schema).unwrap();

        let mut builder = IndexBuilder::new();
        builder.add(&schema).unwrap();
        builder.verify_references().unwrap();
        let index = builder.into_index();

        assert_eq!(
            index.fast.iter().map(|(u, _)| &(**u)).collect::<Vec<_>>(),
            vec!["http://example/schema#Two", "http://other/"]
        );
        assert_eq!(
            index.slow.iter().map(|(u, _)| *u).collect::<Vec<_>>(),
            vec![
                "http://example/schema",
                "http://example/schema#/$defs/one",
                "http://example/schema#/$defs/other",
                "http://example/schema#/$defs/two",
                "http://other/#Three",
                "http://yet-another/"
            ]
        );

        for (uri, is_some) in &[
            // Fast path.
            ("http://other/", true),
            ("http://example/schema#Two", true),
            // Slow path.
            ("http://example/schema", true),
            ("http://example/schema#/$defs/other", true),
            ("http://other/#Three", true),
            // Misses.
            ("http://missing/#Four", false),
            ("http://other/#Five", false),
        ] {
            assert_eq!(index.fetch(uri).is_some(), *is_some, "failed for {}", uri);
        }
    }
}
