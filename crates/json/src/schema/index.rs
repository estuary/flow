use crate::schema::{Annotation, Application, Keyword, Schema};
use std::collections::{BTreeMap, BTreeSet};
use thiserror;

#[derive(thiserror::Error, Debug, serde::Serialize)]
pub enum Error {
    #[error("duplicate canonical URI: '{0}'")]
    DuplicateCanonicalURI(url::Url),
    #[error("duplicate anchor URI: '{0}'")]
    DuplicateAnchorURI(url::Url),
    #[error("schema $ref '{ruri}', referenced by '{curi}', was not found")]
    InvalidReference { ruri: url::Url, curi: url::Url },
    #[error("schema '{uri}' was not found")]
    NotFound { uri: url::Url },
}

/// IndexBuilder builds an index of Schemas indexed on their
/// canconical and alternative anchor-form URIs.
/// Once built, an IndexBuilder is converted into a packed Index
/// for fast query lookups.
pub struct IndexBuilder<'s, A>(BTreeMap<&'s url::Url, &'s Schema<A>>)
where
    A: Annotation;

impl<'s, A> IndexBuilder<'s, A>
where
    A: Annotation,
{
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn add(&mut self, schema: &'s Schema<A>) -> Result<(), Error> {
        use Error::*;

        // Index this schema's canonical URI.
        if let Some(_) = self.0.insert(&schema.curi, schema) {
            return Err(DuplicateCanonicalURI(schema.curi.clone()));
        }

        for kw in &schema.kw {
            match kw {
                // Recurse to index a subordinate schema application.
                Keyword::Application(_, child) => self.add(child)?,
                // Index an alternative, anchor-form canonical URI.
                Keyword::Anchor(auri) => {
                    if let Some(_) = self.0.insert(auri, schema) {
                        return Err(DuplicateAnchorURI(schema.curi.clone()));
                    }
                }
                // No-ops.
                Keyword::RecursiveAnchor | Keyword::Validation(_) | Keyword::Annotation(_) => (),
            }
        }
        Ok(())
    }

    pub fn verify_references(&self) -> Result<(), Error> {
        for (referrer, referrent) in self.references() {
            if !self.0.contains_key(referrent) {
                return Err(Error::InvalidReference {
                    ruri: referrent.clone(),
                    curi: referrer.clone(),
                });
            }
        }
        Ok(())
    }

    pub fn into_index(self) -> Index<'s, A> {
        let referrents: BTreeSet<_> = self.references().map(|(_, r)| r).collect();

        let (fast, slow) = self.0.into_iter().partition(|e| referrents.contains(&e.0));
        Index { fast, slow }
    }

    fn references<'a>(&'a self) -> impl Iterator<Item = (&'s url::Url, &'s url::Url)> + 'a {
        self.0.iter().flat_map(|(referrer, schema)| {
            schema.kw.iter().filter_map(move |kw| match kw {
                Keyword::Application(Application::Ref(referrent), _) => {
                    Some((*referrer, referrent))
                }
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
    // Store is subdivided between a |fast| and |slow| index.
    // |fast| items are statically known to be referenced, and there are fewer of them.
    // |slow| items may still be referenced, and there are more of them.
    fast: Vec<(&'s url::Url, &'s Schema<A>)>,
    slow: Vec<(&'s url::Url, &'s Schema<A>)>,
}

impl<'s, A> Index<'s, A>
where
    A: Annotation,
{
    pub fn fetch(&self, uri: &url::Url) -> Option<&'s Schema<A>> {
        if let Ok(ind) = self.fast.binary_search_by_key(&uri, |(s, _)| s) {
            Some(self.fast[ind].1)
        } else if let Ok(ind) = self.slow.binary_search_by_key(&uri, |(s, _)| s) {
            Some(self.slow[ind].1)
        } else {
            None
        }
    }

    pub fn must_fetch(&self, uri: &url::Url) -> Result<&'s Schema<A>, Error> {
        match self.fetch(uri) {
            None => Err(Error::NotFound { uri: uri.clone() }),
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
                    "const": 3
                },
                "other": { "$ref": "http://other" },
            },
            "$ref": "#Two",
        });

        let curi = url::Url::parse("http://example/schema").unwrap();
        let schema = build_schema::<CoreAnnotation>(curi.clone(), &schema).unwrap();

        let mut builder = IndexBuilder::new();
        builder.add(&schema).unwrap();
        builder.verify_references().unwrap();
        let index = builder.into_index();

        assert_eq!(
            index
                .fast
                .iter()
                .map(|(u, _)| u.as_str())
                .collect::<Vec<_>>(),
            vec!["http://example/schema#Two", "http://other/"]
        );
        assert_eq!(
            index
                .slow
                .iter()
                .map(|(u, _)| u.as_str())
                .collect::<Vec<_>>(),
            vec![
                "http://example/schema",
                "http://example/schema#/$defs/one",
                "http://example/schema#/$defs/other",
                "http://example/schema#/$defs/other/$ref",
                "http://example/schema#/$defs/two",
                "http://example/schema#/$ref",
                "http://other/#Three"
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
            assert_eq!(
                index.fetch(&url::Url::parse(uri).unwrap()).is_some(),
                *is_some,
            );
        }
    }
}
