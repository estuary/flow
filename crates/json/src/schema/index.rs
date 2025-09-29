use super::{Annotation, Keyword, PackedStr, Schema};
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
}

/// Builder builds an index of Schemas indexed on their
/// canonical and alternative anchor-form URIs.
/// Once populated, it's converted into a packed Index
/// for fast query lookups.
pub struct Builder<'s, A>
where
    A: Annotation,
{
    // Index of (URI) => (Schema, $dynamicAnchor?)
    idx: BTreeMap<&'s str, (&'s Schema<A>, bool)>,
}

impl<'s, A> Builder<'s, A>
where
    A: Annotation,
{
    pub fn new() -> Self {
        Self {
            idx: BTreeMap::new(),
        }
    }

    // Index a schema and all sub-schemas by their canonical URIs.
    pub fn add(&mut self, schema: &'s Schema<A>) -> Result<(), Error> {
        // Process all keywords for sub-schemas and anchors.
        for kw in schema.keywords.iter() {
            match kw {
                Keyword::Id { curi, explicit: _ } => {
                    if let Some(_) = self.idx.insert(curi, (schema, false)) {
                        return Err(Error::DuplicateCanonicalURI(curi.to_string()));
                    }
                }
                Keyword::Anchor { anchor } => {
                    if let Some(_) = self.idx.insert(anchor, (schema, false)) {
                        return Err(Error::DuplicateAnchorURI(anchor.to_string()));
                    }
                }
                Keyword::DynamicAnchor { dynamic_anchor } => {
                    if let Some(_) = self.idx.insert(dynamic_anchor, (schema, true)) {
                        return Err(Error::DuplicateAnchorURI(dynamic_anchor.to_string()));
                    }
                }

                // Referenced schemas are just links, not new schemas to index.
                Keyword::Ref { .. } | Keyword::DynamicRef { .. } => {}

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

                // All other keywords don't contain sub-schemas.
                _ => {}
            }
        }
        Ok(())
    }

    pub fn verify_references(&self) -> Result<(), Error> {
        for (referrer, referent) in self.references() {
            if !self.idx.contains_key(referent) {
                return Err(Error::InvalidReference {
                    ruri: referent.to_string(),
                    curi: referrer.to_string(),
                });
            }
        }
        Ok(())
    }

    pub fn into_index(self) -> Index<'s, A> {
        let referents: BTreeSet<_> = self.references().map(|(_, r)| r).collect();

        // Walk the builder index, pruning entries which are unreferenced and
        // aren't dynamic. Re-allocate URIs into PackedStr, making them likely
        // to be co-located in index order within the heap.
        let pruned: Vec<(PackedStr, bool, &'s Schema<A>)> = self
            .idx
            .into_iter()
            .filter_map(|(uri, (schema, dynamic))| {
                if dynamic || referents.contains(uri) {
                    Some((uri.to_string().into(), dynamic, schema))
                } else {
                    None
                }
            })
            .collect();

        Index { idx: pruned.into() }
    }

    fn references(&self) -> impl Iterator<Item = (&'s str, &'s str)> + '_ {
        self.idx.iter().flat_map(|(referrer, (schema, _dynamic))| {
            schema.keywords.iter().filter_map(move |kw| match kw {
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
    idx: Box<
        [(
            super::PackedStr, // URI.
            bool,             // Is $dynamicAnchor?
            &'s Schema<A>,    // Schema.
        )],
    >,
}

impl<'s, A> Index<'s, A>
where
    A: Annotation,
{
    // Attempt to resolve the given URI to an indexed Schema.
    pub fn fetch(&self, uri: &str) -> Option<(&'s Schema<A>, bool)> {
        if let Ok(ind) = self.idx.binary_search_by_key(&uri, |(key, _, _)| key) {
            Some((self.idx[ind].2, self.idx[ind].1))
        } else {
            None
        }
    }
    // Access the contents of the index as ordered (URI, is_dynamic, Schema) tuples.
    pub fn iter(&self) -> impl Iterator<Item = (&str, bool, &'s Schema<A>)> {
        self.idx.iter().map(|(u, d, s)| (u.as_ref(), *d, *s))
    }
}

#[cfg(test)]
mod test {
    use super::{super::build::build_schema, super::CoreAnnotation, Builder, Error};
    use serde_json::json;

    #[test]
    fn test_indexing() {
        let schema = json!({
            "$id": "http://example/root",
            "$dynamicAnchor": "meta",
            "title": "root schema",

            "$defs": {
                // Test basic $anchor
                "simple": {
                    "$anchor": "SimpleAnchor",
                    "type": "string"
                },

                // Test nested $id with new base URI
                "nested": {
                    "$id": "http://example/nested",
                    "title": "nested schema with new id",
                    "$defs": {
                        "inner": {
                            "$anchor": "InnerAnchor",
                            "type": "number"
                        },
                        "relative": {
                            "$id": "relative/path",
                            "type": "boolean"
                        }
                    },
                    "$ref": "#InnerAnchor"
                },

                // Test $dynamicAnchor for recursive structures
                "recursive": {
                    "$id": "http://example/recursive",
                    "$dynamicAnchor": "meta",
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                        "children": {
                            "type": "array",
                            "items": {
                                "$dynamicRef": "#meta"
                            }
                        }
                    }
                },

                // Test reference combinations
                "refs": {
                    "$id": "refs",
                    "$anchor": "RefsAnchor",
                    "allOf": [
                        { "$ref": "http://example/root#SimpleAnchor" },
                        { "$ref": "http://example/nested#InnerAnchor" },
                        { "$ref": "http://example/relative/path" },
                        { "$ref": "#RefsAnchor" },  // Self-reference via anchor
                        { "$ref": "http://example/root#/$defs/simple" }
                    ]
                }
            },

            // Test absolute and relative refs at root
            "properties": {
                "absolute": { "$ref": "http://example/nested" },
                "anchor": { "$ref": "#SimpleAnchor" },
                "pointer": { "$ref": "#/$defs/simple" },
                "dynamic": { "$dynamicRef": "#meta" }
            }
        });

        let curi = url::Url::parse("http://example/root").unwrap();
        let schema = build_schema::<CoreAnnotation>(&curi, &schema).unwrap();

        let mut builder = Builder::new();
        builder.add(&schema).unwrap();
        builder.verify_references().unwrap();
        let index = builder.into_index();

        // Verify all expected URIs are indexed
        let indexed_uris: Vec<(&str, bool)> =
            index.idx.iter().map(|(u, d, _)| (&(**u), *d)).collect();

        // Check that all expected URIs are present
        let expected_uris = vec![
            ("http://example/root#SimpleAnchor", false),
            ("http://example/root#/$defs/simple", false),
            ("http://example/root#meta", true), // $dynamicAnchor
            ("http://example/nested", false),
            ("http://example/nested#InnerAnchor", false),
            ("http://example/relative/path", false), // relative to root, not nested
            ("http://example/recursive#meta", true), // $dynamicAnchor
            ("http://example/refs#RefsAnchor", false), // anchor in refs
        ];

        for (uri, is_dynamic) in &expected_uris {
            assert!(
                indexed_uris
                    .iter()
                    .any(|(u, d)| u == uri && *d == *is_dynamic),
                "Expected URI '{uri}' with dynamic={is_dynamic} not found in index. Indexed URIs: {:?}",
                indexed_uris
            );
        }

        // Test fetch operations
        for (uri, expect_found, expect_dynamic) in &[
            // Successful lookups
            ("http://example/root#SimpleAnchor", true, false),
            ("http://example/root#/$defs/simple", true, false),
            ("http://example/root#meta", true, true),
            ("http://example/nested", true, false),
            ("http://example/nested#InnerAnchor", true, false),
            ("http://example/relative/path", true, false),
            ("http://example/recursive#meta", true, true),
            ("http://example/refs#RefsAnchor", true, false),
            // Failed lookups
            ("http://example/missing", false, false),
            ("http://example/root#MissingAnchor", false, false),
            ("http://other/schema", false, false),
        ] {
            let result = index.fetch(uri);
            if *expect_found {
                let (_schema, dynamic) = result.unwrap();
                assert_eq!(dynamic, *expect_dynamic);
            } else {
                assert!(result.is_none());
            }
        }
    }

    #[test]
    fn test_indexing_errors() {
        // Helper to build and test a schema for errors
        fn test_schema_error(
            schema_json: serde_json::Value,
            base_uri: &str,
            test_references: bool,
            expected_error: impl FnOnce(Result<(), Error>) -> bool,
        ) {
            let curi = url::Url::parse(base_uri).unwrap();
            let schema = build_schema::<CoreAnnotation>(&curi, &schema_json).unwrap();

            let mut builder = Builder::new();
            let add_result = builder.add(&schema);

            if test_references && add_result.is_ok() {
                let verify_result = builder.verify_references();
                assert!(expected_error(verify_result));
            } else {
                assert!(expected_error(add_result));
            }
        }

        // Test duplicate canonical URI error
        test_schema_error(
            json!({
                "$id": "http://example/dup",
                "$defs": {
                    "bad": {
                        "$id": "http://example/dup",  // Duplicate!
                        "type": "string"
                    }
                }
            }),
            "http://example/dup",
            false,
            |result| matches!(result, Err(Error::DuplicateCanonicalURI(uri)) if uri == "http://example/dup"),
        );

        // Test duplicate anchor URI error
        test_schema_error(
            json!({
                "$id": "http://example/anchor",
                "$anchor": "duplicate",
                "$defs": {
                    "bad": {
                        "$anchor": "duplicate",  // Duplicate!
                        "type": "string"
                    }
                }
            }),
            "http://example/anchor",
            false,
            |result| matches!(result, Err(Error::DuplicateAnchorURI(uri)) if uri == "http://example/anchor#duplicate"),
        );

        // Test duplicate $dynamicAnchor error
        test_schema_error(
            json!({
                "$id": "http://example/dynamic",
                "$dynamicAnchor": "meta",
                "$defs": {
                    "bad": {
                        "$dynamicAnchor": "meta",  // Duplicate!
                        "type": "string"
                    }
                }
            }),
            "http://example/dynamic",
            false,
            |result| matches!(result, Err(Error::DuplicateAnchorURI(uri)) if uri == "http://example/dynamic#meta"),
        );

        // Test invalid reference error
        test_schema_error(
            json!({
                "$id": "http://example/refs",
                "$ref": "#/does/not/exist"
            }),
            "http://example/refs",
            true,
            |result| {
                matches!(
                    result,
                    Err(Error::InvalidReference { ruri, curi })
                        if ruri == "http://example/refs#/does/not/exist" && curi == "http://example/refs"
                )
            },
        );
    }
}
