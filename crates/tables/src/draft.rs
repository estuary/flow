pub trait DraftRow: crate::Row {
    type Spec;

    fn new(
        catalog_name: Self::Key,
        scope: url::Url,
        expect_build_id: Option<models::Id>,
        spec: Option<Self::Spec>,
    ) -> Self;

    fn catalog_name(&self) -> &Self::Key;
    fn scope(&self) -> &url::Url;
    fn expect_build_id(&self) -> Option<models::Id>;
    fn spec(&self) -> Option<&Self::Spec>;

    fn into_parts(self) -> (Self::Key, url::Url, Option<models::Id>, Option<Self::Spec>);
}

impl DraftRow for crate::DraftCapture {
    type Spec = models::CaptureDef;

    fn new(
        catalog_name: Self::Key,
        scope: url::Url,
        expect_build_id: Option<models::Id>,
        spec: Option<Self::Spec>,
    ) -> Self {
        Self {
            catalog_name,
            scope,
            expect_build_id,
            spec,
        }
    }

    fn catalog_name(&self) -> &Self::Key {
        &self.catalog_name
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn expect_build_id(&self) -> Option<models::Id> {
        self.expect_build_id
    }
    fn spec(&self) -> Option<&Self::Spec> {
        self.spec.as_ref()
    }

    fn into_parts(self) -> (Self::Key, url::Url, Option<models::Id>, Option<Self::Spec>) {
        (
            self.catalog_name,
            self.scope,
            self.expect_build_id,
            self.spec,
        )
    }
}

impl DraftRow for crate::DraftCollection {
    type Spec = models::CollectionDef;

    fn new(
        catalog_name: Self::Key,
        scope: url::Url,
        expect_build_id: Option<models::Id>,
        spec: Option<Self::Spec>,
    ) -> Self {
        Self {
            catalog_name,
            scope,
            expect_build_id,
            spec,
        }
    }

    fn catalog_name(&self) -> &Self::Key {
        &self.catalog_name
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn expect_build_id(&self) -> Option<models::Id> {
        self.expect_build_id
    }
    fn spec(&self) -> Option<&Self::Spec> {
        self.spec.as_ref()
    }

    fn into_parts(self) -> (Self::Key, url::Url, Option<models::Id>, Option<Self::Spec>) {
        (
            self.catalog_name,
            self.scope,
            self.expect_build_id,
            self.spec,
        )
    }
}

impl DraftRow for crate::DraftMaterialization {
    type Spec = models::MaterializationDef;

    fn new(
        catalog_name: Self::Key,
        scope: url::Url,
        expect_build_id: Option<models::Id>,
        spec: Option<Self::Spec>,
    ) -> Self {
        Self {
            catalog_name,
            scope,
            expect_build_id,
            spec,
        }
    }

    fn catalog_name(&self) -> &Self::Key {
        &self.catalog_name
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn expect_build_id(&self) -> Option<models::Id> {
        self.expect_build_id
    }
    fn spec(&self) -> Option<&Self::Spec> {
        self.spec.as_ref()
    }

    fn into_parts(self) -> (Self::Key, url::Url, Option<models::Id>, Option<Self::Spec>) {
        (
            self.catalog_name,
            self.scope,
            self.expect_build_id,
            self.spec,
        )
    }
}

impl DraftRow for crate::DraftTest {
    type Spec = models::TestDef;

    fn new(
        catalog_name: Self::Key,
        scope: url::Url,
        expect_build_id: Option<models::Id>,
        spec: Option<Self::Spec>,
    ) -> Self {
        Self {
            catalog_name,
            scope,
            expect_build_id,
            spec,
        }
    }

    fn catalog_name(&self) -> &Self::Key {
        &self.catalog_name
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn expect_build_id(&self) -> Option<models::Id> {
        self.expect_build_id
    }
    fn spec(&self) -> Option<&Self::Spec> {
        self.spec.as_ref()
    }

    fn into_parts(self) -> (Self::Key, url::Url, Option<models::Id>, Option<Self::Spec>) {
        (
            self.catalog_name,
            self.scope,
            self.expect_build_id,
            self.spec,
        )
    }
}

/*
use itertools::Itertools;
use std::collections::{BTreeMap, BTreeSet};

use crate::{
    cmp_by_name, AnySpec, DraftCapture, DraftCaptures, DraftCollection, DraftCollections,
    DraftMaterialization, DraftMaterializations, DraftSpecs, DraftTest, DraftTests, Id, SpecExt,
};

impl DraftCaptures {
    pub fn producers_of<'a, 'b: 'a>(
        &'a self,
        collection_name: &'b str,
    ) -> impl Iterator<Item = &'a DraftCapture> {
        self.iter().filter(|c| c.spec.produces(collection_name))
    }
}

impl DraftCollections {
    pub fn consumers_of<'a, 'b: 'a>(
        &'a self,
        collection_name: &'b str,
    ) -> impl Iterator<Item = &'a DraftCollection> {
        self.iter().filter(|c| c.spec.consumes(collection_name))
    }
}

impl DraftMaterializations {
    pub fn consumers_of<'a, 'b: 'a>(
        &'a self,
        collection_name: &'b str,
    ) -> impl Iterator<Item = &'a DraftMaterialization> {
        self.iter().filter(|c| c.spec.consumes(collection_name))
    }
}

impl DraftTests {
    pub fn consumers_of<'a, 'b: 'a>(
        &'a self,
        collection_name: &'b str,
    ) -> impl Iterator<Item = &'a DraftTest> {
        self.iter().filter(|c| c.spec.consumes(collection_name))
    }

    pub fn producers_of<'a, 'b: 'a>(
        &'a self,
        collection_name: &'b str,
    ) -> impl Iterator<Item = &'a DraftTest> {
        self.iter().filter(|c| c.spec.produces(collection_name))
    }
}

impl Into<models::BaseCatalog> for DraftSpecs {
    fn into(self) -> models::BaseCatalog {
        let DraftSpecs {
            captures,
            collections,
            materializations,
            tests,
            deletions: _,
        } = self;
        let captures = captures
            .into_iter()
            .map(|c| (models::Capture::new(c.catalog_name), c.spec))
            .collect();
        let collections = collections
            .into_iter()
            .map(|c| (models::Collection::new(c.catalog_name), c.spec))
            .collect();
        let materializations = materializations
            .into_iter()
            .map(|c| (models::Materialization::new(c.catalog_name), c.spec))
            .collect();
        let tests = tests
            .into_iter()
            .map(|c| (models::Test::new(c.catalog_name), c.spec))
            .collect();
        models::BaseCatalog {
            captures,
            collections,
            materializations,
            tests,
        }
    }
}

impl DraftSpecs {
    pub fn from_catalog(
        catalog: models::Catalog,
        expect_pub_ids: BTreeMap<String, Id>,
    ) -> DraftSpecs {
        let models::Catalog {
            captures,
            collections,
            materializations,
            tests,
            storage_mappings: _,
            import: _,
            _schema,
        } = catalog;

        let draft_captures = captures
            .into_iter()
            .map(|(name, spec)| {
                let expect_pub_id = expect_pub_ids.get(name.as_str()).copied();
                DraftCapture {
                    catalog_name: name.into(),
                    expect_pub_id,
                    spec,
                }
            })
            .collect();

        let draft_collections = collections
            .into_iter()
            .map(|(name, spec)| {
                let expect_pub_id = expect_pub_ids.get(name.as_str()).copied();
                DraftCollection {
                    catalog_name: name.into(),
                    expect_pub_id,
                    spec,
                }
            })
            .collect();
        let draft_materializations = materializations
            .into_iter()
            .map(|(name, spec)| {
                let expect_pub_id = expect_pub_ids.get(name.as_str()).copied();
                DraftMaterialization {
                    catalog_name: name.into(),
                    expect_pub_id,
                    spec,
                }
            })
            .collect();
        let draft_tests = tests
            .into_iter()
            .map(|(name, spec)| {
                let expect_pub_id = expect_pub_ids.get(name.as_str()).copied();
                DraftTest {
                    catalog_name: name.into(),
                    expect_pub_id,
                    spec,
                }
            })
            .collect();

        DraftSpecs {
            captures: draft_captures,
            collections: draft_collections,
            materializations: draft_materializations,
            tests: draft_tests,
            deletions: Default::default(),
        }
    }

    pub fn to_catalog(&self) -> models::BaseCatalog {
        let DraftSpecs {
            captures,
            collections,
            materializations,
            tests,
            deletions: _,
        } = self;
        let captures = captures
            .iter()
            .map(|c| (models::Capture::new(&c.catalog_name), c.spec.clone()))
            .collect();
        let collections = collections
            .iter()
            .map(|c| (models::Collection::new(&c.catalog_name), c.spec.clone()))
            .collect();
        let materializations = materializations
            .iter()
            .map(|c| {
                (
                    models::Materialization::new(&c.catalog_name),
                    c.spec.clone(),
                )
            })
            .collect();
        let tests = tests
            .iter()
            .map(|c| (models::Test::new(&c.catalog_name), c.spec.clone()))
            .collect();
        models::BaseCatalog {
            captures,
            collections,
            materializations,
            tests,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.captures.is_empty()
            && self.collections.is_empty()
            && self.materializations.is_empty()
            && self.tests.is_empty()
            && self.deletions.is_empty()
    }

    pub fn merge(&mut self, other: DraftSpecs) {
        let DraftSpecs {
            captures,
            collections,
            materializations,
            tests,
            deletions,
        } = other;
        self.captures.extend(captures.into_iter());
        self.collections.extend(collections.into_iter());
        self.materializations.extend(materializations.into_iter());
        self.tests.extend(tests.into_iter());
        self.deletions.extend(deletions.into_iter());
    }

    pub fn all_spec_names(&self) -> BTreeSet<String> {
        let mut set = BTreeSet::new();
        set.extend(self.captures.iter().map(|c| c.catalog_name.clone()));
        set.extend(self.collections.iter().map(|c| c.catalog_name.clone()));
        set.extend(self.materializations.iter().map(|c| c.catalog_name.clone()));
        set.extend(self.tests.iter().map(|c| c.catalog_name.clone()));
        set.extend(self.deletions.iter().map(|c| c.catalog_name.clone()));
        set
    }

    pub fn consumers_of<'a, 'b: 'a>(
        &'a self,
        collection_name: &'b str,
    ) -> impl Iterator<Item = (&'a str, AnySpec<'a>)> + 'a {
        self.collections
            .consumers_of(collection_name)
            .map(|c| (c.catalog_name.as_str(), AnySpec::Collection(&c.spec)))
            .merge_by(
                self.materializations
                    .consumers_of(collection_name)
                    .map(|m| (m.catalog_name.as_str(), AnySpec::Materialization(&m.spec))),
                cmp_by_name,
            )
            .merge_by(
                self.tests
                    .consumers_of(collection_name)
                    .map(|t| (t.catalog_name.as_str(), AnySpec::Test(&t.spec))),
                cmp_by_name,
            )
    }

    pub fn producers_of<'a, 'b: 'a>(
        &'a self,
        collection_name: &'b str,
    ) -> impl Iterator<Item = (&'a str, AnySpec<'a>)> + 'a {
        self.captures
            .producers_of(collection_name)
            .map(|c| (c.catalog_name.as_str(), AnySpec::Capture(&c.spec)))
    }
}
*/
