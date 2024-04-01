use std::collections::BTreeSet;

use crate::{
    cmp_by_name, AnySpec, DraftCapture, DraftCollection, DraftMaterialization, DraftSpecs,
    DraftTest, LiveCapture, LiveCaptures, LiveCollection, LiveCollections, LiveDeletedSpec,
    LiveMaterialization, LiveMaterializations, LiveSpecs, LiveTest, LiveTests, SpecExt,
};
use itertools::Itertools;

impl LiveCaptures {
    pub fn producers_of<'a, 'b: 'a>(
        &'a self,
        collection_name: &'b str,
    ) -> impl Iterator<Item = &'a LiveCapture> + 'a {
        self.iter().filter(|c| c.spec.produces(collection_name))
    }
}

impl LiveCollections {
    pub fn consumers_of<'a, 'b: 'a>(
        &'a self,
        collection_name: &'b str,
    ) -> impl Iterator<Item = &'a LiveCollection> + 'a {
        self.iter().filter(|c| c.spec.consumes(collection_name))
    }
}

impl LiveMaterializations {
    pub fn consumers_of<'a, 'b: 'a>(
        &'a self,
        collection_name: &'b str,
    ) -> impl Iterator<Item = &'a LiveMaterialization> + 'a {
        self.iter().filter(|c| c.spec.consumes(collection_name))
    }
}

impl LiveTests {
    pub fn consumers_of<'a, 'b: 'a>(
        &'a self,
        collection_name: &'b str,
    ) -> impl Iterator<Item = &'a LiveTest> + 'a {
        self.iter().filter(|c| c.spec.consumes(collection_name))
    }
}

impl Into<models::BaseCatalog> for LiveSpecs {
    fn into(self) -> models::BaseCatalog {
        let LiveSpecs {
            captures,
            collections,
            materializations,
            tests,
            deleted: _,
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

impl LiveSpecs {
    pub fn to_catalog(&self) -> models::BaseCatalog {
        let LiveSpecs {
            captures,
            collections,
            materializations,
            tests,
            deleted: _,
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
            && self.deleted.is_empty()
    }

    pub fn into_draft(self) -> DraftSpecs {
        let LiveSpecs {
            captures,
            collections,
            materializations,
            tests,
            // Ignore deletions, because there's no need to ever draft them again.
            deleted: _,
        } = self;

        let captures = captures
            .into_iter()
            .map(|l| DraftCapture {
                catalog_name: l.catalog_name,
                expect_pub_id: Some(l.last_pub_id),
                spec: l.spec,
            })
            .collect();
        let collections = collections
            .into_iter()
            .map(|l| DraftCollection {
                catalog_name: l.catalog_name,
                expect_pub_id: Some(l.last_pub_id),
                spec: l.spec,
            })
            .collect();
        let materializations = materializations
            .into_iter()
            .map(|l| DraftMaterialization {
                catalog_name: l.catalog_name,
                expect_pub_id: Some(l.last_pub_id),
                spec: l.spec,
            })
            .collect();
        let tests = tests
            .into_iter()
            .map(|l| DraftTest {
                catalog_name: l.catalog_name,
                expect_pub_id: Some(l.last_pub_id),
                spec: l.spec,
            })
            .collect();

        DraftSpecs {
            captures,
            collections,
            materializations,
            tests,
            deletions: Default::default(),
        }
    }

    pub fn merge(&mut self, other: LiveSpecs) {
        let LiveSpecs {
            captures,
            collections,
            materializations,
            tests,
            deleted,
        } = other;
        self.captures.extend(captures.into_iter());
        self.collections.extend(collections.into_iter());
        self.materializations.extend(materializations.into_iter());
        self.tests.extend(tests.into_iter());
        self.deleted.extend(deleted.into_iter());
    }

    pub fn get_named(&self, names: &BTreeSet<String>) -> LiveSpecs {
        let captures = crate::inner_join(self.captures.iter(), names.iter())
            .map(|(l, _)| l.clone())
            .collect();
        let tests = crate::inner_join(self.tests.iter(), names.iter())
            .map(|(l, _)| l.clone())
            .collect();
        let materializations = crate::inner_join(self.materializations.iter(), names.iter())
            .map(|(l, _)| l.clone())
            .collect();
        let collections = crate::inner_join(self.collections.iter(), names.iter())
            .map(|(l, _)| l.clone())
            .collect();
        let deleted = crate::inner_join(self.deleted.iter(), names.iter())
            .map(|(l, _)| l.clone())
            .collect();
        LiveSpecs {
            captures,
            collections,
            materializations,
            tests,
            deleted,
        }
    }

    pub fn related_tasks(&self, collection_names: &BTreeSet<String>) -> LiveSpecs {
        let captures = self
            .captures
            .iter()
            .filter(|c| {
                c.spec
                    .bindings
                    .iter()
                    .any(|b| !b.disable && collection_names.contains(b.target.as_str()))
            })
            .cloned()
            .collect();
        let collections = self
            .collections
            .iter()
            .filter(|c| {
                if let Some(derive) = &c.spec.derive {
                    derive.transforms.iter().any(|b| {
                        !b.disable && collection_names.contains(b.source.collection().as_str())
                    })
                } else {
                    false
                }
            })
            .cloned()
            .collect();
        let materializations = self
            .materializations
            .iter()
            .filter(|c| {
                c.spec.bindings.iter().any(|b| {
                    !b.disable && collection_names.contains(b.source.collection().as_str())
                })
            })
            .cloned()
            .collect();
        let tests = self
            .tests
            .iter()
            .filter(|t| {
                t.spec.iter().any(|s| match s {
                    models::TestStep::Ingest(i) => collection_names.contains(i.collection.as_str()),
                    models::TestStep::Verify(v) => {
                        collection_names.contains(v.collection.collection().as_str())
                    }
                })
            })
            .cloned()
            .collect();
        LiveSpecs {
            captures,
            collections,
            materializations,
            tests,
            deleted: Default::default(),
        }
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
