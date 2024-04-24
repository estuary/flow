use crate::{
    AnySpec, DraftCapture, DraftCollection, DraftMaterialization, DraftTest, Error, LiveCapture,
    LiveCollection, LiveMaterialization, LiveTest,
};
use anyhow::Context;
use models::{CatalogType, ModelDef};
use serde_json::value::RawValue;

impl super::DraftCatalog {
    /// Retrieve all catalog names which are included or referenced
    /// by this DraftCatalog, in sorted and unique order.
    pub fn all_catalog_names<'s>(&'s self) -> Vec<&'s str> {
        let mut out = Vec::new();

        fn inner<'d, D>(out: &mut Vec<&'d str>, rows: &'d [D])
        where
            D: crate::DraftRow,
            D::Key: AsRef<str>,
        {
            for row in rows {
                out.push(row.catalog_name().as_ref());

                let Some(model) = row.model() else { continue };

                for source in model.sources() {
                    out.push(source.collection());
                }
                for target in model.targets() {
                    out.push(target);
                }
            }
        }

        inner(&mut out, &self.captures);
        inner(&mut out, &self.collections);
        inner(&mut out, &self.materializations);
        inner(&mut out, &self.tests);

        out.sort();
        out.dedup();

        out
    }

    pub fn delete(
        &mut self,
        catalog_name: &str,
        spec_type: CatalogType,
        expect_pub_id: Option<models::Id>,
    ) {
        let scope = crate::synthetic_scope(spec_type, catalog_name);
        match spec_type {
            CatalogType::Capture => self.captures.insert(crate::DraftCapture {
                capture: models::Capture::new(catalog_name),
                scope,
                expect_pub_id,
                model: None,
            }),
            CatalogType::Collection => self.collections.insert(crate::DraftCollection {
                collection: models::Collection::new(catalog_name),
                scope,
                expect_pub_id,
                model: None,
            }),
            CatalogType::Materialization => {
                self.materializations.insert(crate::DraftMaterialization {
                    materialization: models::Materialization::new(catalog_name),
                    scope,
                    expect_pub_id,
                    model: None,
                })
            }
            CatalogType::Test => self.tests.insert(crate::DraftTest {
                test: models::Test::new(catalog_name),
                scope,
                expect_pub_id,
                model: None,
            }),
        };
    }

    pub fn add_any_spec(
        &mut self,
        catalog_name: &str,
        spec: AnySpec,
        expect_pub_id: Option<models::Id>,
    ) {
        let scope = crate::synthetic_scope(spec.catalog_type(), catalog_name);
        match spec {
            AnySpec::Capture(model) => {
                self.captures.insert(DraftCapture {
                    capture: models::Capture::new(catalog_name),
                    expect_pub_id,
                    scope,
                    model: Some(model),
                });
            }
            AnySpec::Collection(model) => {
                self.collections.insert(DraftCollection {
                    collection: models::Collection::new(catalog_name),
                    expect_pub_id,
                    scope,
                    model: Some(model),
                });
            }
            AnySpec::Materialization(model) => {
                self.materializations.insert(DraftMaterialization {
                    materialization: models::Materialization::new(catalog_name),
                    expect_pub_id,
                    scope,
                    model: Some(model),
                });
            }
            AnySpec::Test(model) => {
                self.tests.insert(DraftTest {
                    test: models::Test::new(catalog_name),
                    expect_pub_id,
                    scope,
                    model: Some(model),
                });
            }
        }
    }

    pub fn add_spec(
        &mut self,
        spec_type: models::CatalogType,
        catalog_name: &str,
        scope: url::Url,
        expect_pub_id: Option<models::Id>,
        maybe_model: Option<&RawValue>,
    ) -> Result<(), Error> {
        match spec_type {
            models::CatalogType::Capture => {
                let model = if let Some(model_json) = maybe_model {
                    serde_json::from_str(model_json.get())
                        .context("deserializing draft capture spec")
                        .map_err(|error| Error {
                            scope: scope.clone(),
                            error,
                        })?
                } else {
                    None
                };

                self.captures.insert(DraftCapture {
                    capture: models::Capture::new(catalog_name),
                    scope,
                    expect_pub_id,
                    model,
                });
            }
            models::CatalogType::Collection => {
                let model = if let Some(model_json) = maybe_model {
                    serde_json::from_str(model_json.get())
                        .context("deserializing draft collection spec")
                        .map_err(|error| Error {
                            scope: scope.clone(),
                            error,
                        })?
                } else {
                    None
                };
                self.collections.insert(DraftCollection {
                    collection: models::Collection::new(catalog_name),
                    scope,
                    expect_pub_id,
                    model,
                });
            }
            models::CatalogType::Materialization => {
                let model = if let Some(model_json) = maybe_model {
                    serde_json::from_str(model_json.get())
                        .context("deserializing draft materialization spec")
                        .map_err(|error| Error {
                            scope: scope.clone(),
                            error,
                        })?
                } else {
                    None
                };
                self.materializations.insert(DraftMaterialization {
                    materialization: models::Materialization::new(catalog_name),
                    scope,
                    expect_pub_id,
                    model,
                });
            }
            models::CatalogType::Test => {
                let model = if let Some(model_json) = maybe_model {
                    serde_json::from_str(model_json.get())
                        .context("deserializing draft test spec")
                        .map_err(|error| Error {
                            scope: scope.clone(),
                            error,
                        })?
                } else {
                    None
                };
                self.tests.insert(DraftTest {
                    test: models::Test::new(catalog_name),
                    scope,
                    expect_pub_id,
                    model,
                });
            }
        }

        Ok(())
    }
}

/// DraftRow is a common trait of rows reflecting draft specifications.
pub trait DraftRow: crate::Row {
    type ModelDef: models::ModelDef;

    // Build a new DraftRow from its parts.
    fn new(
        catalog_name: Self::Key,
        scope: url::Url,
        expect_pub_id: Option<models::Id>,
        model: Option<Self::ModelDef>,
    ) -> Self;

    /// Convert this DraftRow into its parts.
    fn into_parts(
        self,
    ) -> (
        Self::Key,
        url::Url,
        Option<models::Id>,
        Option<Self::ModelDef>,
    );

    /// Name of this specification.
    fn catalog_name(&self) -> &Self::Key;
    /// Scope of the draft specification.
    fn scope(&self) -> &url::Url;
    /// Expected last publication ID of this specification.
    fn expect_pub_id(&self) -> Option<models::Id>;
    /// Model of this specification.
    fn model(&self) -> Option<&Self::ModelDef>;
}

impl DraftRow for crate::DraftCapture {
    type ModelDef = models::CaptureDef;

    fn new(
        capture: Self::Key,
        scope: url::Url,
        expect_pub_id: Option<models::Id>,
        model: Option<Self::ModelDef>,
    ) -> Self {
        Self {
            capture,
            scope,
            expect_pub_id,
            model,
        }
    }

    fn into_parts(
        self,
    ) -> (
        Self::Key,
        url::Url,
        Option<models::Id>,
        Option<Self::ModelDef>,
    ) {
        (self.capture, self.scope, self.expect_pub_id, self.model)
    }

    fn catalog_name(&self) -> &Self::Key {
        &self.capture
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn expect_pub_id(&self) -> Option<models::Id> {
        self.expect_pub_id
    }
    fn model(&self) -> Option<&Self::ModelDef> {
        self.model.as_ref()
    }
}

impl DraftRow for crate::DraftCollection {
    type ModelDef = models::CollectionDef;

    fn new(
        collection: Self::Key,
        scope: url::Url,
        expect_pub_id: Option<models::Id>,
        model: Option<Self::ModelDef>,
    ) -> Self {
        Self {
            collection,
            scope,
            expect_pub_id,
            model,
        }
    }

    fn into_parts(
        self,
    ) -> (
        Self::Key,
        url::Url,
        Option<models::Id>,
        Option<Self::ModelDef>,
    ) {
        (self.collection, self.scope, self.expect_pub_id, self.model)
    }

    fn catalog_name(&self) -> &Self::Key {
        &self.collection
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn expect_pub_id(&self) -> Option<models::Id> {
        self.expect_pub_id
    }
    fn model(&self) -> Option<&Self::ModelDef> {
        self.model.as_ref()
    }
}

impl DraftRow for crate::DraftMaterialization {
    type ModelDef = models::MaterializationDef;

    fn new(
        materialization: Self::Key,
        scope: url::Url,
        expect_pub_id: Option<models::Id>,
        model: Option<Self::ModelDef>,
    ) -> Self {
        Self {
            materialization,
            scope,
            expect_pub_id,
            model,
        }
    }

    fn into_parts(
        self,
    ) -> (
        Self::Key,
        url::Url,
        Option<models::Id>,
        Option<Self::ModelDef>,
    ) {
        (
            self.materialization,
            self.scope,
            self.expect_pub_id,
            self.model,
        )
    }

    fn catalog_name(&self) -> &Self::Key {
        &self.materialization
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn expect_pub_id(&self) -> Option<models::Id> {
        self.expect_pub_id
    }
    fn model(&self) -> Option<&Self::ModelDef> {
        self.model.as_ref()
    }
}

impl DraftRow for crate::DraftTest {
    type ModelDef = models::TestDef;

    fn new(
        test: Self::Key,
        scope: url::Url,
        expect_pub_id: Option<models::Id>,
        model: Option<Self::ModelDef>,
    ) -> Self {
        Self {
            test,
            scope,
            expect_pub_id,
            model,
        }
    }

    fn into_parts(
        self,
    ) -> (
        Self::Key,
        url::Url,
        Option<models::Id>,
        Option<Self::ModelDef>,
    ) {
        (self.test, self.scope, self.expect_pub_id, self.model)
    }

    fn catalog_name(&self) -> &Self::Key {
        &self.test
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn expect_pub_id(&self) -> Option<models::Id> {
        self.expect_pub_id
    }
    fn model(&self) -> Option<&Self::ModelDef> {
        self.model.as_ref()
    }
}

impl From<LiveCapture> for DraftCapture {
    fn from(value: LiveCapture) -> Self {
        let LiveCapture {
            scope,
            capture,
            last_pub_id,
            model,
            spec: _,
        } = value;
        DraftCapture {
            scope,
            capture,
            expect_pub_id: Some(last_pub_id),
            model: Some(model),
        }
    }
}

impl From<LiveCollection> for DraftCollection {
    fn from(value: LiveCollection) -> Self {
        let LiveCollection {
            scope,
            collection,
            last_pub_id,
            model,
            spec: _,
        } = value;
        DraftCollection {
            scope,
            collection,
            expect_pub_id: Some(last_pub_id),
            model: Some(model),
        }
    }
}

impl From<LiveMaterialization> for DraftMaterialization {
    fn from(value: LiveMaterialization) -> Self {
        let LiveMaterialization {
            materialization,
            spec: _,
            model,
            scope,
            last_pub_id,
        } = value;
        DraftMaterialization {
            scope,
            materialization,
            expect_pub_id: Some(last_pub_id),
            model: Some(model),
        }
    }
}
impl From<LiveTest> for DraftTest {
    fn from(value: LiveTest) -> Self {
        let LiveTest {
            test,
            last_pub_id,
            spec: _,
            model,
            scope,
        } = value;
        DraftTest {
            scope,
            test,
            expect_pub_id: Some(last_pub_id),
            model: Some(model),
        }
    }
}
