use crate::{
    synthetic_scope, DraftCapture, DraftCaptures, DraftCollection, DraftCollections,
    DraftMaterialization, DraftMaterializations, DraftTest, DraftTests, Error, Errors, Fetches,
    Imports, Resources, Row, Table,
};
use anyhow::Context;
use models::{CatalogType, ModelDef};
use serde_json::value::RawValue;

/// DraftCatalog are tables which are populated by catalog loads of the `sources` crate.
#[derive(Default)]
pub struct DraftCatalog {
    pub captures: DraftCaptures,
    pub collections: DraftCollections,
    pub materializations: DraftMaterializations,
    pub errors: Errors,
    pub fetches: Fetches,
    pub imports: Imports,
    pub resources: Resources,
    pub tests: DraftTests,
}

impl DraftCatalog {
    pub fn spec_count(&self) -> usize {
        self.all_spec_names().count()
    }

    pub fn all_spec_names(&self) -> impl Iterator<Item = &str> {
        self.captures
            .iter()
            .map(|c| c.catalog_name().as_str())
            .chain(self.collections.iter().map(|c| c.catalog_name().as_str()))
            .chain(
                self.materializations
                    .iter()
                    .map(|m| m.catalog_name().as_str()),
            )
            .chain(self.tests.iter().map(|t| t.catalog_name().as_str()))
    }

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
                if let Some(cap) = model.materialization_source_capture() {
                    out.push(cap.as_str());
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

impl std::fmt::Debug for DraftCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = &mut f.debug_struct("DraftCatalog");

        fn field<'a, 'b, 'c, T: Row>(
            s: &'c mut std::fmt::DebugStruct<'a, 'b>,
            name: &str,
            value: &Table<T>,
        ) -> &'c mut std::fmt::DebugStruct<'a, 'b> {
            if !value.is_empty() {
                s.field(name, value);
            }
            s
        }

        s = field(s, "captures", &self.captures);
        s = field(s, "collections", &self.collections);
        s = field(s, "materializations", &self.materializations);
        s = field(s, "tests", &self.tests);
        s = field(s, "errors", &self.errors);
        s = field(s, "fetches", &self.fetches);
        s = field(s, "imports", &self.imports);
        s = field(s, "resources", &self.resources);
        s.finish()
    }
}

impl From<models::Catalog> for DraftCatalog {
    fn from(value: models::Catalog) -> Self {
        Self {
            captures: value
                .captures
                .into_iter()
                .map(|(name, mut spec)| {
                    let expect_pub_id = spec.expect_pub_id.take();
                    DraftCapture {
                        scope: synthetic_scope(models::CatalogType::Capture, &name),
                        capture: name,
                        model: Some(spec),
                        expect_pub_id,
                    }
                })
                .collect(),
            collections: value
                .collections
                .into_iter()
                .map(|(name, mut spec)| {
                    let expect_pub_id = spec.expect_pub_id.take();
                    DraftCollection {
                        scope: synthetic_scope(models::CatalogType::Collection, &name),
                        collection: name,
                        model: Some(spec),
                        expect_pub_id,
                    }
                })
                .collect(),
            materializations: value
                .materializations
                .into_iter()
                .map(|(name, mut spec)| {
                    let expect_pub_id = spec.expect_pub_id.take();
                    DraftMaterialization {
                        scope: synthetic_scope(models::CatalogType::Materialization, &name),
                        materialization: name,
                        model: Some(spec),
                        expect_pub_id,
                    }
                })
                .collect(),
            tests: value
                .tests
                .into_iter()
                .map(|(name, mut spec)| {
                    let expect_pub_id = spec.expect_pub_id.take();
                    DraftTest {
                        scope: synthetic_scope(models::CatalogType::Test, &name),
                        test: name,
                        model: Some(spec),
                        expect_pub_id,
                    }
                })
                .collect(),
            ..Default::default()
        }
    }
}

#[cfg(feature = "persist")]
impl DraftCatalog {
    pub fn into_result(mut self) -> Result<Self, Errors> {
        match std::mem::take(&mut self.errors) {
            errors if errors.is_empty() => Ok(self),
            errors => Err(errors),
        }
    }

    // Access all tables as an array of dynamic TableObj instances.
    pub fn as_tables(&self) -> Vec<&dyn crate::SqlTableObj> {
        // This de-structure ensures we can't fail to update as tables change.
        let Self {
            captures,
            collections,
            errors,
            fetches,
            imports,
            materializations,
            resources,
            tests,
        } = self;

        vec![
            captures,
            collections,
            errors,
            fetches,
            imports,
            materializations,
            resources,
            tests,
        ]
    }

    // Access all tables as an array of mutable dynamic SqlTableObj instances.
    pub fn as_tables_mut(&mut self) -> Vec<&mut dyn crate::SqlTableObj> {
        let Self {
            captures,
            collections,
            errors,
            fetches,
            imports,
            materializations,
            resources,
            tests,
        } = self;

        vec![
            captures,
            collections,
            errors,
            fetches,
            imports,
            materializations,
            resources,
            tests,
        ]
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
