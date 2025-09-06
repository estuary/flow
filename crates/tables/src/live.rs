use anyhow::Context;
use serde_json::value::RawValue;

use crate::{
    DataPlanes, Errors, InferredSchemas, LiveCapture, LiveCaptures, LiveCollection,
    LiveCollections, LiveMaterialization, LiveMaterializations, LiveTest, LiveTests,
    StorageMappings,
};

// CatalogResolver is a trait which maps `catalog_names`, such as those from
// DraftCatalog::all_catalog_names(), into their live specifications.
pub trait CatalogResolver {
    /// Fetch live specifications drawn from the provided iterator of catalog names.
    ///
    /// A CatalogResolver MUST return all matched specifications, and MAY return
    /// additional specifications which weren't in the argument `catalog_names`.
    /// One use for such over-fetching is to return alternative, similarly-named
    /// specifications which can help produce better errors for users.
    ///
    /// `catalog_names` may be in any order, and may contain duplicates.
    ///
    fn resolve<'a>(
        &'a self,
        catalog_names: Vec<&'a str>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::LiveCatalog> + Send + 'a>>;
}

/// LiveRow is a common trait of rows reflecting live specifications.
pub trait LiveRow: crate::Row {
    type ModelDef: models::ModelDef;
    type BuiltSpec: Clone;

    // Name of this specification.
    fn catalog_name(&self) -> &Self::Key;
    // Scope of the live specification.
    fn scope(&self) -> url::Url;
    // Control-plane ID of this specification.
    fn control_id(&self) -> models::Id;
    // Data-plane assignment of this specification, if applicable.
    fn data_plane_id(&self) -> Option<models::Id>;
    // Most recent publication ID of this specification.
    fn last_pub_id(&self) -> models::Id;
    // Most recent publication ID of this specification.
    fn last_build_id(&self) -> models::Id;
    // Model of this specification.
    fn model(&self) -> &Self::ModelDef;
    // Most-recent built specification.
    fn spec(&self) -> &Self::BuiltSpec;
    /// Hash of the dependencies that were used to build this row
    fn dependency_hash(&self) -> Option<&str>;
}

impl LiveRow for crate::LiveCapture {
    type ModelDef = models::CaptureDef;
    type BuiltSpec = proto_flow::flow::CaptureSpec;

    fn catalog_name(&self) -> &Self::Key {
        &self.capture
    }
    fn scope(&self) -> url::Url {
        crate::synthetic_scope(models::CatalogType::Capture.to_string(), &self.capture)
    }
    fn control_id(&self) -> models::Id {
        self.control_id
    }
    fn data_plane_id(&self) -> Option<models::Id> {
        Some(self.data_plane_id)
    }
    fn last_pub_id(&self) -> models::Id {
        self.last_pub_id
    }
    fn last_build_id(&self) -> models::Id {
        self.last_build_id
    }
    fn model(&self) -> &Self::ModelDef {
        &self.model
    }
    fn spec(&self) -> &Self::BuiltSpec {
        &self.spec
    }
    fn dependency_hash(&self) -> Option<&str> {
        self.dependency_hash.as_deref()
    }
}

impl LiveRow for crate::LiveCollection {
    type ModelDef = models::CollectionDef;
    type BuiltSpec = proto_flow::flow::CollectionSpec;

    fn catalog_name(&self) -> &Self::Key {
        &self.collection
    }
    fn scope(&self) -> url::Url {
        crate::synthetic_scope(
            models::CatalogType::Collection.to_string(),
            &self.collection,
        )
    }
    fn control_id(&self) -> models::Id {
        self.control_id
    }
    fn data_plane_id(&self) -> Option<models::Id> {
        Some(self.data_plane_id)
    }
    fn last_pub_id(&self) -> models::Id {
        self.last_pub_id
    }
    fn last_build_id(&self) -> models::Id {
        self.last_build_id
    }
    fn model(&self) -> &Self::ModelDef {
        &self.model
    }
    fn spec(&self) -> &Self::BuiltSpec {
        &self.spec
    }
    fn dependency_hash(&self) -> Option<&str> {
        self.dependency_hash.as_deref()
    }
}

impl LiveRow for crate::LiveMaterialization {
    type ModelDef = models::MaterializationDef;
    type BuiltSpec = proto_flow::flow::MaterializationSpec;

    fn catalog_name(&self) -> &Self::Key {
        &self.materialization
    }
    fn scope(&self) -> url::Url {
        crate::synthetic_scope(
            models::CatalogType::Materialization.to_string(),
            &self.materialization,
        )
    }
    fn control_id(&self) -> models::Id {
        self.control_id
    }
    fn data_plane_id(&self) -> Option<models::Id> {
        Some(self.data_plane_id)
    }
    fn last_pub_id(&self) -> models::Id {
        self.last_pub_id
    }
    fn last_build_id(&self) -> models::Id {
        self.last_build_id
    }
    fn model(&self) -> &Self::ModelDef {
        &self.model
    }
    fn spec(&self) -> &Self::BuiltSpec {
        &self.spec
    }
    fn dependency_hash(&self) -> Option<&str> {
        self.dependency_hash.as_deref()
    }
}

impl LiveRow for crate::LiveTest {
    type ModelDef = models::TestDef;
    type BuiltSpec = proto_flow::flow::TestSpec;

    fn catalog_name(&self) -> &Self::Key {
        &self.test
    }
    fn scope(&self) -> url::Url {
        crate::synthetic_scope(models::CatalogType::Test.to_string(), &self.test)
    }
    fn control_id(&self) -> models::Id {
        self.control_id
    }
    fn data_plane_id(&self) -> Option<models::Id> {
        None
    }
    fn last_pub_id(&self) -> models::Id {
        self.last_pub_id
    }
    fn last_build_id(&self) -> models::Id {
        self.last_build_id
    }
    fn model(&self) -> &Self::ModelDef {
        &self.model
    }
    fn spec(&self) -> &Self::BuiltSpec {
        &self.spec
    }
    fn dependency_hash(&self) -> Option<&str> {
        self.dependency_hash.as_deref()
    }
}

#[cfg(feature = "persist")]
impl LiveCatalog {
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
            data_planes,
            errors,
            inferred_schemas,
            materializations,
            storage_mappings,
            tests,
        } = self;

        vec![
            captures,
            collections,
            data_planes,
            errors,
            inferred_schemas,
            materializations,
            storage_mappings,
            tests,
        ]
    }

    // Access all tables as an array of mutable dynamic SqlTableObj instances.
    pub fn as_tables_mut(&mut self) -> Vec<&mut dyn crate::SqlTableObj> {
        let Self {
            captures,
            collections,
            data_planes,
            errors,
            inferred_schemas,
            materializations,
            storage_mappings,
            tests,
        } = self;

        vec![
            captures,
            collections,
            data_planes,
            errors,
            inferred_schemas,
            materializations,
            storage_mappings,
            tests,
        ]
    }
}

// LiveCatalog are tables which are populated from the Estuary control plane.
#[derive(Default, Debug)]
pub struct LiveCatalog {
    pub captures: LiveCaptures,
    pub collections: LiveCollections,
    pub data_planes: DataPlanes,
    pub errors: Errors,
    pub inferred_schemas: InferredSchemas,
    pub materializations: LiveMaterializations,
    pub storage_mappings: StorageMappings,
    pub tests: LiveTests,
}

impl LiveCatalog {
    pub fn is_empty(&self) -> bool {
        self.captures.is_empty()
            && self.collections.is_empty()
            && self.inferred_schemas.is_empty()
            && self.materializations.is_empty()
            && self.tests.is_empty()
    }

    pub fn all_spec_names(&self) -> impl Iterator<Item = &str> {
        self.captures
            .iter()
            .map(|c| c.capture.as_str())
            .chain(self.collections.iter().map(|c| c.collection.as_str()))
            .chain(
                self.materializations
                    .iter()
                    .map(|c| c.materialization.as_str()),
            )
            .chain(self.tests.iter().map(|c| c.test.as_str()))
    }

    pub fn spec_count(&self) -> usize {
        self.captures.len()
            + self.collections.len()
            + self.materializations.len()
            + self.tests.len()
    }

    pub fn last_pub_ids<'a>(&'a self) -> impl Iterator<Item = models::Id> + 'a {
        self.captures
            .iter()
            .map(|v| v.last_pub_id)
            .chain(self.collections.iter().map(|v| v.last_pub_id))
            .chain(self.materializations.iter().map(|v| v.last_pub_id))
            .chain(self.tests.iter().map(|v| v.last_pub_id))
    }

    pub fn add_spec(
        &mut self,
        spec_type: models::CatalogType,
        catalog_name: &str,
        control_id: models::Id,
        data_plane_id: models::Id,
        last_pub_id: models::Id,
        last_build_id: models::Id,
        model_json: &RawValue,
        built_spec_json: &RawValue,
        dependency_hash: Option<String>,
    ) -> anyhow::Result<()> {
        match spec_type {
            models::CatalogType::Capture => {
                let model = serde_json::from_str(model_json.get())
                    .context("deserializing live capture spec")?;
                let built = serde_json::from_str(built_spec_json.get())
                    .context("deserializing live built capture spec")?;
                self.captures.insert(LiveCapture {
                    capture: models::Capture::new(catalog_name),
                    control_id,
                    data_plane_id,
                    last_pub_id,
                    last_build_id,
                    model,
                    spec: built,
                    dependency_hash,
                });
            }
            models::CatalogType::Collection => {
                let model = serde_json::from_str(model_json.get())
                    .context("deserializing live collection spec")?;
                let built = serde_json::from_str(built_spec_json.get())
                    .context("deserializing live built collection spec")?;
                self.collections.insert(LiveCollection {
                    collection: models::Collection::new(catalog_name),
                    control_id,
                    data_plane_id,
                    last_pub_id,
                    last_build_id,
                    model,
                    spec: built,
                    dependency_hash,
                });
            }
            models::CatalogType::Materialization => {
                let model = serde_json::from_str(model_json.get())
                    .context("deserializing live materialization spec")?;
                let built = serde_json::from_str(built_spec_json.get())
                    .context("deserializing live built materialization spec")?;
                self.materializations.insert(LiveMaterialization {
                    materialization: models::Materialization::new(catalog_name),
                    control_id,
                    data_plane_id,
                    last_pub_id,
                    last_build_id,
                    model,
                    spec: built,
                    dependency_hash,
                });
            }
            models::CatalogType::Test => {
                let model = serde_json::from_str(model_json.get())
                    .context("deserializing live test spec")?;
                let built = serde_json::from_str(built_spec_json.get())
                    .context("deserializing live built test spec")?;
                self.tests.insert(LiveTest {
                    test: models::Test::new(catalog_name),
                    control_id,
                    last_pub_id,
                    last_build_id,
                    model,
                    spec: built,
                    dependency_hash,
                });
            }
        }
        Ok(())
    }
}
