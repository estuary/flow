use anyhow::Context;
use serde_json::value::RawValue;

use crate::{LiveCapture, LiveCollection, LiveMaterialization, LiveTest};

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
    fn scope(&self) -> &url::Url;
    // Most recent publication ID of this specification.
    fn last_pub_id(&self) -> models::Id;
    // Model of this specification.
    fn model(&self) -> &Self::ModelDef;
    // Most-recent built specification.
    fn spec(&self) -> &Self::BuiltSpec;
}

impl LiveRow for crate::LiveCapture {
    type ModelDef = models::CaptureDef;
    type BuiltSpec = proto_flow::flow::CaptureSpec;

    fn catalog_name(&self) -> &Self::Key {
        &self.capture
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn last_pub_id(&self) -> models::Id {
        self.last_pub_id
    }
    fn model(&self) -> &Self::ModelDef {
        &self.model
    }
    fn spec(&self) -> &Self::BuiltSpec {
        &self.spec
    }
}

impl LiveRow for crate::LiveCollection {
    type ModelDef = models::CollectionDef;
    type BuiltSpec = proto_flow::flow::CollectionSpec;

    fn catalog_name(&self) -> &Self::Key {
        &self.collection
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn last_pub_id(&self) -> models::Id {
        self.last_pub_id
    }
    fn model(&self) -> &Self::ModelDef {
        &self.model
    }
    fn spec(&self) -> &Self::BuiltSpec {
        &self.spec
    }
}

impl LiveRow for crate::LiveMaterialization {
    type ModelDef = models::MaterializationDef;
    type BuiltSpec = proto_flow::flow::MaterializationSpec;

    fn catalog_name(&self) -> &Self::Key {
        &self.materialization
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn last_pub_id(&self) -> models::Id {
        self.last_pub_id
    }
    fn model(&self) -> &Self::ModelDef {
        &self.model
    }
    fn spec(&self) -> &Self::BuiltSpec {
        &self.spec
    }
}

impl LiveRow for crate::LiveTest {
    type ModelDef = models::TestDef;
    type BuiltSpec = proto_flow::flow::TestSpec;

    fn catalog_name(&self) -> &Self::Key {
        &self.test
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn last_pub_id(&self) -> models::Id {
        self.last_pub_id
    }
    fn model(&self) -> &Self::ModelDef {
        &self.model
    }
    fn spec(&self) -> &Self::BuiltSpec {
        &self.spec
    }
}

impl super::LiveCatalog {
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
        scope: url::Url,
        last_pub_id: models::Id,
        model_json: &RawValue,
        built_spec_json: &RawValue,
    ) -> anyhow::Result<()> {
        match spec_type {
            models::CatalogType::Capture => {
                let model = serde_json::from_str(model_json.get())
                    .context("deserializing live capture spec")?;
                let built = serde_json::from_str(built_spec_json.get())
                    .context("deserializing live built capture spec")?;
                self.captures.insert(LiveCapture {
                    capture: models::Capture::new(catalog_name),
                    scope,
                    last_pub_id,
                    model,
                    spec: built,
                });
            }
            models::CatalogType::Collection => {
                let model = serde_json::from_str(model_json.get())
                    .context("deserializing live collection spec")?;
                let built = serde_json::from_str(built_spec_json.get())
                    .context("deserializing live built collection spec")?;
                self.collections.insert(LiveCollection {
                    collection: models::Collection::new(catalog_name),
                    scope,
                    last_pub_id,
                    model,
                    spec: built,
                });
            }
            models::CatalogType::Materialization => {
                let model = serde_json::from_str(model_json.get())
                    .context("deserializing live materialization spec")?;
                let built = serde_json::from_str(built_spec_json.get())
                    .context("deserializing live built materialization spec")?;
                self.materializations.insert(LiveMaterialization {
                    materialization: models::Materialization::new(catalog_name),
                    scope,
                    last_pub_id,
                    model,
                    spec: built,
                });
            }
            models::CatalogType::Test => {
                let model = serde_json::from_str(model_json.get())
                    .context("deserializing live test spec")?;
                let built = serde_json::from_str(built_spec_json.get())
                    .context("deserializing live built test spec")?;
                self.tests.insert(LiveTest {
                    test: models::Test::new(catalog_name),
                    scope,
                    last_pub_id,
                    model,
                    spec: built,
                });
            }
        }
        Ok(())
    }
}
