use crate::{
    DraftCapture, DraftCollection, DraftMaterialization, DraftTest, LiveCapture, LiveCollection,
    LiveMaterialization, LiveTest,
};
use models::CatalogType;

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

impl From<LiveCapture> for DraftCapture {
    fn from(value: LiveCapture) -> Self {
        let LiveCapture {
            live_spec_id: _,
            catalog_name,
            last_build_id,
            spec,
            built_spec: _,
        } = value;
        DraftCapture {
            scope: crate::synthetic_scope(CatalogType::Capture, &catalog_name),
            catalog_name,
            expect_build_id: Some(last_build_id),
            spec: Some(spec),
        }
    }
}

impl From<LiveCollection> for DraftCollection {
    fn from(value: LiveCollection) -> Self {
        let LiveCollection {
            live_spec_id: _,
            catalog_name,
            last_build_id,
            spec,
            built_spec: _,
            inferred_schema_md5: _,
        } = value;
        DraftCollection {
            scope: crate::synthetic_scope(CatalogType::Collection, &catalog_name),
            catalog_name,
            expect_build_id: Some(last_build_id),
            spec: Some(spec),
        }
    }
}

impl From<LiveMaterialization> for DraftMaterialization {
    fn from(value: LiveMaterialization) -> Self {
        let LiveMaterialization {
            live_spec_id: _,
            catalog_name,
            last_build_id,
            spec,
            built_spec: _,
        } = value;
        DraftMaterialization {
            scope: crate::synthetic_scope(CatalogType::Materialization, &catalog_name),
            catalog_name,
            expect_build_id: Some(last_build_id),
            spec: Some(spec),
        }
    }
}
impl From<LiveTest> for DraftTest {
    fn from(value: LiveTest) -> Self {
        let LiveTest {
            live_spec_id: _,
            catalog_name,
            last_build_id,
            spec,
            built_spec: _,
        } = value;
        DraftTest {
            scope: crate::synthetic_scope(CatalogType::Test, &catalog_name),
            catalog_name,
            expect_build_id: Some(last_build_id),
            spec: Some(spec),
        }
    }
}
