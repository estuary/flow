/// BuiltRow is a common trait of rows reflecting built specifications.
pub trait BuiltRow: crate::Row {
    type ModelDef: models::ModelDef;
    type Validated;
    type BuiltSpec;

    // Build a new BuiltRow from its parts.
    fn new(
        catalog_name: Self::Key,
        scope: url::Url,
        expect_pub_id: models::Id,
        model: Option<Self::ModelDef>,
        validated: Option<Self::Validated>,
        spec: Option<Self::BuiltSpec>,
        previous_spec: Option<Self::BuiltSpec>,
    ) -> Self;

    // Name of this specification.
    fn catalog_name(&self) -> &Self::Key;
    // Scope of the built specification.
    fn scope(&self) -> &url::Url;
    // Expected last publication ID for optimistic concurrency.
    fn expect_pub_id(&self) -> models::Id;
    // Model of the built specification.
    fn model(&self) -> Option<&Self::ModelDef>;
    // Validated response which was used to build this spec.
    fn validated(&self) -> Option<&Self::Validated>;
    // Built specification, or None if it's being deleted.
    fn spec(&self) -> Option<&Self::BuiltSpec>;
    // Previous specification which is being modified or deleted,
    // or None if unchanged OR this is an insertion.
    fn previous_spec(&self) -> Option<&Self::BuiltSpec>;

    // Is this specification unchanged (passed through) from its live specification?
    fn is_unchanged(&self) -> bool {
        !self.expect_pub_id().is_zero() && self.previous_spec().is_none()
    }
    fn is_insert(&self) -> bool {
        self.expect_pub_id().is_zero()
    }
    fn is_update(&self) -> bool {
        !self.expect_pub_id().is_zero() && self.previous_spec().is_some() && self.spec().is_some()
    }
    fn is_delete(&self) -> bool {
        !self.expect_pub_id().is_zero() && self.previous_spec().is_some() && self.spec().is_none()
    }
}

impl BuiltRow for crate::BuiltCapture {
    type ModelDef = models::CaptureDef;
    type Validated = proto_flow::capture::response::Validated;
    type BuiltSpec = proto_flow::flow::CaptureSpec;

    fn new(
        catalog_name: Self::Key,
        scope: url::Url,
        expect_pub_id: models::Id,
        model: Option<Self::ModelDef>,
        validated: Option<Self::Validated>,
        spec: Option<Self::BuiltSpec>,
        previous_spec: Option<Self::BuiltSpec>,
    ) -> Self {
        Self {
            capture: catalog_name,
            scope,
            expect_pub_id,
            model,
            validated,
            spec,
            previous_spec,
        }
    }
    fn catalog_name(&self) -> &Self::Key {
        &self.capture
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn expect_pub_id(&self) -> models::Id {
        self.expect_pub_id
    }
    fn model(&self) -> Option<&Self::ModelDef> {
        self.model.as_ref()
    }
    fn validated(&self) -> Option<&Self::Validated> {
        self.validated.as_ref()
    }
    fn spec(&self) -> Option<&Self::BuiltSpec> {
        self.spec.as_ref()
    }
    fn previous_spec(&self) -> Option<&Self::BuiltSpec> {
        self.previous_spec.as_ref()
    }
}

impl BuiltRow for crate::BuiltCollection {
    type ModelDef = models::CollectionDef;
    type Validated = proto_flow::derive::response::Validated;
    type BuiltSpec = proto_flow::flow::CollectionSpec;

    fn new(
        catalog_name: Self::Key,
        scope: url::Url,
        expect_pub_id: models::Id,
        model: Option<Self::ModelDef>,
        validated: Option<Self::Validated>,
        spec: Option<Self::BuiltSpec>,
        previous_spec: Option<Self::BuiltSpec>,
    ) -> Self {
        Self {
            collection: catalog_name,
            scope,
            expect_pub_id,
            model,
            validated,
            spec,
            previous_spec,
        }
    }
    fn catalog_name(&self) -> &Self::Key {
        &self.collection
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn expect_pub_id(&self) -> models::Id {
        self.expect_pub_id
    }
    fn model(&self) -> Option<&Self::ModelDef> {
        self.model.as_ref()
    }
    fn validated(&self) -> Option<&Self::Validated> {
        self.validated.as_ref()
    }
    fn spec(&self) -> Option<&Self::BuiltSpec> {
        self.spec.as_ref()
    }
    fn previous_spec(&self) -> Option<&Self::BuiltSpec> {
        self.previous_spec.as_ref()
    }
}

impl BuiltRow for crate::BuiltMaterialization {
    type ModelDef = models::MaterializationDef;
    type Validated = proto_flow::materialize::response::Validated;
    type BuiltSpec = proto_flow::flow::MaterializationSpec;

    fn new(
        catalog_name: Self::Key,
        scope: url::Url,
        expect_pub_id: models::Id,
        model: Option<Self::ModelDef>,
        validated: Option<Self::Validated>,
        spec: Option<Self::BuiltSpec>,
        previous_spec: Option<Self::BuiltSpec>,
    ) -> Self {
        Self {
            materialization: catalog_name,
            scope,
            expect_pub_id,
            model,
            validated,
            spec,
            previous_spec,
        }
    }
    fn catalog_name(&self) -> &Self::Key {
        &self.materialization
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn expect_pub_id(&self) -> models::Id {
        self.expect_pub_id
    }
    fn model(&self) -> Option<&Self::ModelDef> {
        self.model.as_ref()
    }
    fn validated(&self) -> Option<&Self::Validated> {
        self.validated.as_ref()
    }
    fn spec(&self) -> Option<&Self::BuiltSpec> {
        self.spec.as_ref()
    }
    fn previous_spec(&self) -> Option<&Self::BuiltSpec> {
        self.previous_spec.as_ref()
    }
}

impl BuiltRow for crate::BuiltTest {
    type ModelDef = models::TestDef;
    type Validated = ();
    type BuiltSpec = proto_flow::flow::TestSpec;

    fn new(
        catalog_name: Self::Key,
        scope: url::Url,
        expect_pub_id: models::Id,
        model: Option<Self::ModelDef>,
        _validated: Option<Self::Validated>,
        spec: Option<Self::BuiltSpec>,
        previous_spec: Option<Self::BuiltSpec>,
    ) -> Self {
        Self {
            test: catalog_name,
            scope,
            expect_pub_id,
            model,
            spec,
            previous_spec,
        }
    }
    fn catalog_name(&self) -> &Self::Key {
        &self.test
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn expect_pub_id(&self) -> models::Id {
        self.expect_pub_id
    }
    fn model(&self) -> Option<&Self::ModelDef> {
        self.model.as_ref()
    }
    fn validated(&self) -> Option<&Self::Validated> {
        None
    }
    fn spec(&self) -> Option<&Self::BuiltSpec> {
        self.spec.as_ref()
    }
    fn previous_spec(&self) -> Option<&Self::BuiltSpec> {
        self.previous_spec.as_ref()
    }
}

impl super::Validations {
    pub fn count(&self) -> usize {
        self.built_captures.len()
            + self.built_collections.len()
            + self.built_materializations.len()
            + self.built_tests.len()
    }
}
