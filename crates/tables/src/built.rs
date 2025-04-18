use crate::{BuiltCaptures, BuiltCollections, BuiltMaterializations, BuiltTests, Errors};

/// BuiltRow is a common trait of rows reflecting built specifications.
pub trait BuiltRow: crate::Row {
    type ModelDef: models::ModelDef;
    type Validated;
    type BuiltSpec;

    // Build a new BuiltRow from its parts.
    fn new(
        catalog_name: Self::Key,
        scope: url::Url,
        control_id: models::Id,
        data_plane_id: models::Id,
        expect_pub_id: models::Id,
        expect_build_id: models::Id,
        model: Option<Self::ModelDef>,
        model_fixes: Vec<String>,
        validated: Option<Self::Validated>,
        spec: Option<Self::BuiltSpec>,
        previous_spec: Option<Self::BuiltSpec>,
        is_touch: bool,
        dependency_hash: Option<String>,
    ) -> Self;

    // Name of this specification.
    fn catalog_name(&self) -> &Self::Key;
    // Scope of the built specification.
    fn scope(&self) -> &url::Url;
    // Control-plane ID of this specification, or zero if un-assigned.
    fn control_id(&self) -> models::Id;
    // Data-plane ID of this specification.
    fn data_plane_id(&self) -> models::Id;
    // Expected last publication ID for optimistic concurrency.
    fn expect_pub_id(&self) -> models::Id;
    // Expected last build ID for optimistic concurrency.
    fn expect_build_id(&self) -> models::Id;
    // Model of the built specification.
    fn model(&self) -> Option<&Self::ModelDef>;
    // Descriptions of automated model changes made during validation.
    // If non-empty then `is_touch` is false.
    fn model_fixes(&self) -> &[String];
    // Validated response which was used to build this spec.
    fn validated(&self) -> Option<&Self::Validated>;
    // Built specification, or None if it's being deleted.
    fn spec(&self) -> Option<&Self::BuiltSpec>;
    // Previous specification which is being modified or deleted,
    // or None if unchanged OR this is an insertion.
    fn previous_spec(&self) -> Option<&Self::BuiltSpec>;
    // When true this was a "touch" which refreshed the build of the
    // unchanged live model. Its publication ID shouldn't increase.
    fn is_touch(&self) -> bool;

    /// Is this specification untouched (passed through) from its live specification?
    fn is_passthrough(&self) -> bool {
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
        control_id: models::Id,
        data_plane_id: models::Id,
        expect_pub_id: models::Id,
        expect_build_id: models::Id,
        model: Option<Self::ModelDef>,
        model_fixes: Vec<String>,
        validated: Option<Self::Validated>,
        spec: Option<Self::BuiltSpec>,
        previous_spec: Option<Self::BuiltSpec>,
        is_touch: bool,
        dependency_hash: Option<String>,
    ) -> Self {
        Self {
            capture: catalog_name,
            scope,
            control_id,
            data_plane_id,
            expect_pub_id,
            expect_build_id,
            model,
            model_fixes,
            validated,
            spec,
            previous_spec,
            is_touch,
            dependency_hash,
        }
    }
    fn catalog_name(&self) -> &Self::Key {
        &self.capture
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn control_id(&self) -> models::Id {
        self.control_id
    }
    fn data_plane_id(&self) -> models::Id {
        self.data_plane_id
    }
    fn expect_pub_id(&self) -> models::Id {
        self.expect_pub_id
    }
    fn expect_build_id(&self) -> models::Id {
        self.expect_build_id
    }
    fn model(&self) -> Option<&Self::ModelDef> {
        self.model.as_ref()
    }
    fn model_fixes(&self) -> &[String] {
        &self.model_fixes
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
    fn is_touch(&self) -> bool {
        self.is_touch
    }
}

impl BuiltRow for crate::BuiltCollection {
    type ModelDef = models::CollectionDef;
    type Validated = proto_flow::derive::response::Validated;
    type BuiltSpec = proto_flow::flow::CollectionSpec;

    fn new(
        catalog_name: Self::Key,
        scope: url::Url,
        control_id: models::Id,
        data_plane_id: models::Id,
        expect_pub_id: models::Id,
        expect_build_id: models::Id,
        model: Option<Self::ModelDef>,
        model_fixes: Vec<String>,
        validated: Option<Self::Validated>,
        spec: Option<Self::BuiltSpec>,
        previous_spec: Option<Self::BuiltSpec>,
        is_touch: bool,
        dependency_hash: Option<String>,
    ) -> Self {
        Self {
            collection: catalog_name,
            scope,
            control_id,
            data_plane_id,
            expect_pub_id,
            expect_build_id,
            model,
            model_fixes,
            validated,
            spec,
            previous_spec,
            is_touch,
            dependency_hash,
        }
    }
    fn catalog_name(&self) -> &Self::Key {
        &self.collection
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn control_id(&self) -> models::Id {
        self.control_id
    }
    fn data_plane_id(&self) -> models::Id {
        self.data_plane_id
    }
    fn expect_pub_id(&self) -> models::Id {
        self.expect_pub_id
    }
    fn expect_build_id(&self) -> models::Id {
        self.expect_build_id
    }
    fn model(&self) -> Option<&Self::ModelDef> {
        self.model.as_ref()
    }
    fn model_fixes(&self) -> &[String] {
        &self.model_fixes
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
    fn is_touch(&self) -> bool {
        self.is_touch
    }
}

impl BuiltRow for crate::BuiltMaterialization {
    type ModelDef = models::MaterializationDef;
    type Validated = proto_flow::materialize::response::Validated;
    type BuiltSpec = proto_flow::flow::MaterializationSpec;

    fn new(
        catalog_name: Self::Key,
        scope: url::Url,
        control_id: models::Id,
        data_plane_id: models::Id,
        expect_pub_id: models::Id,
        expect_build_id: models::Id,
        model: Option<Self::ModelDef>,
        model_fixes: Vec<String>,
        validated: Option<Self::Validated>,
        spec: Option<Self::BuiltSpec>,
        previous_spec: Option<Self::BuiltSpec>,
        is_touch: bool,
        dependency_hash: Option<String>,
    ) -> Self {
        Self {
            materialization: catalog_name,
            scope,
            control_id,
            data_plane_id,
            expect_pub_id,
            expect_build_id,
            model,
            model_fixes,
            validated,
            spec,
            previous_spec,
            is_touch,
            dependency_hash,
        }
    }
    fn catalog_name(&self) -> &Self::Key {
        &self.materialization
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn control_id(&self) -> models::Id {
        self.control_id
    }
    fn data_plane_id(&self) -> models::Id {
        self.data_plane_id
    }
    fn expect_pub_id(&self) -> models::Id {
        self.expect_pub_id
    }
    fn expect_build_id(&self) -> models::Id {
        self.expect_build_id
    }
    fn model(&self) -> Option<&Self::ModelDef> {
        self.model.as_ref()
    }
    fn model_fixes(&self) -> &[String] {
        &self.model_fixes
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
    fn is_touch(&self) -> bool {
        self.is_touch
    }
}

impl BuiltRow for crate::BuiltTest {
    type ModelDef = models::TestDef;
    type Validated = ();
    type BuiltSpec = proto_flow::flow::TestSpec;

    fn new(
        catalog_name: Self::Key,
        scope: url::Url,
        control_id: models::Id,
        _data_plane_id: models::Id,
        expect_pub_id: models::Id,
        expect_build_id: models::Id,
        model: Option<Self::ModelDef>,
        model_fixes: Vec<String>,
        _validated: Option<Self::Validated>,
        spec: Option<Self::BuiltSpec>,
        previous_spec: Option<Self::BuiltSpec>,
        is_touch: bool,
        dependency_hash: Option<String>,
    ) -> Self {
        Self {
            test: catalog_name,
            scope,
            control_id,
            expect_pub_id,
            expect_build_id,
            model,
            model_fixes,
            spec,
            previous_spec,
            is_touch,
            dependency_hash,
        }
    }
    fn catalog_name(&self) -> &Self::Key {
        &self.test
    }
    fn scope(&self) -> &url::Url {
        &self.scope
    }
    fn control_id(&self) -> models::Id {
        self.control_id
    }
    fn data_plane_id(&self) -> models::Id {
        models::Id::zero()
    }
    fn expect_pub_id(&self) -> models::Id {
        self.expect_pub_id
    }
    fn expect_build_id(&self) -> models::Id {
        self.expect_build_id
    }
    fn model(&self) -> Option<&Self::ModelDef> {
        self.model.as_ref()
    }
    fn model_fixes(&self) -> &[String] {
        &self.model_fixes
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
    fn is_touch(&self) -> bool {
        self.is_touch
    }
}

/// Validations are tables populated by catalog validations of the `validation` crate.
#[derive(Default, Debug)]
pub struct Validations {
    pub built_captures: BuiltCaptures,
    pub built_collections: BuiltCollections,
    pub built_materializations: BuiltMaterializations,
    pub built_tests: BuiltTests,
    pub errors: Errors,
}

impl Validations {
    pub fn all_spec_names(&self) -> impl Iterator<Item = &str> {
        self.built_captures
            .iter()
            .map(|r| r.catalog_name().as_str())
            .chain(
                self.built_collections
                    .iter()
                    .map(|r| r.catalog_name().as_str()),
            )
            .chain(
                self.built_materializations
                    .iter()
                    .map(|r| r.catalog_name().as_str()),
            )
            .chain(self.built_tests.iter().map(|r| r.catalog_name().as_str()))
    }

    pub fn spec_count(&self) -> usize {
        self.all_spec_names().count()
    }
}

#[cfg(feature = "persist")]
impl Validations {
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
            built_captures,
            built_collections,
            built_materializations,
            built_tests,
            errors,
        } = self;

        vec![
            built_captures,
            built_collections,
            built_materializations,
            built_tests,
            errors,
        ]
    }

    // Access all tables as an array of mutable dynamic SqlTableObj instances.
    pub fn as_tables_mut(&mut self) -> Vec<&mut dyn crate::SqlTableObj> {
        let Self {
            built_captures,
            built_collections,
            built_materializations,
            built_tests,
            errors,
        } = self;

        vec![
            built_captures,
            built_collections,
            built_materializations,
            built_tests,
            errors,
        ]
    }
}
