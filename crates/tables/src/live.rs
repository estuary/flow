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
