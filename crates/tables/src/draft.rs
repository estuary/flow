use models::ModelDef;

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
