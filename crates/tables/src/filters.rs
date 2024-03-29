use std::collections::BTreeSet;

use models::TestStep;

#[derive(Debug, Clone)]
pub enum AnySpec<'a> {
    Capture(&'a models::CaptureDef),
    Collection(&'a models::CollectionDef),
    Materialization(&'a models::MaterializationDef),
    Test(&'a models::TestDef),
}

impl<'a> From<&'a models::CaptureDef> for AnySpec<'a> {
    fn from(value: &'a models::CaptureDef) -> Self {
        AnySpec::Capture(value)
    }
}
impl<'a> From<&'a models::CollectionDef> for AnySpec<'a> {
    fn from(value: &'a models::CollectionDef) -> Self {
        AnySpec::Collection(value)
    }
}
impl<'a> From<&'a models::MaterializationDef> for AnySpec<'a> {
    fn from(value: &'a models::MaterializationDef) -> Self {
        AnySpec::Materialization(value)
    }
}
impl<'a> From<&'a models::TestDef> for AnySpec<'a> {
    fn from(value: &'a models::TestDef) -> Self {
        AnySpec::Test(value)
    }
}

pub trait SpecExt {
    fn consumes(&self, collection_name: &str) -> bool;

    fn produces(&self, collection_name: &str) -> bool;

    fn reads_from(&self) -> BTreeSet<String> {
        Default::default()
    }

    fn writes_to(&self) -> BTreeSet<String> {
        Default::default()
    }
}

impl SpecExt for models::CaptureDef {
    fn consumes(&self, _: &str) -> bool {
        false
    }

    fn produces(&self, collection_name: &str) -> bool {
        self.bindings
            .iter()
            .any(|b| !b.disable && b.target.as_str() == collection_name)
    }

    fn writes_to(&self) -> BTreeSet<String> {
        self.bindings
            .iter()
            .filter(|b| !b.disable)
            .map(|b| b.target.to_string())
            .collect()
    }
}

impl SpecExt for models::MaterializationDef {
    fn consumes(&self, collection_name: &str) -> bool {
        self.bindings
            .iter()
            .any(|b| !b.disable && b.source.collection().as_str() == collection_name)
    }

    fn produces(&self, _: &str) -> bool {
        false
    }

    fn reads_from(&self) -> BTreeSet<String> {
        self.bindings
            .iter()
            .filter(|b| !b.disable)
            .map(|b| b.source.collection().to_string())
            .collect()
    }
}

impl SpecExt for models::CollectionDef {
    fn consumes(&self, collection_name: &str) -> bool {
        let Some(derive) = &self.derive else {
            return false;
        };
        derive
            .transforms
            .iter()
            .any(|t| !t.disable && t.source.collection().as_str() == collection_name)
    }

    fn produces(&self, _: &str) -> bool {
        false
    }

    fn reads_from(&self) -> BTreeSet<String> {
        self.derive
            .iter()
            .flat_map(|derive| {
                derive
                    .transforms
                    .iter()
                    .filter(|b| !b.disable)
                    .map(|b| b.source.collection().to_string())
            })
            .collect()
    }
}

// TODO: IDK if produces and consumes makes a lot of sense for tests
impl SpecExt for models::TestDef {
    fn consumes(&self, collection_name: &str) -> bool {
        self.0.iter().any(|step| match step {
            TestStep::Verify(v) => v.collection.collection().as_str() == collection_name,
            _ => false,
        })
    }

    fn produces(&self, collection_name: &str) -> bool {
        self.0.iter().any(|step| match step {
            TestStep::Ingest(i) => i.collection.as_str() == collection_name,
            _ => false,
        })
    }

    fn reads_from(&self) -> BTreeSet<String> {
        Default::default()
    }

    fn writes_to(&self) -> BTreeSet<String> {
        Default::default()
    }
}
