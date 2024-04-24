use std::collections::BTreeSet;

use models::{Collection, TestStep};

#[derive(Debug, Clone, Copy)]
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
    fn is_enabled(&self) -> bool;

    fn uses_any(&self, collections: &BTreeSet<Collection>) -> bool;

    fn consumes(&self, collection_name: &Collection) -> bool;
    fn produces(&self, collection_name: &Collection) -> bool;

    fn reads_from(&self) -> BTreeSet<Collection> {
        Default::default()
    }

    fn writes_to(&self) -> BTreeSet<Collection> {
        Default::default()
    }
}

// impl<'a> SpecExt for AnySpec<'a> {
//     fn consumes(&self, collection_name: &Collection) -> bool {
//         match self {
//             AnySpec::Capture(_) => false,
//             AnySpec::Collection(c) => c.consumes(collection_name),
//             AnySpec::Materialization(m) => m.consumes(collection_name),
//             AnySpec::Test(t) => t.consumes(collection_name),
//         }
//     }

//     fn produces(&self, collection_name: &Collection) -> bool {
//         match self {
//             AnySpec::Capture(c) => c.produces(collection_name),
//             AnySpec::Collection(_) => false,
//             AnySpec::Materialization(_) => false,
//             AnySpec::Test(t) => t.produces(collection_name),
//         }
//     }

//     fn reads_from(&self) -> BTreeSet<String> {
//         match self {
//             AnySpec::Capture(_) => BTreeSet::new(),
//             AnySpec::Collection(c) => c.reads_from(),
//             AnySpec::Materialization(m) => m.reads_from(),
//             AnySpec::Test(t) => t.reads_from(),
//         }
//     }

//     fn writes_to(&self) -> BTreeSet<String> {
//         match self {
//             AnySpec::Capture(c) => c.writes_to(),
//             AnySpec::Collection(_) => BTreeSet::new(),
//             AnySpec::Materialization(_) => BTreeSet::new(),
//             AnySpec::Test(t) => t.writes_to(),
//         }
//     }

//     fn is_enabled(&self) -> bool {
//         match self {
//             AnySpec::Capture(c) => c.is_enabled(),
//             AnySpec::Collection(c) => c.is_enabled(),
//             AnySpec::Materialization(m) => m.is_enabled(),
//             AnySpec::Test(t) => t.is_enabled(),
//         }
//     }
// }

impl SpecExt for models::CaptureDef {
    fn consumes(&self, _: &Collection) -> bool {
        false
    }

    fn produces(&self, collection_name: &Collection) -> bool {
        self.bindings
            .iter()
            .any(|b| !b.disable && b.target == *collection_name)
    }

    fn writes_to(&self) -> BTreeSet<Collection> {
        self.bindings
            .iter()
            .filter(|b| !b.disable)
            .map(|b| b.target.clone())
            .collect()
    }

    fn is_enabled(&self) -> bool {
        !self.shards.disable
    }

    fn uses_any(&self, collections: &BTreeSet<Collection>) -> bool {
        self.bindings
            .iter()
            .any(|b| !b.disable && collections.contains(&b.target))
    }
}

impl SpecExt for models::MaterializationDef {
    fn uses_any(&self, collections: &BTreeSet<Collection>) -> bool {
        self.bindings
            .iter()
            .any(|b| !b.disable && collections.contains(b.source.collection()))
    }

    fn consumes(&self, collection_name: &Collection) -> bool {
        self.bindings
            .iter()
            .any(|b| !b.disable && b.source.collection() == collection_name)
    }

    fn produces(&self, _: &Collection) -> bool {
        false
    }

    fn reads_from(&self) -> BTreeSet<Collection> {
        self.bindings
            .iter()
            .filter(|b| !b.disable)
            .map(|b| b.source.collection().clone())
            .collect()
    }

    fn is_enabled(&self) -> bool {
        !self.shards.disable
    }
}

impl SpecExt for models::CollectionDef {
    fn consumes(&self, collection_name: &Collection) -> bool {
        let Some(derive) = &self.derive else {
            return false;
        };
        derive
            .transforms
            .iter()
            .any(|t| !t.disable && t.source.collection() == collection_name)
    }

    fn produces(&self, _: &Collection) -> bool {
        false
    }

    fn reads_from(&self) -> BTreeSet<Collection> {
        self.derive
            .iter()
            .flat_map(|derive| {
                derive
                    .transforms
                    .iter()
                    .filter(|b| !b.disable)
                    .map(|b| b.source.collection().clone())
            })
            .collect()
    }

    fn is_enabled(&self) -> bool {
        self.derive
            .as_ref()
            .map(|d| !d.shards.disable)
            .unwrap_or(true)
    }

    fn uses_any(&self, collections: &BTreeSet<Collection>) -> bool {
        self.derive
            .iter()
            .flat_map(|derive| {
                derive
                    .transforms
                    .iter()
                    .filter(|b| !b.disable)
                    .map(|b| b.source.collection())
            })
            .any(|c| collections.contains(c))
    }
}

// TODO: IDK if produces and consumes makes a lot of sense for tests
impl SpecExt for models::TestDef {
    fn consumes(&self, collection_name: &Collection) -> bool {
        self.0.iter().any(|step| match step {
            TestStep::Verify(v) => v.collection.collection() == collection_name,
            _ => false,
        })
    }

    fn produces(&self, collection_name: &Collection) -> bool {
        self.0.iter().any(|step| match step {
            TestStep::Ingest(i) => i.collection == *collection_name,
            _ => false,
        })
    }

    fn reads_from(&self) -> BTreeSet<Collection> {
        Default::default()
    }

    fn writes_to(&self) -> BTreeSet<Collection> {
        Default::default()
    }

    fn is_enabled(&self) -> bool {
        true // there's no way to disable a test
    }

    fn uses_any(&self, collections: &BTreeSet<Collection>) -> bool {
        self.0.iter().any(|step| match step {
            TestStep::Verify(v) => collections.contains(&v.collection.collection()),
            _ => false,
        })
    }
}
