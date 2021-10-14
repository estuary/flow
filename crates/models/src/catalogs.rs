use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json, Value};
use std::collections::BTreeMap;

use super::{
    Capture, CaptureDef, Collection, CollectionDef, Import, Materialization, MaterializationDef,
    StorageMapping, Test, TestStep,
};

/// Each catalog source defines a portion of a Flow Catalog, by defining
/// collections, derivations, tests, and materializations of the Catalog.
/// Catalog sources may reference and import other sources, in order to
/// collections and other entities that source defines.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Catalog {
    /// # JSON-Schema against which the Catalog is validated.
    #[serde(default, rename = "$schema")]
    pub _schema: Option<String>,
    /// # Import other Flow catalog sources.
    /// By importing another Flow catalog source, the collections, schemas, and derivations
    /// it defines become usable within this Catalog source. Each import is an absolute URI,
    /// or a URI which is relative to this source location.
    #[serde(default)]
    pub import: Vec<Import>,
    /// # NPM package dependencies of the Catalog.
    /// Dependencies are included when building the catalog's build NodeJS
    /// package, as {"package-name": "version"}. I.e. {"moment": "^2.24"}.
    ///
    /// Version strings can take any form understood by NPM.
    /// See https://docs.npmjs.com/files/package.json#dependencies
    #[serde(default)]
    #[schemars(default = "Catalog::default_node_dependencies")]
    pub npm_dependencies: BTreeMap<String, String>,
    /// # Collections defined by this Catalog.
    #[serde(default)]
    #[schemars(example = "Catalog::example_collections")]
    pub collections: BTreeMap<Collection, CollectionDef>,
    /// # Materializations of this Catalog.
    #[serde(default)]
    pub materializations: BTreeMap<Materialization, MaterializationDef>,
    /// # Captures of this Catalog.
    #[serde(default)]
    pub captures: BTreeMap<Capture, CaptureDef>,
    // Tests of the catalog, indexed by name.
    #[serde(default)]
    #[schemars(default = "Catalog::default_test")]
    #[schemars(example = "Catalog::example_test")]
    pub tests: BTreeMap<Test, Vec<TestStep>>,
    // # Storage mappings of this Catalog.
    #[serde(default)]
    #[schemars(example = "StorageMapping::example")]
    pub storage_mappings: Vec<StorageMapping>,
}

impl Catalog {
    /// Build a root JSON schema for the Catalog model.
    pub fn root_json_schema() -> schemars::schema::RootSchema {
        let settings = schemars::gen::SchemaSettings::draft2019_09();
        let generator = schemars::gen::SchemaGenerator::new(settings);
        generator.into_root_schema_for::<Self>()
    }

    fn default_node_dependencies() -> BTreeMap<String, String> {
        from_value(json!({"a-npm-package": "^1.2.3"})).unwrap()
    }
    fn default_test() -> Value {
        json!({"Test that fob quips ipsum": []})
    }
    fn example_collections() -> BTreeMap<Collection, CollectionDef> {
        vec![(Collection::example(), CollectionDef::example())]
            .into_iter()
            .collect()
    }
    fn example_test() -> Value {
        json!({
            "Test that fob quips ipsum": [
                TestStep::example_ingest(),
                TestStep::example_verify(),
            ]
        })
    }
}
