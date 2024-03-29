use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use std::collections::BTreeMap;

use super::{
    Capture, CaptureDef, Collection, CollectionDef, Materialization, MaterializationDef, Prefix,
    RelativeUrl, StorageDef, Test, TestDef, TestStep, TransformDef,
};

// TODO: is BaseCatalog worth keeping?
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BaseCatalog {
    /// # Captures of this Catalog.
    #[schemars(schema_with = "captures_schema")]
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub captures: BTreeMap<Capture, CaptureDef>,
    /// # Collections of this Catalog.
    #[schemars(schema_with = "collections_schema")]
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub collections: BTreeMap<Collection, CollectionDef>,
    /// # Materializations of this Catalog.
    #[schemars(schema_with = "materializations_schema")]
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub materializations: BTreeMap<Materialization, MaterializationDef>,
    /// # Tests of this Catalog.
    #[schemars(schema_with = "tests_schema")]
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tests: BTreeMap<Test, TestDef>,
}

impl BaseCatalog {
    pub fn merge(&mut self, other: BaseCatalog) {
        let BaseCatalog {
            captures,
            collections,
            materializations,
            tests,
        } = other;
        for (name, spec) in captures {
            self.captures.insert(name, spec);
        }
        for (name, spec) in collections {
            self.collections.insert(name, spec);
        }
        for (name, spec) in materializations {
            self.materializations.insert(name, spec);
        }
        for (name, spec) in tests {
            self.tests.insert(name, spec);
        }
    }

    /// Returns the names of all specs that are directly included within this catalog.
    /// This does _not_ include specs from imported catalogs.
    pub fn all_spec_names(&self) -> impl Iterator<Item = &str> {
        self.collections
            .keys()
            .map(AsRef::<str>::as_ref)
            .chain(self.captures.keys().map(AsRef::<str>::as_ref))
            .chain(self.materializations.keys().map(AsRef::<str>::as_ref))
            .chain(self.tests.keys().map(AsRef::<str>::as_ref))
    }
}

impl From<Catalog> for BaseCatalog {
    fn from(value: Catalog) -> Self {
        let Catalog {
            captures,
            collections,
            materializations,
            tests,
            ..
        } = value;
        BaseCatalog {
            captures,
            collections,
            materializations,
            tests,
        }
    }
}

/// Each catalog source defines a portion of a Flow Catalog, by defining
/// collections, derivations, tests, and materializations of the Catalog.
/// Catalog sources may reference and import other sources, in order to
/// collections and other entities that source defines.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Catalog {
    /// # JSON-Schema against which the Catalog is validated.
    #[serde(default, rename = "$schema", skip_serializing_if = "Option::is_none")]
    pub _schema: Option<String>,
    /// # Import other Flow catalog sources.
    /// By importing another Flow catalog source, its collections, schemas, and derivations
    /// are bundled into the publication context of this specification.
    /// Imports are relative or absolute URLs, relative to this specification's location.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub import: Vec<RelativeUrl>,
    /// # Captures of this Catalog.
    #[schemars(schema_with = "captures_schema")]
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub captures: BTreeMap<Capture, CaptureDef>,
    /// # Collections of this Catalog.
    #[schemars(schema_with = "collections_schema")]
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub collections: BTreeMap<Collection, CollectionDef>,
    /// # Materializations of this Catalog.
    #[schemars(schema_with = "materializations_schema")]
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub materializations: BTreeMap<Materialization, MaterializationDef>,
    /// # Tests of this Catalog.
    #[schemars(schema_with = "tests_schema")]
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tests: BTreeMap<Test, TestDef>,
    // # Storage mappings of this Catalog.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(skip)]
    pub storage_mappings: BTreeMap<Prefix, StorageDef>,
}

impl From<BaseCatalog> for Catalog {
    fn from(value: BaseCatalog) -> Self {
        let BaseCatalog {
            captures,
            collections,
            materializations,
            tests,
        } = value;
        Catalog {
            captures,
            collections,
            materializations,
            tests,
            _schema: None,
            import: Vec::new(),
            storage_mappings: BTreeMap::new(),
        }
    }
}

impl Catalog {
    /// Build a root JSON schema for the Catalog model.
    pub fn root_json_schema() -> schemars::schema::RootSchema {
        let mut settings = schemars::gen::SchemaSettings::draft2019_09();
        settings.option_add_null_type = false;

        let generator = schemars::gen::SchemaGenerator::new(settings);
        let mut root = generator.into_root_schema_for::<Self>();

        TransformDef::patch_schema(
            root.definitions
                .get_mut(&TransformDef::schema_name())
                .unwrap(),
        );

        root
    }

    /// Returns the names of all specs that are directly included within this catalog.
    /// This does _not_ include specs from imported catalogs.
    pub fn all_spec_names(&self) -> impl Iterator<Item = &str> {
        self.collections
            .keys()
            .map(AsRef::<str>::as_ref)
            .chain(self.captures.keys().map(AsRef::<str>::as_ref))
            .chain(self.materializations.keys().map(AsRef::<str>::as_ref))
            .chain(self.tests.keys().map(AsRef::<str>::as_ref))
    }

    /// Returns the total number of specifications that are directly included in this catalog.
    /// This does not include storage mappings or any specs that may be indirectly included via `import`.
    pub fn spec_count(&self) -> usize {
        self.collections.len()
            + self.captures.len()
            + self.materializations.len()
            + self.tests.len()
    }

    /// Returns true if this catalog does not contain any specs.
    pub fn is_empty(&self) -> bool {
        self.spec_count() == 0
    }
}

fn collections_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    let schema = Collection::json_schema(gen);
    gen.definitions_mut()
        .insert(Collection::schema_name(), schema);

    let mut schema = CollectionDef::json_schema(gen);

    // Extend CollectionDef schema with a oneOf which requires either schema or readSchema / writeSchema.
    let schemars::schema::Schema::Object(schema_obj) = &mut schema else {
        panic!("must be a schema object")
    };
    schema_obj.subschemas().one_of = Some(vec![
        from_value(json!({
            "required": ["schema"],
            "properties": { "readSchema": false, "writeSchema": false },
        }))
        .unwrap(),
        from_value(json!({
            "required": ["readSchema", "writeSchema"],
            "properties": { "schema": false },
        }))
        .unwrap(),
    ]);

    gen.definitions_mut()
        .insert(CollectionDef::schema_name(), schema);

    from_value(json!({
        "type": "object",
        "patternProperties": {
            Collection::schema_pattern(): {
                "$ref": format!("#/definitions/{}", CollectionDef::schema_name()),
            },
        },
        "additionalProperties": false,
        "examples": [{
            Collection::example().as_str(): CollectionDef::example()
        }],
    }))
    .unwrap()
}

fn captures_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    let schema = Capture::json_schema(gen);
    gen.definitions_mut().insert(Capture::schema_name(), schema);

    let schema = CaptureDef::json_schema(gen);
    gen.definitions_mut()
        .insert(CaptureDef::schema_name(), schema);

    from_value(json!({
        "type": "object",
        "patternProperties": {
            Capture::schema_pattern(): {
                "$ref": format!("#/definitions/{}", CaptureDef::schema_name()),
            },
        },
        "additionalProperties": false,
        "examples": [{
            Capture::example().as_str(): CaptureDef::example()
        }],
    }))
    .unwrap()
}

fn materializations_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    let schema = Materialization::json_schema(gen);
    gen.definitions_mut()
        .insert(Materialization::schema_name(), schema);

    let schema = MaterializationDef::json_schema(gen);
    gen.definitions_mut()
        .insert(MaterializationDef::schema_name(), schema);

    from_value(json!({
        "type": "object",
        "patternProperties": {
            Materialization::schema_pattern(): {
                "$ref": format!("#/definitions/{}", MaterializationDef::schema_name()),
            },
        },
        "additionalProperties": false,
        "examples": [{
            Materialization::example().as_str(): MaterializationDef::example()
        }],
    }))
    .unwrap()
}

fn tests_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    let schema = Test::json_schema(gen);
    gen.definitions_mut().insert(Test::schema_name(), schema);

    let schema = TestDef::json_schema(gen);
    gen.definitions_mut().insert(TestDef::schema_name(), schema);

    from_value(json!({
        "type": "object",
        "patternProperties": {
            Test::schema_pattern(): {
                "$ref": format!("#/definitions/{}", TestDef::schema_name()),
            },
        },
        "additionalProperties": false,
        "examples": [{
            Test::example().as_str(): [
                TestStep::example_ingest(),
                TestStep::example_verify(),
            ],
        }],
    }))
    .unwrap()
}
