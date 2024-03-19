use super::{Collection, RawValue, Source};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TestDef(pub Vec<TestStep>);

impl JsonSchema for TestDef {
    fn schema_name() -> String {
        String::from("TestDef")
    }

    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        let schema = TestStep::json_schema(gen);
        gen.definitions_mut()
            .insert(TestStep::schema_name(), schema);

        from_value(json!({
            "type": "array",
            "items": {
                "$ref": format!("#/definitions/{}", TestStep::schema_name()),
            },
            "examples": [TestDef::example()],
        }))
        .unwrap()
    }
}

impl std::ops::Deref for TestDef {
    type Target = Vec<TestStep>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for TestDef {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TestDef {
    pub fn example() -> Self {
        TestDef(vec![TestStep::example_ingest(), TestStep::example_verify()])
    }
}

/// A test step describes either an "ingest" of document fixtures into a
/// collection, or a "verify" of expected document fixtures from a collection.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[schemars(example = "TestDocuments::example_relative")]
#[schemars(example = "TestDocuments::example_inline")]
pub struct TestDocuments(RawValue);

impl std::ops::Deref for TestDocuments {
    type Target = RawValue;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for TestDocuments {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TestDocuments {
    pub fn example_relative() -> Self {
        from_value(json!("../path/to/test-documents.json")).unwrap()
    }
    pub fn example_inline() -> Self {
        from_value(json!([
            {"a": "document"},
            {"another": "document"},
        ]))
        .unwrap()
    }
}

/// A test step describes either an "ingest" of document fixtures into a
/// collection, or a "verify" of expected document fixtures from a collection.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "TestStep::example_ingest")]
#[schemars(example = "TestStep::example_verify")]
pub enum TestStep {
    /// Ingest document fixtures into a collection.
    Ingest(TestStepIngest),
    /// Verify the contents of a collection match a set of document fixtures.
    Verify(TestStepVerify),
}

impl TestStep {
    pub fn example_ingest() -> Self {
        TestStep::Ingest(TestStepIngest::example())
    }
    pub fn example_verify() -> Self {
        TestStep::Verify(TestStepVerify::example())
    }
}

/// An ingestion test step ingests document fixtures into the named
/// collection.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "TestStepIngest::example")]
pub struct TestStepIngest {
    /// # Description of this test ingestion.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// # Name of the collection into which the test will ingest.
    pub collection: Collection,
    /// # Documents to ingest.
    /// Each document must conform to the collection's schema.
    pub documents: TestDocuments,
}

impl TestStepIngest {
    pub fn example() -> Self {
        Self {
            description: "Description of the ingestion.".to_string(),
            collection: Collection::example(),
            documents: TestDocuments::example_inline(),
        }
    }
}

/// A verification test step verifies that the contents of the named
/// collection match the expected fixtures, after fully processing all
/// preceding ingestion test steps.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "TestStepVerify::example")]
pub struct TestStepVerify {
    /// # Description of this test verification.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// # Collection from which the test will verify.
    pub collection: Source,
    /// # Documents to verify.
    /// Each document may contain only a portion of the matched document's
    /// properties, and any properties present in the actual document but
    /// not in this document fixture are ignored. All other values must
    /// match or the test will fail.
    pub documents: TestDocuments,
}

impl TestStepVerify {
    pub fn example() -> Self {
        Self {
            description: "Description of the verification.".to_string(),
            collection: Source::Collection(Collection::new("acmeCo/collection")),
            documents: TestDocuments::example_inline(),
        }
    }
}
