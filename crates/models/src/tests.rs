use super::{Collection, Id, RawValue, Source};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};

/// Test the behavior of reductions and derivations, through a sequence of test steps.
#[derive(Serialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "TestDef::example")]
pub struct TestDef {
    /// # Description of this test.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// # Sequential steps of this test.
    pub steps: Vec<TestStep>,
    /// # Expected publication ID of this test within the control plane.
    /// When present, a publication of the test will fail if the
    /// last publication ID in the control plane doesn't match this value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect_pub_id: Option<Id>,
    /// # Delete this test within the control plane.
    /// When true, a publication will delete this test.
    #[serde(default, skip_serializing_if = "super::is_false")]
    pub delete: bool,
}

impl TestDef {
    pub fn example() -> Self {
        Self {
            description: "An example test".to_string(),
            steps: vec![TestStep::example_ingest(), TestStep::example_verify()],
            expect_pub_id: None,
            delete: false,
        }
    }
}

/// A test step describes either an "ingest" of document fixtures into a
/// collection, or a "verify" of expected document fixtures from a collection.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
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
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
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
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
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
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
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

impl super::ModelDef for TestDef {
    fn sources(&self) -> impl Iterator<Item = &crate::Source> {
        self.steps.iter().filter_map(|step| {
            let TestStep::Verify(verify) = step else {
                return None;
            };
            Some(&verify.collection)
        })
    }
    fn targets(&self) -> impl Iterator<Item = &crate::Collection> {
        self.steps.iter().filter_map(|step| {
            let TestStep::Ingest(ingest) = step else {
                return None;
            };
            Some(&ingest.collection)
        })
    }

    fn catalog_type(&self) -> crate::CatalogType {
        crate::CatalogType::Test
    }

    fn is_enabled(&self) -> bool {
        true // there's no way to disable a test
    }

    fn connector_image(&self) -> Option<&str> {
        None
    }
}

// TEMPORARY: support a custom deserializer that maps from the legacy array
// representation into a TestDef. We can remove this when all test models
// have been updated.

struct TestDefVisitor {}

impl<'de> serde::Deserialize<'de> for TestDef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(TestDefVisitor {})
    }
}

impl<'de> serde::de::Visitor<'de> for TestDefVisitor {
    type Value = TestDef;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("A TestDef object or a legacy array of TestSteps")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut steps: Vec<TestStep> = Vec::new();

        while let Some(step) = seq.next_element()? {
            steps.push(step)
        }

        Ok(TestDef {
            description: String::new(),
            steps,
            expect_pub_id: None,
            delete: false,
        })
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: serde::de::MapAccess<'de>,
    {
        let mut description = None;
        let mut steps = None;
        let mut expect_pub_id = None;
        let mut delete = None;

        const DESCRIPTION: &str = "description";
        const STEPS: &str = "steps";
        const EXPECT_PUB_ID: &str = "expectPubId";
        const DELETE: &str = "delete";

        // We must deserialize the key as an owned String, or else deserialization of a
        // `serde_json::Value` will fail, because it only uses owned Strings for keys.
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                DESCRIPTION => {
                    if description.is_some() {
                        return Err(serde::de::Error::duplicate_field(DESCRIPTION));
                    }
                    description = Some(map.next_value()?);
                }
                STEPS => {
                    if steps.is_some() {
                        return Err(serde::de::Error::duplicate_field(STEPS));
                    }
                    steps = Some(map.next_value()?);
                }
                EXPECT_PUB_ID => {
                    if expect_pub_id.is_some() {
                        return Err(serde::de::Error::duplicate_field(EXPECT_PUB_ID));
                    }
                    expect_pub_id = Some(map.next_value()?);
                }
                DELETE => {
                    if delete.is_some() {
                        return Err(serde::de::Error::duplicate_field(DELETE));
                    }
                    delete = Some(map.next_value()?);
                }
                _ => {
                    return Err(serde::de::Error::unknown_field(
                        key.as_str(),
                        &[DESCRIPTION, STEPS, EXPECT_PUB_ID, DELETE],
                    ))
                }
            }
        }

        Ok(TestDef {
            description: description.unwrap_or_default(),
            steps: steps.ok_or_else(|| serde::de::Error::missing_field(STEPS))?,
            expect_pub_id,
            delete: delete.unwrap_or_default(),
        })
    }
}
