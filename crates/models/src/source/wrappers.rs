use schemars::{schema, JsonSchema};
use serde::{Deserialize, Serialize};
use serde_json::{from_value as from_json_value, json};

pub use protocol::flow::shuffle::Hash as ShuffleHash;

/// EndpointType enumerates the endpoint types understood by Flow.
#[derive(Copy, Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum EndpointType {
    Postgres,
    Sqlite,
    S3,
    GS,
    Remote,
}

impl EndpointType {
    pub fn as_scheme(&self) -> &str {
        match self {
            Self::Postgres => "postgres",
            Self::Sqlite => "sqlite",
            Self::S3 => "s3",
            Self::GS => "gs",
            Self::Remote => "remote",
        }
    }
}

/// ContentType enumerates resource content types understood by Flow.
#[derive(Copy, Debug, Clone, Serialize, Deserialize)]
pub enum ContentType {
    CatalogSpec,
    JsonSchema,
    NpmPack,
}

/// Names consist of Unicode letters, numbers, and symbols: - _ . /
///
/// Spaces and other special characters are disallowed.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialOrd, PartialEq, Ord, Eq, Hash)]
#[schemars(example = "CollectionName::example")]
pub struct CollectionName(#[schemars(schema_with = "CollectionName::schema")] String);

impl CollectionName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
    pub fn example() -> Self {
        Self("a/collection".to_owned())
    }
    fn schema(_: &mut schemars::gen::SchemaGenerator) -> schema::Schema {
        from_json_value(json!({
            "type": "string",
            "pattern": "^[^ \t\n\\!@#$%^&*()+=\\<\\>?;:'\"\\[\\]\\|~`]+$",
        }))
        .unwrap()
    }
}

impl std::ops::Deref for CollectionName {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialOrd, PartialEq, Ord, Eq)]
#[schemars(example = "TransformName::example")]
pub struct TransformName(String);

impl TransformName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
    pub fn example() -> Self {
        Self("a transform".to_owned())
    }
}

impl std::ops::Deref for TransformName {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

/// EndpointName names a Flow endpoint.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialOrd, PartialEq, Ord, Eq)]
#[schemars(example = "TransformName::example")]
pub struct EndpointName(String);

impl EndpointName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
    pub fn example() -> Self {
        Self("an endpoint".to_owned())
    }
}

impl std::ops::Deref for EndpointName {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

/// MaterializationName names a Flow materialization.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialOrd, PartialEq, Ord, Eq)]
#[schemars(example = "TransformName::example")]
pub struct MaterializationName(String);

impl MaterializationName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
    pub fn example() -> Self {
        Self("a materialization".to_owned())
    }
}

impl std::ops::Deref for MaterializationName {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

/// CaptureName names a Flow capture.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialOrd, PartialEq, Ord, Eq)]
#[schemars(example = "TransformName::example")]
pub struct CaptureName(String);

impl CaptureName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
    pub fn example() -> Self {
        Self("a capture".to_owned())
    }
}

impl std::ops::Deref for CaptureName {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

/// TestName names a Flow catalog test.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialOrd, PartialEq, Ord, Eq)]
#[schemars(example = "TransformName::example")]
pub struct TestName(String);

impl TestName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
    pub fn example() -> Self {
        Self("a capture".to_owned())
    }
}

impl std::ops::Deref for TestName {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

/// A URL identifying a resource, which may be a relative local path
/// with respect to the current resource (i.e, ../path/to/flow.yaml),
/// or may be an external absolute URL (i.e., http://example/flow.yaml).
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[schemars(example = "RelativeUrl::example_relative")]
#[schemars(example = "RelativeUrl::example_absolute")]
pub struct RelativeUrl(String);

impl RelativeUrl {
    pub fn example_relative() -> Self {
        Self("../path/to/local.yaml".to_owned())
    }
    pub fn example_absolute() -> Self {
        Self("https://example/resource".to_owned())
    }
}

impl std::ops::Deref for RelativeUrl {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

/// JSON Pointer which identifies a location in a document.
#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone, PartialEq, Eq)]
#[schemars(example = "JsonPointer::example")]
pub struct JsonPointer(#[schemars(schema_with = "JsonPointer::schema")] String);

impl JsonPointer {
    pub fn new(ptr: impl Into<String>) -> Self {
        Self(ptr.into())
    }
    pub fn example() -> Self {
        Self("/json/ptr".to_owned())
    }
    fn schema(_: &mut schemars::gen::SchemaGenerator) -> schema::Schema {
        from_json_value(json!({
            "type": "string",
            "pattern": "^/.+",
        }))
        .unwrap()
    }
}

impl std::ops::Deref for JsonPointer {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for JsonPointer {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
