use schemars::{schema, JsonSchema};
use serde::{Deserialize, Serialize};
use serde_json::{from_value as from_json_value, json};

pub use protocol::flow::shuffle::Hash as ShuffleHash;

/// EndpointType enumerates the endpoint types understood by Flow.
#[derive(Copy, Debug, Clone)]
pub enum EndpointType {
    Postgres,
    Sqlite,
    S3,
}

impl EndpointType {
    pub const POSTGRES: &'static str = "postgres";
    pub const SQLITE: &'static str = "sqlite";
    pub const S3S: &'static str = "s3";

    pub fn as_str(&self) -> &str {
        match self {
            Self::Postgres => Self::POSTGRES,
            Self::Sqlite => Self::SQLITE,
            Self::S3 => Self::S3S,
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            Self::POSTGRES => Some(Self::Postgres),
            Self::SQLITE => Some(Self::Sqlite),
            Self::S3S => Some(Self::S3),
            _ => None,
        }
    }
}

/// ContentType enumerates resource content types understood by Flow.
#[derive(Copy, Debug, Clone)]
pub enum ContentType {
    CatalogSpec,
    JsonSchema,
    NpmPack,
}

impl ContentType {
    pub const CATALOG_SPEC: &'static str = "application/vnd.estuary.dev-catalog-spec+yaml";
    pub const JSON_SCHEMA: &'static str = "application/schema+yaml";
    pub const NPM_PACK: &'static str = "application/vnd.estuary.dev-catalog-npm-pack";

    pub fn as_str(&self) -> &str {
        match self {
            Self::CatalogSpec => Self::CATALOG_SPEC,
            Self::JsonSchema => Self::JSON_SCHEMA,
            Self::NpmPack => Self::NPM_PACK,
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            Self::CATALOG_SPEC => Some(Self::CatalogSpec),
            Self::JSON_SCHEMA => Some(Self::JsonSchema),
            Self::NPM_PACK => Some(Self::NpmPack),
            _ => None,
        }
    }
}

/// Names consist of Unicode letters, numbers, and symbols: - _ . /
///
/// Spaces and other special characters are disallowed.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialOrd, PartialEq, Ord, Eq)]
#[schemars(example = "CollectionName::example")]
pub struct CollectionName(#[schemars(schema_with = "CollectionName::schema")] String);

impl CollectionName {
    pub fn new(name: impl Into<String>) -> CollectionName {
        CollectionName(name.into())
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

impl AsRef<str> for CollectionName {
    fn as_ref(&self) -> &str {
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

impl AsRef<str> for TransformName {
    fn as_ref(&self) -> &str {
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

impl AsRef<str> for EndpointName {
    fn as_ref(&self) -> &str {
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

impl AsRef<str> for MaterializationName {
    fn as_ref(&self) -> &str {
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

impl AsRef<str> for CaptureName {
    fn as_ref(&self) -> &str {
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

impl AsRef<str> for TestName {
    fn as_ref(&self) -> &str {
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

impl AsRef<str> for RelativeUrl {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<&str> for RelativeUrl {
    fn from(s: &str) -> Self {
        RelativeUrl(s.to_owned())
    }
}

impl RelativeUrl {
    pub fn example_relative() -> Self {
        Self("../path/to/local.yaml".to_owned())
    }
    pub fn example_absolute() -> Self {
        Self("https://example/resource".to_owned())
    }
}

/// JSON Pointer which identifies a location in a document.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[schemars(example = "JsonPointer::example")]
pub struct JsonPointer(#[schemars(schema_with = "JsonPointer::schema")] String);

impl AsRef<str> for JsonPointer {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl JsonPointer {
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
