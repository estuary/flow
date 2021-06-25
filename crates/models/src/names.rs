use schemars::JsonSchema;
use serde::{de::Error as SerdeError, Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

// This module holds project-wide, type-safe wrappers, enums, and *very* simple
// structures which identify or name Flow concepts, and must be referenced from
// multiple different crates.

/// Collection names consist of Unicode letters, numbers, and symbols: - _ . /
///
/// Spaces and other special characters are disallowed.
#[derive(
    Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[schemars(example = "Collection::example")]
pub struct Collection(#[schemars(schema_with = "Collection::schema")] String);

impl Collection {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for Collection {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
#[schemars(example = "Transform::example")]
pub struct Transform(String);

impl Transform {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for Transform {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

/// Capture names a Flow capture.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
#[schemars(example = "Capture::example")]
pub struct Capture(String);

impl Capture {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for Capture {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

/// Materialization names a Flow materialization.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
#[schemars(example = "Materialization::example")]
pub struct Materialization(String);

impl Materialization {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for Materialization {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

/// Test names a Flow catalog test.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
#[schemars(example = "Test::example")]
pub struct Test(String);

impl Test {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for Test {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

/// Rule names a specification rule.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
#[schemars(example = "Rule::example")]
pub struct Rule(String);

impl Rule {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

impl std::ops::Deref for Rule {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

/// JSON Pointer which identifies a location in a document.
#[derive(Serialize, Debug, Clone, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
#[schemars(example = "JsonPointer::example")]
pub struct JsonPointer(#[schemars(schema_with = "JsonPointer::schema")] String);

impl JsonPointer {
    pub fn new(ptr: impl Into<String>) -> Self {
        Self(ptr.into())
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

impl<'de> Deserialize<'de> for JsonPointer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;

        if !s.is_empty() && !s.starts_with("/") {
            Err(D::Error::custom(
                "non-empty JSON pointer must begin with '/'",
            ))
        } else {
            Ok(JsonPointer(s))
        }
    }
}

/// Ordered JSON-Pointers which define how a composite key may be extracted from
/// a collection document.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
#[schemars(example = "CompositeKey::example")]
pub struct CompositeKey(Vec<JsonPointer>);

impl CompositeKey {
    pub fn new(parts: impl Into<Vec<JsonPointer>>) -> Self {
        Self(parts.into())
    }
    pub fn example() -> Self {
        CompositeKey(vec![JsonPointer::example()])
    }
}

impl std::ops::Deref for CompositeKey {
    type Target = Vec<JsonPointer>;

    fn deref(&self) -> &Vec<JsonPointer> {
        &self.0
    }
}

/// Object is an alias for a JSON object.
pub type Object = serde_json::Map<String, Value>;

/// Lambdas are user functions which are invoked by the Flow runtime to
/// process and transform source collection documents into derived collections.
/// Flow supports multiple lambda run-times, with a current focus on TypeScript
/// and remote HTTP APIs.
///
/// TypeScript lambdas are invoked within on-demand run-times, which are
/// automatically started and scaled by Flow's task distribution in order
/// to best co-locate data and processing, as well as to manage fail-over.
///
/// Remote lambdas may be called from many Flow tasks, and are up to the
/// API provider to provision and scale.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "Lambda::example_typescript")]
#[schemars(example = "Lambda::example_remote")]
pub enum Lambda {
    Typescript,
    Remote(String),
}

/// Partition selectors identify a desired subset of the
/// available logical partitions of a collection.
#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "PartitionSelector::example")]
pub struct PartitionSelector {
    /// Partition field names and corresponding values which must be matched
    /// from the Source collection. Only documents having one of the specified
    /// values across all specified partition names will be matched. For example,
    ///   source: [App, Web]
    ///   region: [APAC]
    /// would mean only documents of 'App' or 'Web' source and also occurring
    /// in the 'APAC' region will be processed.
    #[serde(default)]
    pub include: BTreeMap<String, Vec<Value>>,
    /// Partition field names and values which are excluded from the source
    /// collection. Any documents matching *any one* of the partition values
    /// will be excluded.
    #[serde(default)]
    pub exclude: BTreeMap<String, Vec<Value>>,
}
