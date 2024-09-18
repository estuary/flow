use super::RawValue;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Splits a full connector image name into separate image and tag components.
/// The resulting tag will always begin with either a `@sha256:` or `:` if a
/// tag is present. Otherwise, the tag will be an empty string.
pub fn split_image_tag(image_full: &str) -> (String, String) {
    let mut image = image_full.to_string();

    if let Some(pivot) = image.find("@sha256:").or_else(|| image.find(":")) {
        let tag = image.split_off(pivot);
        (image, tag)
    } else {
        (image, String::new())
    }
}

/// Connector image and configuration specification.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
pub struct ConnectorConfig {
    /// # Image of the connector.
    pub image: String,
    /// # Configuration of the connector.
    pub config: RawValue,
}

impl ConnectorConfig {
    pub fn example() -> Self {
        Self {
            image: "connector/image:tag".to_string(),
            config: serde_json::from_str("\"connector-config.yaml\"").unwrap(),
        }
    }
}

/// Local command and its configuration.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
pub struct LocalConfig {
    /// # Command to execute
    pub command: Vec<String>,
    /// # Configuration of the command.
    pub config: RawValue,
    /// # Environment variables
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// # Use protobuf codec instead of JSON.
    #[serde(default, skip_serializing_if = "super::is_false")]
    pub protobuf: bool,
}

impl LocalConfig {
    pub fn example() -> Self {
        Self {
            command: vec![
                "my-connector".to_string(),
                "--arg=one".to_string(),
                "--arg=two".to_string(),
            ],
            config: serde_json::from_value(serde_json::json!({"field": "value", "otherField": 42}))
                .unwrap(),
            env: BTreeMap::new(),
            protobuf: false,
        }
    }
}

/// Dekaf configuration. Currently empty, but present to enable easy addition
/// of config options when they show up in the future.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
pub struct DekafConfig {}

impl DekafConfig {
    pub fn example() -> Self {
        Self {}
    }
}
