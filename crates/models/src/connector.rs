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

/// Connectors with an image name starting with this value are Dekaf-type materializations. No image with this
/// name exists, instead we use it to identify which connectors get marked as `connector_type: ConnectorType::Dekaf`,
/// causing the runtime to invoke Dekaf's in-tree connector logic in `[dekaf::connector]`
pub const DEKAF_IMAGE_NAME_PREFIX: &str = "ghcr.io/estuary/dekaf-";

/// Dekaf doesn't use images, but important information such as endpoint/resource config schema are associated
/// with a particular `connector_tags` row. Rather than refactoring this deeply interconnected piece of the system,
/// we've decided to just give Dekaf a `connector_tags` row. This is its tag.
pub const DEKAF_IMAGE_TAG: &str = ":v1";

/// Dekaf service configuration
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
pub struct DekafConfig {
    /// # Dekaf variant type.
    /// Since we support integrating with a bunch of different providers via Dekaf,
    /// this allows us to store which of those connector variants this particular Dekaf connector was
    /// created as, in order to e.g link to the correct docs URL, show the correct name and logo, etc.
    pub variant: String,
    /// # Dekaf endpoint config.
    pub config: RawValue,
}

impl DekafConfig {
    pub fn image_name(&self) -> String {
        format!("{DEKAF_IMAGE_NAME_PREFIX}{}{DEKAF_IMAGE_TAG}", self.variant)
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
