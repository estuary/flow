use super::RawValue;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Connector image and configuration specification.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
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
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
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
