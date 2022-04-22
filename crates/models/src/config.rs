use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json, value::RawValue};

use super::{Object, RelativeUrl};

/// A configuration which is either defined inline, or is a relative or
/// absolute URI to a configuration file.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(untagged)]
#[schemars(example = "Config::example_absolute")]
#[schemars(example = "Config::example_relative")]
#[schemars(example = "Config::example_inline")]
pub enum Config {
    /// Relative URL to a configuration file.
    Url(RelativeUrl),
    /// Inline configuration.
    Inline(Object),
}

impl Config {
    pub fn example_absolute() -> Self {
        from_value(json!("http://example/config")).unwrap()
    }
    pub fn example_relative() -> Self {
        from_value(json!("../path/to/config.yaml")).unwrap()
    }
    pub fn example_inline() -> Self {
        from_value(json!({
            "config_key": "value",
        }))
        .unwrap()
    }
}

/// Connector image and configuration specification.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct ConnectorConfig {
    /// # Image of the connector.
    pub image: String,
    /// # Configuration of the connector.
    #[schemars(schema_with = "Config::json_schema")]
    pub config: Box<RawValue>,
}

impl ConnectorConfig {
    pub fn example() -> Self {
        Self {
            image: "connector/image:tag".to_string(),
            config: RawValue::from_string(
                serde_json::to_string(&Config::Url(RelativeUrl::new("connector-config.yaml")))
                    .unwrap(),
            )
            .unwrap(),
        }
    }
}
