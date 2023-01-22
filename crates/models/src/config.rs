use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json, value::RawValue};
use std::collections::BTreeMap;

use super::{Object, PortName, RelativeUrl};

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

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortSpec {
    /// The port number that your connector will listen on
    pub port: u16,
    /// The ALPN protocol to associate with this port
    ///
    /// Many use cases will work without specifying a protocol at all, because
    /// a Flow data-plane will accept whichever procotol the client requests (or not) by default.
    /// But exposing HTTP services will typically require specifying either `http/1.1` or `h2`,
    /// as appropriate.
    pub protocol: Option<String>,
}

fn ports_json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    from_value(json!({
        "type": "object",
        "patternProperties": {
            PortName::schema_pattern(): PortSpec::json_schema(gen),
        },
        "additionalProperties": false,
    }))
    .unwrap()
}

/// Connector image and configuration specification.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct ConnectorConfig {
    /// # Image of the connector.
    pub image: String,
    /// # Configuration of the connector.
    #[schemars(schema_with = "Config::json_schema")]
    pub config: Box<RawValue>,
    /// # Network ports to expose to the internet
    #[schemars(schema_with = "ports_json_schema")]
    pub ports: BTreeMap<PortName, PortSpec>,
}

impl ConnectorConfig {
    pub fn example() -> Self {
        let mut ex_ports = BTreeMap::new();
        ex_ports.insert(
            PortName::new("http"),
            PortSpec {
                port: 80,
                protocol: None,
            },
        );
        Self {
            image: "connector/image:tag".to_string(),
            config: RawValue::from_string(
                serde_json::to_string(&Config::Url(RelativeUrl::new("connector-config.yaml")))
                    .unwrap(),
            )
            .unwrap(),
            ports: ex_ports,
        }
    }
}
