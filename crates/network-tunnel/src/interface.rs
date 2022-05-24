use super::networktunnel::NetworkTunnel;
use super::sshforwarding::{SshForwarding, SshForwardingConfig};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum NetworkTunnelConfig {
    SshForwarding(SshForwardingConfig),
}

// manually implement JsonSchema so that we can add the 'advanced' annotation, which causes this
// configuration form to be collapsed by default in the UI.
impl JsonSchema for NetworkTunnelConfig {
    fn schema_name() -> String {
        "NetworkTunnelConfig".to_owned()
    }

    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        let ssh_forwarding = gen.subschema_for::<SshForwardingConfig>();
        serde_json::from_value(serde_json::json!({
            "title": "Network Tunneling",
            "description": "Setup a network tunnel to access systems on a private network",
            "advanced": true,
            "oneOf": [ssh_forwarding],
        }))
        .unwrap()
    }
}

impl NetworkTunnelConfig {
    pub fn new_tunnel(self) -> Box<dyn NetworkTunnel> {
        match self {
            NetworkTunnelConfig::SshForwarding(config) => Box::new(SshForwarding::new(config)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn network_tunnel_config_has_advanced_annotation() {
        let schema = schemars::schema_for!(NetworkTunnelConfig);
        // assert on the serialized form of the schema, to prove that it's actually serialized
        let as_json = serde_json::to_value(schema.clone()).unwrap();
        assert_eq!(
            Some(&serde_json::Value::Bool(true)),
            as_json.pointer("/advanced"),
            "actual: {}",
            as_json,
        );

        assert!(as_json.pointer("/oneOf").is_some());
        assert!(as_json.pointer("/oneOf").unwrap().is_array());
        assert_eq!(
            as_json.pointer("/oneOf").unwrap().as_array().unwrap().len(),
            1
        );
    }

    #[test]
    fn test_network_config_parse_without_tunnel_type() {
        let config = "{
            \"sshForwarding\": {
                \"sshEndpoint\": \"ssh://localhost:2222\",
                \"user\": \"flow\",
                \"forwardHost\": \"localhost\",
                \"forwardPort\": 5432,
                \"privateKey\": \"\",
                \"localPort\": 5432
            }
        }";

        let result: Result<NetworkTunnelConfig, _> = serde_json::from_str(config);

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            NetworkTunnelConfig::SshForwarding(SshForwardingConfig {
                ssh_endpoint: "ssh://localhost:2222".to_string(),
                user: "flow".to_string(),
                forward_host: "localhost".to_string(),
                forward_port: 5432,
                private_key: "".to_string(),
                local_port: 5432
            })
        );
    }
}
