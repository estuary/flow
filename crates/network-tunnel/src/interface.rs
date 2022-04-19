use super::networktunnel::NetworkTunnel;
use super::sshforwarding::{SshForwarding, SshForwardingConfig};

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Serialize, Deserialize, JsonSchema, Debug, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields, remote = "Self")]
pub enum NetworkTunnelConfig {
    SshForwarding(SshForwardingConfig),
}

// There is a useless "tunnelType" field in the JSON representation of this enum
// This deserialize implementation gets rid of that field without
// complicating the data structure. See https://github.com/serde-rs/serde/issues/1343
impl<'de> Deserialize<'de> for NetworkTunnelConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Wrapper {
            #[serde(rename = "tunnelType")]
            _ignore: Option<String>,
            #[serde(flatten, with = "NetworkTunnelConfig")]
            inner: NetworkTunnelConfig,
        }
        Wrapper::deserialize(deserializer).map(|w| w.inner)
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
    fn test_network_config_parse_with_tunnel_type() {
        let config = "{
            \"tunnelType\": \"sshForwarding\",
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
