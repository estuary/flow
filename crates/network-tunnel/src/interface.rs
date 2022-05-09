use super::networktunnel::NetworkTunnel;
use super::sshforwarding::{SshForwarding, SshForwardingConfig};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JsonSchema, Debug, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(
    title = "Network Tunneling",
    description = "Connect to systems on a private network using tunneling."
)]
pub enum NetworkTunnelConfig {
    SshForwarding(SshForwardingConfig),
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
