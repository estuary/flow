use super::networktunnel::NetworkTunnel;
use super::sshforwarding::{SshForwarding, SshForwardingConfig};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
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
