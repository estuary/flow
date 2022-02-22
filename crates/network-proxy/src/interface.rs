use super::networkproxy::NetworkProxy;
use super::sshforwarding::{SshForwarding, SshForwardingConfig};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub enum NetworkProxyConfig {
    SshForwarding(SshForwardingConfig),
}

impl NetworkProxyConfig {
    pub fn new_proxy(self) -> Box<dyn NetworkProxy> {
        match self {
            NetworkProxyConfig::SshForwarding(config) => Box::new(SshForwarding::new(config)),
        }
    }
}
