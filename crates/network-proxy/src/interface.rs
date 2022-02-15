use super::sshforwarding::{SshForwardingConfig, SshForwarding};
use super::networkproxy::NetworkProxy;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub enum NetworkProxyConfig {
    SshForwarding(SshForwardingConfig)
}

impl NetworkProxyConfig {
  pub fn new_proxy(self) -> Box<dyn NetworkProxy> {
        match self {
            NetworkProxyConfig::SshForwarding(config) =>
                Box::new(SshForwarding::new(config))
        }
    }
}