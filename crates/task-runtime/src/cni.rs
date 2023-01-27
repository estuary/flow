use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Interface {
    pub name: String,
    pub mac: Option<String>,
    pub sandbox: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct IPConfig {
    pub interface: Option<usize>,
    pub address: ipnetwork::IpNetwork,
    pub gateway: std::net::IpAddr,
}

#[derive(Serialize, Deserialize)]
pub struct Result {
    #[serde(rename = "cniVersion")]
    pub cni_version: String,
    pub interfaces: Vec<Interface>,
    pub ips: Vec<IPConfig>,
}
