use core::fmt;
use std::{fmt::Display, str::FromStr};

use anyhow::{anyhow, Context};
use fancy_regex::Regex;
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

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum PortMappingProtocol {
    TCP,
    UDP,
}

impl fmt::Display for PortMappingProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let val = serde_plain::to_string(self).unwrap_or("unknown".to_owned());
        write!(f, "{}", val)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PortMapping {
    pub host_port: usize,
    pub container_port: usize,
    pub protocol: PortMappingProtocol,
}

lazy_static::lazy_static! {
    static ref PORT_RE: Regex = Regex::new(r"^([0-9]{1,5}):([0-9]{1,5})(/(\w+))?$").unwrap();
}

impl FromStr for PortMapping {
    type Err = anyhow::Error;

    /// https://docs.docker.com/config/containers/container-networking/#published-ports
    /// 8080:80 - Map TCP port 80 in the guest to port 8080 on the host.
    /// 8080:80/udp - Map UDP port 80 in the guest to port 8080 on the host.
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let capture_groups = PORT_RE
            .captures_iter(s)
            .next()
            .context(format!("Invalid port mapping {s}"))?
            .context(format!("Invalid port mapping {s}"))?;

        let host_port = capture_groups
            .get(1)
            .context(format!("Missing host port in port mapping: {s}"))?
            .as_str()
            .parse::<usize>()?;
        let guest_port = capture_groups
            .get(2)
            .context(format!("Missing guest port in port mapping: {s}"))?
            .as_str()
            .parse::<usize>()?;
        let protocol = capture_groups
            .get(4)
            .map(|proto| serde_plain::from_str(proto.as_str()))
            .unwrap_or(Ok(PortMappingProtocol::TCP))?;

        Ok(Self {
            host_port,
            container_port: guest_port,
            protocol,
        })
    }
}
