use std::collections::HashMap;

use ipnetwork::IpNetwork;

/// Image is the object returned by `docker inspect` over an image.
#[derive(Debug, serde::Deserialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Image {
    pub config: ImageConfig,
    pub repo_tags: Vec<String>,
}

#[derive(Debug, serde::Deserialize, Clone, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ImageConfig {
    pub cmd: Option<Vec<String>>,
    pub entrypoint: Option<Vec<String>>,
    pub labels: HashMap<String, String>,
    #[serde(rename = "env")]
    _env: Vec<String>,
    #[serde(skip)]
    pub env: HashMap<String, String>,
    pub working_dir: Option<String>,
    pub user: Option<String>,
}

impl Image {
    pub fn parse_from_json_file(path: &str) -> anyhow::Result<Self> {
        let [mut out] = serde_json::from_slice::<[Image; 1]>(&std::fs::read(path)?)?;
        out.config.env = out
            .config
            ._env
            .iter()
            .map(|e| {
                let mut splitted = e.splitn(2, "=");
                (
                    splitted.next().unwrap().to_owned(),
                    splitted.next().unwrap().to_owned(),
                )
            })
            .collect();
        Ok(out)
    }

    /// Find the arguments required to invoke the connector,
    /// as indicated by either an ENTRYPOINT or CMD of the Dockerfile.
    pub fn get_argv(&self) -> anyhow::Result<Vec<String>> {
        if let Some(a) = &self.config.entrypoint {
            Ok(a.clone())
        } else if let Some(a) = &self.config.cmd {
            Ok(a.clone())
        } else {
            anyhow::bail!("image config has neither entrypoint nor cmd")
        }
    }
}

#[derive(serde::Deserialize, Debug, Default)]
pub struct GuestConfig {
    pub ip_configs: Option<Vec<IPConfig>>,
    pub hostname: String,

    pub root_device: Option<String>,

    pub etc_resolv: Option<EtcResolv>,
    pub etc_hosts: Option<Vec<EtcHost>>,
}

impl GuestConfig {
    pub fn parse_from_json_file(path: &str) -> anyhow::Result<Self> {
        let [out] = serde_json::from_slice::<[GuestConfig; 1]>(&std::fs::read(path)?)?;
        Ok(out)
    }
}

#[derive(serde::Deserialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct IPConfig {
    pub gateway: IpNetwork,
    #[serde(rename = "IP")]
    pub ip: IpNetwork,
    pub mask: u8,
}

#[derive(serde::Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Mount {
    pub mount_path: String,
    pub device_path: String,
}

#[derive(serde::Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct EtcHost {
    pub host: String,
    #[serde(rename = "IP")]
    pub ip: String,
    pub desc: Option<String>,
}

#[derive(serde::Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct EtcResolv {
    pub nameservers: Vec<String>,
}
