use std::collections::HashMap;

use ipnetwork::IpNetwork;
use serde::{ser::SerializeSeq, Deserialize, Serialize};

/// Image is the object returned by `docker inspect` over an image.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Image {
    pub config: ImageConfig,
    pub repo_tags: Vec<String>,
}

fn deserialize_env_var<'de, D>(deserializer: D) -> Result<HashMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let buf = Vec::<String>::deserialize(deserializer)?;

    let res = buf
        .iter()
        .map(|e| {
            let mut splitted = e.splitn(2, "=");
            (
                splitted.next().unwrap().to_owned(),
                splitted.next().unwrap().to_owned(),
            )
        })
        .collect();

    Ok(res)
}

fn serialize_env_var<S>(x: &HashMap<String, String>, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let mut seq = s.serialize_seq(Some(x.len()))?;
    for (key, val) in x.iter() {
        seq.serialize_element(&format!("{}={}", key, val))?;
    }
    seq.end()
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ImageConfig {
    pub cmd: Option<Vec<String>>,
    pub entrypoint: Option<Vec<String>>,
    pub labels: HashMap<String, String>,
    #[serde(
        serialize_with = "serialize_env_var",
        deserialize_with = "deserialize_env_var"
    )]
    pub env: HashMap<String, String>,
    pub working_dir: Option<String>,
    pub user: Option<String>,
}

impl Image {
    pub fn parse_from_json_file(path: &str) -> anyhow::Result<Self> {
        let [mut out] = serde_json::from_slice::<[Image; 1]>(&std::fs::read(path)?)?;
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

#[derive(Serialize, Deserialize, Debug, Default, Builder)]
pub struct GuestConfig {
    #[builder(setter(into, strip_option))]
    pub ip_configs: Option<Vec<IPConfig>>,
    pub hostname: String,

    #[builder(setter(into, strip_option))]
    pub root_device: Option<String>,

    #[builder(setter(into, strip_option))]
    pub etc_resolv: Option<EtcResolv>,
    #[builder(setter(into, strip_option))]
    pub etc_hosts: Option<Vec<EtcHost>>,
}

impl GuestConfig {
    pub fn parse_from_json_file(path: &str) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(&std::fs::read(path)?)?)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct IPConfig {
    pub gateway: IpNetwork,
    #[serde(rename = "IP")]
    pub ip: IpNetwork,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Mount {
    pub mount_path: String,
    pub device_path: String,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct EtcHost {
    pub host: String,
    #[serde(rename = "IP")]
    pub ip: String,
    pub desc: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct EtcResolv {
    pub nameservers: Vec<String>,
}
