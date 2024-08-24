use std::collections::HashMap;

/// Image is the object returned by `docker inspect` over an image.
#[derive(Debug, serde::Deserialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Image {
    pub config: ImageConfig,
}

#[derive(Debug, serde::Deserialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct ImageConfig {
    pub cmd: Option<Vec<String>>,
    pub entrypoint: Option<Vec<String>>,
    pub labels: HashMap<String, String>,
    pub env: Vec<String>,
}

impl Image {
    pub fn parse_from_json_file(path: &str) -> anyhow::Result<Self> {
        let [out] = serde_json::from_slice::<[Image; 1]>(&std::fs::read(path)?)?;
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

    /// Fetch the value of a LABEL or ENV of the image which has the given name.
    pub fn get_label_or_env(&self, name: &str) -> Option<&str> {
        if let Some(value) = self.config.labels.get(name) {
            return Some(value);
        }
        let env_prefix = format!("{name}=");

        for env in self.config.env.iter() {
            if env.starts_with(&env_prefix) {
                return Some(&env[env_prefix.len()..]);
            }
        }
        None
    }
}
