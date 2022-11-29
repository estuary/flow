use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Config {
    /// URL of the Flow UI which will be used when `flowctl` opens up a browser tab.
    pub dashboard_url: Option<url::Url>,
    /// ID of the current draft, or None if no draft is configured.
    pub draft: Option<String>,
    /// Configures how to communicate with the control plane API.
    /// This section is typically populated by running `flowctl auth` subcommands.
    pub api: Option<API>,
}

impl Config {
    /// Loads the config for the given named profile, returning the default if no config
    /// file exists for it. This expects to find the config directory at:
    /// - `$HOME/.config/flowctl` on linux
    /// - `$HOME/Library/Application Support/flowctl` on macos
    /// Within that directory, config files are named as `${profile}.json`.
    pub fn load(profile: &str) -> anyhow::Result<Config> {
        let config_file = Config::file_path(profile)?;

        match std::fs::read(&config_file) {
            Ok(v) => {
                let conf = serde_json::from_slice(&v).context("parsing config")?;
                tracing::debug!(profile = %profile, path = %config_file.display(), "loaded config from file");
                Ok(conf)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                tracing::debug!(profile = %profile, path = %config_file.display(), "config file not found, using default config");
                Ok(Config::default())
            }
            Err(err) => Err(err).context("opening config"),
        }
    }

    /// Writes the config to the file corresponding to the given profile. The file location is described
    /// in `load`.
    pub fn write(&self, profile: &str) -> anyhow::Result<()> {
        let path = Config::file_path(profile)?;

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context("couldn't create user config directory")?;
        }

        std::fs::write(&path, &serde_json::to_vec(self).unwrap()).context("writing config")?;
        Ok(())
    }

    fn file_path(profile: &str) -> anyhow::Result<PathBuf> {
        let config_dir = dirs::config_dir()
            .context("couldn't determine user config directory")?
            .join("flowctl");
        Ok(config_dir.join(format!("{}.json", profile)))
    }

    /// Returns a dashboard URL for the given relative path. Resolves against the
    /// dashboard_url from the config, if present, falling back to `dashboard.estuary.dev`.
    pub fn dashboard_url(&self, path: &str) -> anyhow::Result<url::Url> {
        let resolved = if let Some(url) = self.dashboard_url.as_ref() {
            url.join(path)?
        } else {
            let default_url = url::Url::parse("https://dashboard.estuary.dev").unwrap();
            default_url.join(path)?
        };
        Ok(resolved)
    }

    pub fn cur_draft(&self) -> anyhow::Result<String> {
        match &self.draft {
            Some(draft) => Ok(draft.clone()),
            None => {
                anyhow::bail!("You must create or select a draft");
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct API {
    // URL endpoint of the Flow control-plane Rest API.
    pub endpoint: url::Url,
    // Public (shared) anonymous token of the control-plane API.
    pub public_token: String,

    /// The email address of the authenticated user. Will be `None` if
    /// no authentication has been configured.
    pub user_email: Option<String>,
}

impl API {
    pub fn production() -> Self {
        Self {
            endpoint: url::Url::parse("https://eyrcnmuzzyriypdajwdk.supabase.co/rest/v1").unwrap(),
            public_token: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5cmNubXV6enlyaXlwZGFqd2RrIiwicm9sZSI6ImFub24iLCJpYXQiOjE2NDg3NTA1NzksImV4cCI6MTk2NDMyNjU3OX0.y1OyXD3-DYMz10eGxzo1eeamVMMUwIIeOoMryTRAoco".to_string(),
            user_email: None,
        }
    }
}
