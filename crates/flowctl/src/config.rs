use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

lazy_static::lazy_static! {
    static ref DEFAULT_DASHBOARD_URL: url::Url = url::Url::parse("https://dashboard.estuary.dev/").unwrap();
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Config {
    /// URL of the Flow UI, which will be used as a base when flowctl generates links to it.
    pub dashboard_url: Option<url::Url>,
    /// ID of the current draft, or None if no draft is configured.
    pub draft: Option<String>,
    // Current access token, or None if no token is set.
    pub api: Option<API>,
}

impl Config {
    /// Loads the config corresponding to the given named `profile`.
    /// This loads from:
    /// - $HOME/.config/flowctl/${profile}.json on linux
    /// - $HOME/Library/Application Support/flowctl/${profile}.json on macos
    pub fn load(profile: &str) -> anyhow::Result<Config> {
        let config_file = Config::file_path(profile)?;
        let config = match std::fs::read(&config_file) {
            Ok(v) => {
                let cfg = serde_json::from_slice(&v).with_context(|| {
                    format!(
                        "failed to parse flowctl config at {}",
                        config_file.to_string_lossy(),
                    )
                })?;
                tracing::debug!(path = %config_file.display(), "loaded and used config");
                cfg
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                // We don't use warn here, because it's likely that every new user
                // would see that the very first time they run the CLI. But in other
                // scenarios, this is likely to be very useful information.
                tracing::info!(path = %config_file.display(), profile = %profile,
                    "no config file found at path, using default");
                Config::default()
            }
            Err(err) => {
                return Err(err).context("opening config");
            }
        };
        Ok(config)
    }

    /// Write the config to the file corresponding to the given named `profile`.
    /// The file path is determined as documented in `load`.
    pub fn write(&self, profile: &str) -> anyhow::Result<()> {
        let ser = serde_json::to_vec_pretty(self)?;
        let dir = Config::config_dir()?;
        std::fs::create_dir_all(&dir).context("creating config dir")?;

        // It's important that we persist the config file atomically, so that
        // concurrent invocations of flowctl don't accidentially observe the
        // truncated file before we overwrite it. The `persist` function will
        // use a rename in order to ensure that the operation is atomic. In
        // order for that to work, we must ensure that `temp` and `real` are on
        // the same filesystem, which is why we use the config directory as the
        // temp dir.
        let temp = tempfile::NamedTempFile::new_in(dir)?;
        std::fs::write(&temp, &ser).context("writing config")?;

        let real = Config::file_path(profile)?;
        temp.persist(real).context("persisting config file")?;

        Ok(())
    }

    fn config_dir() -> anyhow::Result<PathBuf> {
        let path = dirs::config_dir()
            .context("couldn't determine user config directory")?
            .join("flowctl");
        Ok(path)
    }

    fn file_path(profile: &str) -> anyhow::Result<PathBuf> {
        let path = Config::config_dir()?.join(format!("{profile}.json"));
        Ok(path)
    }

    pub fn cur_draft(&self) -> anyhow::Result<String> {
        match &self.draft {
            Some(draft) => Ok(draft.clone()),
            None => {
                anyhow::bail!("You must create or select a draft");
            }
        }
    }

    pub fn set_access_token(&mut self, access_token: String) {
        // Don't overwrite the other fields of api if they are already present.
        if let Some(api) = self.api.as_mut() {
            api.access_token = access_token;
        } else {
            self.api = Some(API::managed(access_token));
        }
    }

    pub fn set_refresh_token(&mut self, refresh_token: RefreshToken) {
        // Don't overwrite the other fields of api if they are already present.
        if let Some(api) = self.api.as_mut() {
            api.refresh_token = Some(refresh_token);
        }
    }

    pub fn get_dashboard_url(&self, path: &str) -> anyhow::Result<url::Url> {
        let base = self
            .dashboard_url
            .as_ref()
            .unwrap_or(&*DEFAULT_DASHBOARD_URL);
        let url = base.join(path).context(
            "failed to join path to configured dashboard_url, the dashboard_url is likely invalid",
        )?;
        Ok(url)
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct RefreshToken {
    pub id: String,
    pub secret: String,
}

impl RefreshToken {
    pub fn from_base64(encoded_token: &str) -> anyhow::Result<RefreshToken> {
        let decoded = base64::decode(encoded_token).context("invalid base64")?;
        let tk: RefreshToken = serde_json::from_slice(&decoded)?;
        Ok(tk)
    }

    pub fn to_base64(&self) -> anyhow::Result<String> {
        let ser = serde_json::to_vec(self)?;
        Ok(base64::encode(&ser))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct API {
    // URL endpoint of the Flow control-plane Rest API.
    pub endpoint: url::Url,
    // Public (shared) anonymous token of the control-plane API.
    pub public_token: String,
    // Secret access token of the control-plane API.
    pub access_token: String,
    // Secret refresh token of the control-plane API, used to generate access_token when it expires.
    pub refresh_token: Option<RefreshToken>,
}

pub const PUBLIC_TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5cmNubXV6enlyaXlwZGFqd2RrIiwicm9sZSI6ImFub24iLCJpYXQiOjE2NDg3NTA1NzksImV4cCI6MTk2NDMyNjU3OX0.y1OyXD3-DYMz10eGxzo1eeamVMMUwIIeOoMryTRAoco";

pub const ENDPOINT: &str = "https://eyrcnmuzzyriypdajwdk.supabase.co/rest/v1";

impl API {
    fn managed(access_token: String) -> Self {
        Self {
            endpoint: url::Url::parse(ENDPOINT).unwrap(),
            public_token: PUBLIC_TOKEN.to_string(),
            access_token,
            refresh_token: None,
        }
    }
}
