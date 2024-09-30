use anyhow::Context;
use std::path::PathBuf;

use flow_client::{
    client::RefreshToken, DEFAULT_AGENT_URL, DEFAULT_DASHBOARD_URL, DEFAULT_PG_PUBLIC_TOKEN,
    DEFAULT_PG_URL, LOCAL_AGENT_URL, LOCAL_DASHBOARD_URL, LOCAL_PG_PUBLIC_TOKEN, LOCAL_PG_URL,
};

/// Configuration of `flowctl`.
///
/// We generally keep this minimal and prefer to use built-in default
/// or local value fallbacks, because that means we can update these
/// defaults in future releases of flowctl without breaking local
/// User configuration.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Config {
    /// URL endpoint of the Flow control-plane Agent API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_url: Option<url::Url>,
    /// URL of the Flow UI, which will be used as a base when flowctl generates links to it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dashboard_url: Option<url::Url>,
    /// ID of the current draft, or None if no draft is configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub draft: Option<models::Id>,
    /// Public (shared) anonymous token of the control-plane API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pg_public_token: Option<String>,
    /// URL endpoint of the Flow control-plane PostgREST API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pg_url: Option<url::Url>,
    /// Users's access token for the control-plane API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_access_token: Option<String>,
    /// User's refresh token for the control-plane API,
    /// used to generate access_token when it's unset or expires.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_refresh_token: Option<RefreshToken>,

    #[serde(skip)]
    is_local: bool,

    // Legacy API stanza, which is being phased out.
    #[serde(default, skip_serializing)]
    api: Option<DeprecatedAPISection>,
}

#[derive(Debug, serde::Deserialize)]
struct DeprecatedAPISection {
    #[allow(dead_code)]
    endpoint: url::Url,
    #[allow(dead_code)]
    public_token: String,
    access_token: String,
    refresh_token: Option<RefreshToken>,
}

impl Config {
    pub fn selected_draft(&self) -> anyhow::Result<models::Id> {
        self.draft
            .ok_or(anyhow::anyhow!("No draft is currently selected"))
    }

    pub fn get_agent_url(&self) -> &url::Url {
        if let Some(agent_url) = &self.agent_url {
            agent_url
        } else if self.is_local {
            &LOCAL_AGENT_URL
        } else {
            &DEFAULT_AGENT_URL
        }
    }

    pub fn get_dashboard_url(&self) -> &url::Url {
        if let Some(dashboard_url) = &self.dashboard_url {
            dashboard_url
        } else if self.is_local {
            &LOCAL_DASHBOARD_URL
        } else {
            &DEFAULT_DASHBOARD_URL
        }
    }

    pub fn get_pg_public_token(&self) -> &str {
        if let Some(pg_public_token) = &self.pg_public_token {
            pg_public_token
        } else if self.is_local {
            LOCAL_PG_PUBLIC_TOKEN
        } else {
            DEFAULT_PG_PUBLIC_TOKEN
        }
    }

    pub fn get_pg_url(&self) -> &url::Url {
        if let Some(pg_url) = &self.pg_url {
            pg_url
        } else if self.is_local {
            &LOCAL_PG_URL
        } else {
            &DEFAULT_PG_URL
        }
    }

    /// Loads the config corresponding to the given named `profile`.
    /// This loads from:
    /// - $HOME/.config/flowctl/${profile}.json on linux
    /// - $HOME/Library/Application Support/flowctl/${profile}.json on macos
    pub fn load(profile: &str) -> anyhow::Result<Config> {
        let config_file = Config::file_path(profile)?;
        let mut config = match std::fs::read(&config_file) {
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
                return Err(err).context("failed to read config");
            }
        };

        // Migrate legacy portions of the config.
        if let Some(DeprecatedAPISection {
            endpoint: _,
            public_token: _,
            access_token,
            refresh_token,
        }) = config.api.take()
        {
            config.user_access_token = Some(access_token);
            config.user_refresh_token = refresh_token;
        }

        // If a refresh token is not defined, attempt to parse one from the environment.
        if config.user_refresh_token.is_none() {
            if let Ok(env_token) = std::env::var(FLOW_AUTH_TOKEN) {
                let decoded = base64::decode(env_token).context("FLOW_AUTH_TOKEN is not base64")?;
                let token: RefreshToken =
                    serde_json::from_slice(&decoded).context("FLOW_AUTH_TOKEN is invalid JSON")?;

                tracing::info!("using refresh token from environment variable {FLOW_AUTH_TOKEN}");
                config.user_refresh_token = Some(token);
            }
        }
        config.is_local = profile == "local";

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
    pub fn client(&self) -> flow_client::Client {
        flow_client::Client::new(
            self.get_agent_url().clone(),
            self.get_pg_public_token().to_string(),
            self.get_pg_url().clone(),
            self.user_access_token.clone(),
            self.user_refresh_token.clone(),
        )
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
}

// Environment variable which is inspected for a base64-encoded refresh token.
const FLOW_AUTH_TOKEN: &str = "FLOW_AUTH_TOKEN";
