use anyhow::Context;
use std::path::PathBuf;

use flow_client::{
    DEFAULT_AGENT_URL, DEFAULT_CONFIG_ENCRYPTION_URL, DEFAULT_DASHBOARD_URL,
    DEFAULT_PG_PUBLIC_TOKEN, DEFAULT_PG_URL, LOCAL_AGENT_URL, LOCAL_CONFIG_ENCRYPTION_URL,
    LOCAL_DASHBOARD_URL, LOCAL_PG_PUBLIC_TOKEN, LOCAL_PG_URL, client::RefreshToken,
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
    /// Service-account API key (`flow_sa_...`). Sourced only from the
    /// `FLOW_API_KEY` environment variable and held in memory for the life of
    /// the process — an API key is a long-lived secret, so it is deliberately
    /// never read from or written to the config file. When set, it is the
    /// durable credential used to mint access tokens, in place of a refresh token.
    #[serde(skip)]
    pub user_api_key: Option<String>,
    /// URL endpoint for the config encryption service.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_encryption_url: Option<url::Url>,

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

    pub fn get_config_encryption_url(&self) -> &url::Url {
        if let Some(config_encryption_url) = &self.config_encryption_url {
            config_encryption_url
        } else if self.is_local {
            &LOCAL_CONFIG_ENCRYPTION_URL
        } else {
            &DEFAULT_CONFIG_ENCRYPTION_URL
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

        // FLOW_API_KEY / FLOW_AUTH_TOKEN override any credential loaded from
        // disk. FLOW_API_KEY (a service-account API key) takes precedence when
        // both are set; it's held in memory only, so nothing derived from it is
        // persisted. The dispatch itself lives in `resolve_env_credential` so
        // the precedence ladder is unit-testable without touching the process
        // environment; here we only read the env and emit the operator-facing
        // logging that depends on which vars were set.
        let api_key_env = std::env::var(FLOW_API_KEY).ok();
        let auth_token_env = std::env::var(FLOW_AUTH_TOKEN).ok();

        if api_key_env.is_some() && auth_token_env.is_some() {
            tracing::debug!("both FLOW_API_KEY and FLOW_AUTH_TOKEN are set; ignoring FLOW_AUTH_TOKEN");
        } else if api_key_env.is_none() && auth_token_env.is_some() {
            // FLOW_AUTH_TOKEN is deprecated in favor of FLOW_API_KEY (automated
            // access) and `flowctl auth login` (interactive). It still works but
            // is slated for removal; warn so usage drains ahead of sunset.
            tracing::warn!(
                "FLOW_AUTH_TOKEN is deprecated and will be removed in a future release; \
                 set FLOW_API_KEY to a service-account API key for automated access, \
                 or run `flowctl auth login` for interactive use"
            );
        }

        match resolve_env_credential(api_key_env, auth_token_env)? {
            Some(EnvCredential::ApiKey(api_key)) => {
                tracing::info!("using FLOW_API_KEY environment API key");
                config.user_api_key = Some(api_key);
                config.user_access_token = None;
                config.user_refresh_token = None;
            }
            Some(EnvCredential::AccessToken(token)) => {
                tracing::info!("using FLOW_AUTH_TOKEN environment access token");
                config.user_access_token = Some(token);
                config.user_refresh_token = None;
            }
            Some(EnvCredential::RefreshToken(token)) => {
                tracing::info!("using FLOW_AUTH_TOKEN environment refresh token");
                config.user_refresh_token = Some(token);
                config.user_access_token = None;
            }
            None => {}
        }

        config.is_local = profile == "local";

        Ok(config)
    }

    /// Whether this config should be persisted back to disk after a command.
    /// A config carrying an env-supplied API key authenticates an ephemeral
    /// run, so we never write credentials (or other state) derived from it: the
    /// long-lived secret stays off disk and a human's existing config is left
    /// untouched.
    pub(crate) fn should_persist(&self) -> bool {
        self.user_api_key.is_none()
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

    pub fn build_anon_client(&self) -> flow_client::Client {
        let user_agent = format!("flowctl-{}", env!("CARGO_PKG_VERSION"));
        flow_client::Client::new(
            user_agent,
            self.get_agent_url().clone(),
            self.get_pg_public_token().to_string(),
            self.get_pg_url().clone(),
            None,
            self.get_config_encryption_url().clone(),
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

// Environment variable inspected for an auth credential: either a JWT access
// token, or a base64-encoded refresh token JSON.
const FLOW_AUTH_TOKEN: &str = "FLOW_AUTH_TOKEN";

// Environment variable holding a service-account API key (`flow_sa_...`). Takes
// precedence over FLOW_AUTH_TOKEN when both are set.
const FLOW_API_KEY: &str = "FLOW_API_KEY";

/// The credential contributed by the environment, after dispatching on
/// FLOW_API_KEY / FLOW_AUTH_TOKEN.
#[cfg_attr(test, derive(Debug))]
enum EnvCredential {
    /// A service-account API key from FLOW_API_KEY.
    ApiKey(String),
    /// A JWT access token from FLOW_AUTH_TOKEN.
    AccessToken(String),
    /// A refresh token decoded from FLOW_AUTH_TOKEN.
    RefreshToken(RefreshToken),
}

/// Resolve the credential supplied by the environment from the raw values of
/// FLOW_API_KEY and FLOW_AUTH_TOKEN. Pure (no environment reads) so the
/// precedence ladder and FLOW_AUTH_TOKEN's JWT-vs-refresh-token discrimination
/// are unit-testable. FLOW_API_KEY wins when both are set; an FLOW_API_KEY that
/// isn't a service-account key is an error rather than a silent fallback.
fn resolve_env_credential(
    api_key: Option<String>,
    auth_token: Option<String>,
) -> anyhow::Result<Option<EnvCredential>> {
    if let Some(api_key) = api_key {
        if !api_key.starts_with("flow_sa_") {
            anyhow::bail!(
                "FLOW_API_KEY must be a service-account API key, starting with `flow_sa_`"
            );
        }
        return Ok(Some(EnvCredential::ApiKey(api_key)));
    }

    let Some(auth_token) = auth_token else {
        return Ok(None);
    };

    // A value with three dot-delimited segments is a JWT access token; anything
    // else is a base64-encoded refresh token JSON.
    if auth_token.split('.').count() == 3 {
        return Ok(Some(EnvCredential::AccessToken(auth_token)));
    }

    let decoded = tokens::jwt::parse_base64(&auth_token).context("FLOW_AUTH_TOKEN is not base64")?;
    let token: RefreshToken =
        serde_json::from_slice(&decoded).context("FLOW_AUTH_TOKEN is invalid JSON")?;
    Ok(Some(EnvCredential::RefreshToken(token)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    fn refresh_token_b64() -> String {
        let token = RefreshToken {
            id: models::Id::new([1, 2, 3, 4, 5, 6, 7, 8]),
            secret: "the-secret".to_string(),
        };
        let json = serde_json::to_vec(&token).unwrap();
        base64::engine::general_purpose::STANDARD.encode(json)
    }

    #[test]
    fn api_key_takes_precedence_over_auth_token() {
        let resolved =
            resolve_env_credential(Some("flow_sa_abc".to_string()), Some("a.b.c".to_string()))
                .unwrap();
        assert!(matches!(resolved, Some(EnvCredential::ApiKey(k)) if k == "flow_sa_abc"));
    }

    #[test]
    fn api_key_without_prefix_is_rejected() {
        let err = resolve_env_credential(Some("not-an-sa-key".to_string()), None).unwrap_err();
        assert!(err.to_string().contains("flow_sa_"));
    }

    #[test]
    fn auth_token_jwt_resolves_to_access_token() {
        let resolved = resolve_env_credential(None, Some("header.payload.sig".to_string())).unwrap();
        assert!(matches!(resolved, Some(EnvCredential::AccessToken(t)) if t == "header.payload.sig"));
    }

    #[test]
    fn auth_token_base64_resolves_to_refresh_token() {
        let resolved = resolve_env_credential(None, Some(refresh_token_b64())).unwrap();
        assert!(matches!(resolved, Some(EnvCredential::RefreshToken(t)) if t.secret == "the-secret"));
    }

    #[test]
    fn no_env_credential_resolves_to_none() {
        assert!(resolve_env_credential(None, None).unwrap().is_none());
    }

    #[test]
    fn should_persist_is_false_under_api_key() {
        let config = Config {
            user_api_key: Some("flow_sa_abc".to_string()),
            ..Default::default()
        };
        assert!(!config.should_persist());
    }

    #[test]
    fn should_persist_is_true_without_api_key() {
        assert!(Config::default().should_persist());
    }

    #[test]
    fn api_key_is_never_serialized_to_disk() {
        let config = Config {
            user_api_key: Some("flow_sa_super_secret".to_string()),
            ..Default::default()
        };
        let serialized = serde_json::to_string(&config).unwrap();
        assert!(!serialized.contains("flow_sa_super_secret"));
        assert!(!serialized.contains("user_api_key"));
    }
}
