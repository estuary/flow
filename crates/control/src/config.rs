use std::path::Path;

use once_cell::sync::OnceCell;
use serde::Deserialize;
use validator::{Validate, ValidationError, ValidationErrors};

pub mod app_env;

pub use app_env::app_env;

#[derive(Debug, Deserialize, Validate)]
pub struct Settings {
    pub application: ApplicationSettings,
    pub database: DatabaseSettings,
    #[validate]
    pub builds_root: BuildsRootSettings,
}

#[derive(Debug, Deserialize, Validate)]
pub struct BuildsRootSettings {
    /// The URI of the builds root, where build databases will be stored. In production, this will
    /// always be a `gs://` URI for GCS. When running locally, this may be a `file:///` URI. No
    /// other URI schemes are currently supported.
    #[validate(custom = "BuildsRootSettings::validate_uri")]
    pub uri: url::Url,
}

impl BuildsRootSettings {
    fn validate_uri(uri: &url::Url) -> Result<(), ValidationError> {
        let ensure = |ok: bool, msg: &str| {
            if !ok {
                let mut err = ValidationError::new("builds_root.uri");
                err.message = Some(msg.to_owned().into());
                Err(err)
            } else {
                Ok(())
            }
        };
        ensure(!uri.cannot_be_a_base(), "uri cannot be a base")?;
        ensure(uri.path().ends_with('/'), "uri must end with a '/'")?;
        match uri.scheme() {
            "gs" | "file" => { /* all good here */ }
            other => {
                let msg = format!("invalid uri scheme: '{}'", other);
                ensure(false, msg.as_str())?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub struct ApplicationSettings {
    pub host: String,
    pub port: u16,
    pub cors: CorsSettings,
    pub connector_network: String,
}

impl ApplicationSettings {
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct DatabaseSettings {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub db_name: String,
}

impl DatabaseSettings {
    pub fn url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.username, self.password, self.host, self.port, self.db_name
        )
    }
}

#[derive(Debug, Deserialize)]
pub struct CorsSettings {
    pub allowed_origins: Vec<String>,
}

impl CorsSettings {
    pub fn allowed_origins(&self) -> &[String] {
        self.allowed_origins.as_ref()
    }
}

static SETTINGS: OnceCell<Settings> = OnceCell::new();
pub fn settings() -> &'static Settings {
    SETTINGS
        .get()
        .expect("to have initialized SETTINGS via `load_settings`")
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("loading config: {0}")]
    Loading(#[from] ::config::ConfigError),
    #[error("invalid config: {0}")]
    Validation(#[from] ValidationErrors),
}

pub fn load_settings<P>(config_path: P) -> Result<&'static Settings, ConfigError>
where
    P: AsRef<Path>,
{
    SETTINGS.get_or_try_init(|| {
        let mut config = config::Config::default();

        // Load app_env-specific settings
        config.merge(config::File::from(config_path.as_ref()).required(true))?;

        // Parse the DATABASE_URL env var for database settings
        merge_database_url(&mut config).expect("to parse the database url");

        // Load settings from ENV_VARs
        config.merge(config::Environment::with_prefix("CONTROL").separator("__"))?;

        let settings: Settings = config.try_into()?;
        settings.validate()?;
        Ok(settings)
    })
}

/// SQLx expects the DATABASE_URL in the env, and this makes constructing it in
/// the config files a bit difficult. The `query!` macros read from the env or a
/// .env file exclusively, and if it isn't defined the project fails to compile.
///
/// Rather than duplicating the config, let's use the .env files to store the
/// DATABASE_URL and merge them into our regular settings. There's one single
/// place to define it, but users access the value like any other setting.
///
/// Any component of the DATABASE_URL can be overridden using a specific env var.
/// * ex. `CONTROL_DATABASE__DB_NAME=foobar`
fn merge_database_url(config: &mut config::Config) -> anyhow::Result<()> {
    let db_str = match std::env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(_e) => {
            // If there's no DATABASE_URL set by the environment, that's not an
            // error. We'll use the config files or specific env vars instead.
            return Ok(());
        }
    };

    let url = url::Url::parse(&db_str)?;

    if !url.username().is_empty() {
        config.set_default("database.username", url.username())?;
    }

    if let Some(pw) = url.password() {
        config.set_default("database.password", pw)?;
    }

    if let Some(host) = url.host_str() {
        config.set_default("database.host", host)?;
    }

    if let Some(port) = url.port() {
        config.set_default("database.port", port.to_string())?;
    }

    let path = url.path();
    if !path.is_empty() && path.starts_with('/') && !(&path[1..]).contains('/') {
        config.set_default("database.db_name", &path[1..])?;
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn builds_root_uri_validation() {
        validate_uri("file:///foo/bar/").expect("should pass validation");

        // We probably should eventually make these assertions more specific, but leaving this as
        // "good enough for now".
        validate_uri("file:///foo/bar").expect_err("should fail due to missing trailing slash");
        validate_uri("wut:///foo/bar/").expect_err("should fail due to unsupported scheme");
        validate_uri("gs:foo/bar/").expect_err("should fail due to cannot be a base");
    }

    fn validate_uri(uri: &str) -> Result<(), validator::ValidationErrors> {
        let parsed = url::Url::parse(uri).expect("failed to parse test url");
        let conf = BuildsRootSettings { uri: parsed };
        conf.validate()
    }
}
