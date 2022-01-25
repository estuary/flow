use once_cell::sync::OnceCell;
use serde::Deserialize;

pub mod app_env;

pub use app_env::app_env;

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub application: ApplicationSettings,
    pub database: DatabaseSettings,
}

#[derive(Debug, Deserialize)]
pub struct ApplicationSettings {
    pub host: String,
    pub port: u16,
}

impl ApplicationSettings {
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[derive(Debug, Deserialize)]
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

pub fn settings() -> &'static Settings {
    static SETTINGS: OnceCell<Settings> = OnceCell::new();

    SETTINGS.get_or_init(|| load_settings().expect("Failed to load settings"))
}

fn load_settings() -> Result<Settings, config::ConfigError> {
    let mut config = config::Config::default();

    // TODO: Allow passing a configuration directory as a CLI arg
    let current_dir = std::env::current_dir().expect("The current directory to be available");
    let config_dir = current_dir.join("config");

    // Load base settings
    config.merge(config::File::from(config_dir.join("base")).required(true))?;

    // Load app_env-specific settings
    config.merge(config::File::from(config_dir.join(app_env().as_str())).required(true))?;

    // Load settings from ENV_VARs
    config.merge(config::Environment::with_prefix("CONTROL").separator("_"))?;

    config.try_into()
}
