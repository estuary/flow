use once_cell::sync::OnceCell;
use serde::Deserialize;

pub mod app_env;

pub use app_env::app_env;
use tracing::info;

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub application: ApplicationSettings,
    pub database: DatabaseSettings,
}

#[derive(Debug, Deserialize)]
pub struct ApplicationSettings {
    pub host: String,
    pub port: u16,
    pub cors: CorsSettings,
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

pub fn settings() -> &'static Settings {
    static SETTINGS: OnceCell<Settings> = OnceCell::new();

    SETTINGS.get_or_init(|| {
        load_dotenv(".env");
        load_dotenv(&format!(".env.{}", app_env().as_str()));
        load_settings().expect("Failed to load settings")
    })
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

    // Parse the DATABASE_URL env var for database settings
    merge_database_url(&mut config).expect("to parse the database url");

    // Load settings from ENV_VARs
    config.merge(config::Environment::with_prefix("CONTROL").separator("__"))?;

    config.try_into()
}

/// If we have a .env file to load. In production, we'll be loading k8s secrets
/// into the env, so we won't have a .env file.
fn load_dotenv(filename: &str) {
    match dotenv::overwrite_from_filename(filename) {
        Ok(path) => {
            info!("Loaded env from {}.", path.to_str().unwrap_or(filename));
        }
        Err(dotenv::Error::LineParse(contents, line_num)) => {
            panic!(
                "Found a {} file, but it was malformed on Line {}: {}",
                filename, line_num, contents
            )
        }
        Err(e) => {
            info!("No {} file found: {}", filename, e)
        }
    };
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
