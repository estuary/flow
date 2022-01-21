use std::convert::{TryFrom, TryInto};

use once_cell::sync::OnceCell;

pub fn app_env() -> &'static AppEnv {
    static APP_ENV: OnceCell<AppEnv> = OnceCell::new();

    APP_ENV.get_or_init(|| {
        std::env::var("APP_ENV")
            .unwrap_or_else(|_| "development".into())
            .try_into()
            .expect("To parse APP_ENV")
    })
}

#[derive(Debug)]
pub enum AppEnv {
    Development,
    Production,
    Test,
}

impl AppEnv {
    pub fn as_str(&self) -> &'static str {
        match self {
            &AppEnv::Development => "development",
            &AppEnv::Production => "production",
            &AppEnv::Test => "test",
        }
    }
}

impl TryFrom<String> for AppEnv {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "development" => Ok(Self::Development),
            "production" => Ok(Self::Production),
            "test" => Ok(Self::Test),
            otherwise => Err(format!("{} is not a known environment", otherwise)),
        }
    }
}
