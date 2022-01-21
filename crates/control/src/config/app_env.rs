use std::convert::{TryFrom, TryInto};

use once_cell::sync::OnceCell;

static APP_ENV: OnceCell<AppEnv> = OnceCell::new();

pub fn app_env() -> &'static AppEnv {
    APP_ENV.get_or_init(|| {
        std::env::var("APP_ENV")
            .unwrap_or_else(|_| "development".into())
            .try_into()
            .expect("To parse APP_ENV")
    })
}

pub fn force_env(target: AppEnv) {
    APP_ENV.set(target).expect("app_env to be unset")
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
