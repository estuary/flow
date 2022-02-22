use crate::config::settings;
use crate::models::Id;

pub fn index() -> String {
    prefixed("/accounts")
}

pub fn show(account_id: Id) -> String {
    prefixed(format!("/accounts/{}", account_id.to_string()))
}

fn prefixed(path: impl Into<String>) -> String {
    format!("http://{}{}", settings().application.address(), path.into())
}
