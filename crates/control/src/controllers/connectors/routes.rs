use crate::config::settings;
use crate::models::Id;

pub fn index() -> String {
    prefixed("/connectors")
}

pub fn show(connector_id: Id) -> String {
    prefixed(format!("/connectors/{}", connector_id.to_string()))
}

pub fn images(connector_id: Id) -> String {
    prefixed(format!(
        "/connectors/{}/connector_images",
        connector_id.to_string()
    ))
}

fn prefixed(path: impl Into<String>) -> String {
    format!("http://{}{}", settings().application.address(), path.into())
}
