use crate::config::settings;
use crate::models::Id;

pub fn index() -> String {
    prefixed("/connector_images")
}

pub fn show(image_id: Id) -> String {
    prefixed(format!("/connector_images/{}", image_id.to_string()))
}

pub fn spec(image_id: Id) -> String {
    prefixed(format!("/connector_images/{}/spec", image_id.to_string()))
}

pub fn discovery(connector_id: Id) -> String {
    prefixed(format!(
        "/connector_images/{}/discovery",
        connector_id.to_string()
    ))
}

fn prefixed(path: impl Into<String>) -> String {
    format!("http://{}{}", settings().application.address(), path.into())
}
