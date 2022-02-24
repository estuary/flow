pub mod accounts;
pub mod connector_images;
pub mod connectors;
pub mod health_check;
pub mod json_api;
pub mod sessions;

/// Generates an absolute url to the path based on the application address.
pub fn url_for(path: impl AsRef<str>) -> String {
    use crate::config::settings;

    format!(
        "http://{}{}",
        settings().application.address(),
        path.as_ref()
    )
}
