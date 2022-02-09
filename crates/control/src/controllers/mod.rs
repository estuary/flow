pub mod connector_images;
pub mod connectors;
pub mod health_check;
pub mod json_api;

/// Simple wrapper to differentiate metadata from primary data.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Payload<D> {
    Data(D),
    Error(String),
}

// Temporary re-export.
pub use json_api::RawJson;
