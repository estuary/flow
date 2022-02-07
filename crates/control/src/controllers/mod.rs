pub mod connector_images;
pub mod connectors;
pub mod health_check;

/// Simple wrapper to differentiate metadata from primary data.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Payload<D> {
    Data(D),
    Error(String),
}

/// We often want to act as a passthrough to a connector, forwarding a response
/// exactly as it was sent to/from the connector. We use `RawValue` to avoid
/// serde actually parsing the contents.
///
/// If/when we want to parse/validate/modify payloads on the within the API,
/// we'll remove these usages.
type RawJson = Box<serde_json::value::RawValue>;
