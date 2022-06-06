pub mod properties;
pub mod schema;
pub mod validations;

#[derive(Debug, thiserror::Error)]
pub enum SchemaParseError {
    #[error("failed to parse the value: {0}")]
    InvalidValueType(serde_json::Value),

    #[error("failed generating a property: #{0}")]
    PropertyError(properties::PropertyError),

    #[error("failed to encode schema: #{0}")]
    EncodeError(serde_json::Error),
}

impl From<serde_json::Error> for SchemaParseError {
    fn from(err: serde_json::Error) -> SchemaParseError {
        SchemaParseError::EncodeError(err)
    }
}

impl From<properties::PropertyError> for SchemaParseError {
    fn from(err: properties::PropertyError) -> SchemaParseError {
        SchemaParseError::PropertyError(err)
    }
}
