#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Could not parse Resource JSON")]
    SchemaJsonParsing(#[from] serde_json::Error),
    #[error("Unknown type {r#type} in projection for field '{field}'")]
    UnknownType { r#type: String, field: String },
    #[error("")]
    InvalidSelectedFields,
    #[error("No such projction for field '{field}'")]
    NoProjectionForField { field: String },
    #[error("The field '{field}' may not be materialize because it has constraint: {constraint} with reason: {reason}")]
    NotMaterializableField {
        field: String,
        constraint: String,
        reason: String,
    },
    #[error("Required field '{field}' is missing. It is required because: {reason}")]
    RequiredFieldMissing { field: String, reason: String },
    #[error("The materialization must include a projections of location '{ptr}', but no such projection is included")]
    MissingProjection { ptr: String },
}
