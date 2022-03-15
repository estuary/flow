#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Could not parse Resource JSON")]
    SchemaJsonParsing(#[from] serde_json::Error),
    #[error("Unknown type {r#type} in projection for field {field}")]
    UnknownType { r#type: String, field: String },
    #[error("The binding has no field_selection")]
    FieldSelectionMissing,
    #[error("The binding has no collection")]
    CollectionMissing,
}
