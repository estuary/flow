use std::fmt;

pub const UNSUPPORTED_MULTIPLE_OR_UNSPECIFIED_TYPES: &str =
    "Multiple non-trivial data types or unspecified data types are not supported";
pub const UNSUPPORTED_NON_OBJECT_ROOT: &str =
    "Data types other than objects are not supported for the root.";
pub const UNSUPPORTED_OBJECT_ADDITIONAL_FIELDS: &str =
    "Additional properties on an object are not supported.";
pub const UNSUPPORTED_TUPLE: &str = "Tuple values are not supported.";
pub const UNSUPPORTED_NULLABLE_ARRAY: &str = "Arrays cannot be nullable, they must be required. See https://docs.firebolt.io/general-reference/data-types.html#array.";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Bad schema json")]
    SchemaJsonParsing(#[from] serde_json::Error),
    #[error("Failed building schema")]
    SchemaBuildError(#[from] json::schema::BuildError),
    #[error("Failed indexing schema")]
    SchemaIndexError(#[from] json::schema::index::Error),
    #[error("Unsupported Flow schema in firebolt, details: {message}, shape: {shape:?}")]
    UnsupportedError {
        message: &'static str,
        shape: Box<dyn fmt::Debug + Send + Sync>,
    },
}
