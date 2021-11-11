use serde_json::Value;
use std::fmt;

pub const UNSUPPORTED_MULTIPLE_OR_UNSPECIFIED_TYPES: &str =
    "multiple non-trivial data types or unspecified data types are not supported";
pub const UNSUPPORTED_NON_ARRAY_OR_OBJECTS: &str =
    "data types other than objects or arrays of objects are not supported";
pub const UNSUPPORTED_OBJECT_ADDITIONAL_FIELDS: &str =
    "additional properties on an object are not supported";
pub const UNSUPPORTED_TUPLE: &str = "Tuple is not supported";

pub const POINTER_EMPTY: &str = "empty path";
pub const POINTER_MISSING_FIELD: &str = "pointer of a non-existing field";
pub const POINTER_WRONG_FIELD_TYPE: &str =
    "non-leaf field on json path is a basic type (int, bool..)";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("bad schema json")]
    SchemaJsonParsing(#[from] serde_json::Error),
    #[error("failed building schema")]
    SchemaBuildError(#[from] json::schema::BuildError),
    #[error("failed indexing schema")]
    SchemaIndexError(#[from] json::schema::index::Error),
    #[error("failed parsing schema_json.$id as a url.")]
    UrlParsing(#[from] url::ParseError),
    #[error("a valid $id field in the input json schema is missing.")]
    MissingOrInvalidIdField(),
    #[error("unsupported Flow schema in elastic search, details: {message}, shape: {shape:?}")]
    UnSupportedError {
        message: &'static str,
        shape: Box<dyn fmt::Debug>,
    },
    #[error("unable to override elastic search schema, details: {message}, overriding_schema: {overriding_schema}, pointer: {pointer}")]
    OverridePointerError {
        message: &'static str,
        overriding_schema: Value,
        pointer: String,
    },
}
