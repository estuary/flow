//! JSON Schema + Translate = Schemalate
//! Contains modules for generating various things from JSON schemas.

/// Generates Markdown documentation of the fields in a schema.
pub mod markdown;

/// Generates Typescript types that serialize/deserialize into/from JSON that validates against the
/// schema.
pub mod typescript;

/// Generates Elasticsearch schemas.
pub mod elasticsearch;
