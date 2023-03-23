//! JSON Schema + Translate = Schemalate
//! Contains modules for generating various things from JSON schemas.

/// Generates Markdown documentation of the fields in a schema.
pub mod markdown;

/// Generates Elasticsearch schemas.
pub mod elasticsearch;

// Generates Firebolt schemas.
pub mod firebolt;
