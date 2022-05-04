pub mod connector_tags;
pub mod discover;
pub mod publications;
use serde::{Deserialize, Serialize};

mod id;
pub use id::Id;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "catalog_spec_type")]
#[sqlx(rename_all = "lowercase")]
pub enum CatalogType {
    Capture,
    Collection,
    Materialization,
    Test,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "grant_capability")]
#[sqlx(rename_all = "lowercase")]
#[serde(rename_all = "camelCase")]
pub enum Capability {
    Read,
    Write,
    Admin,
}
