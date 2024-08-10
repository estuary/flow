pub mod connector_tags;
pub mod controllers;
pub mod data_plane;
pub mod directives;
pub mod discovers;
pub mod drafts;
pub mod evolutions;
pub mod live_specs;
pub mod publications;
use serde::{Deserialize, Serialize};
use sqlx::types::Uuid;
use std::fmt::{self, Display};

pub use models::Id;

mod text_json;
pub use text_json::TextJson;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "flow_type")]
#[sqlx(rename_all = "snake_case")]
pub enum FlowType {
    Capture,
    Collection,
    Materialization,
    Test,
    SourceCapture,
}

impl From<CatalogType> for FlowType {
    fn from(c: CatalogType) -> Self {
        match c {
            CatalogType::Capture => FlowType::Capture,
            CatalogType::Collection => FlowType::Collection,
            CatalogType::Materialization => FlowType::Materialization,
            CatalogType::Test => FlowType::Test,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "catalog_spec_type")]
#[sqlx(rename_all = "lowercase")]
pub enum CatalogType {
    Capture,
    Collection,
    Materialization,
    Test,
}

impl sqlx::postgres::PgHasArrayType for CatalogType {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("_catalog_spec_type")
    }
}

impl Display for CatalogType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match *self {
            CatalogType::Capture => "capture",
            CatalogType::Collection => "collection",
            CatalogType::Materialization => "materialization",
            CatalogType::Test => "test",
        };
        f.write_str(s)
    }
}

impl Into<models::CatalogType> for CatalogType {
    fn into(self) -> models::CatalogType {
        match self {
            CatalogType::Capture => models::CatalogType::Capture,
            CatalogType::Collection => models::CatalogType::Collection,
            CatalogType::Materialization => models::CatalogType::Materialization,
            CatalogType::Test => models::CatalogType::Test,
        }
    }
}

impl From<models::CatalogType> for CatalogType {
    fn from(m: models::CatalogType) -> Self {
        match m {
            models::CatalogType::Capture => CatalogType::Capture,
            models::CatalogType::Collection => CatalogType::Collection,
            models::CatalogType::Materialization => CatalogType::Materialization,
            models::CatalogType::Test => CatalogType::Test,
        }
    }
}

/// Note that the discriminants here align with those in the database type.
#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Serialize,
    Deserialize,
    sqlx::Type,
    schemars::JsonSchema,
)]
#[sqlx(type_name = "grant_capability")]
#[sqlx(rename_all = "lowercase")]
#[serde(rename_all = "camelCase")]
pub enum Capability {
    Read = 0x10,
    Write = 0x20,
    Admin = 0x30,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RoleGrant {
    pub subject_role: String,
    pub object_role: String,
    pub capability: Capability,
}

/// Returns the user ID for the given email address, or an error if the email address is not found.
pub async fn get_user_id_for_email(email: &str, db: &sqlx::PgPool) -> sqlx::Result<Uuid> {
    sqlx::query_scalar!(
        r#"
        SELECT id
        FROM auth.users
        WHERE email = $1
        "#,
        email
    )
    .fetch_one(db)
    .await
}
