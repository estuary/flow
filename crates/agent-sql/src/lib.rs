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

mod text_json;
pub use text_json::TextJson;

pub use models::{Capability, CatalogType, Id};
pub use tables::RoleGrant;

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
