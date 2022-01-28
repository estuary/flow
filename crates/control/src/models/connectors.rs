use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

use crate::models::Id;

#[derive(Debug, Deserialize, Serialize, sqlx::Type)]
#[serde(rename_all = "camelCase")]
#[sqlx(type_name = "TEXT", rename_all = "camelCase")]
pub enum ConnectorType {
    Source,
    Materialization,
}

#[derive(Debug, Deserialize, FromRow, Serialize)]
pub struct Connector {
    pub created_at: DateTime<Utc>,
    pub description: String,
    pub id: Id,
    pub name: String,
    pub owner: String,
    pub r#type: ConnectorType,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, FromRow, Serialize)]
pub struct CreateConnector {
    pub description: String,
    pub name: String,
    pub owner: String,
    pub r#type: ConnectorType,
}
