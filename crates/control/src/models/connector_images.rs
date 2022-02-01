use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

use crate::models::Id;

#[derive(Debug, Deserialize, FromRow, Serialize)]
pub struct ConnectorImage {
    pub connector_id: Id,
    pub created_at: DateTime<Utc>,
    pub id: Id,
    pub image: String,
    pub sha256: String,
    pub tag: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, FromRow, Serialize)]
pub struct CreateConnectorImage {
    pub connector_id: Id,
    pub image: String,
    pub sha256: String,
    pub tag: String,
}
