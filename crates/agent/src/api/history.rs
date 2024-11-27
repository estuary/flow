use std::sync::Arc;

use axum::{
    extract::{Path, State},
    Extension, Json,
};
use chrono::{DateTime, Utc};
use models::Id;

use crate::publications;

use super::{ApiError, App, ControlClaims};

#[derive(Debug, serde::Serialize)]
pub struct UserPublication {
    id: Id,
    user_email: String,
    detail: Option<String>,
    pub_id: Id,
    result: publications::JobStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<crate::draft::Error>,
}

#[derive(Debug, serde::Serialize)]
pub struct ControllerPublication {
    pub_id: Id,
    detail: Option<String>,
    result: publications::JobStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<crate::draft::Error>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_touch: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(Debug, serde::Serialize)]
pub struct Event {
    first_ts: DateTime<Utc>,
    last_ts: DateTime<Utc>,
    count: u32,
    #[serde(flatten)]
    data: EventType,
}

#[derive(Debug, serde::Serialize)]
pub enum EventType {
    UserPublication(UserPublication),
    ControllerPublication(ControllerPublication),
}

#[derive(Debug, serde::Serialize)]
pub struct HistoryResponse {
    catalog_name: String,
    live_spec_id: Id,
    spec_type: Option<models::CatalogType>,
    events: Vec<Event>,
}

pub async fn handle_get_history(
    state: State<Arc<App>>,
    Extension(claims): Extension<ControlClaims>,
    Path(catalog_name): Path<String>,
) -> Result<Json<HistoryResponse>, ApiError> {
    todo!()
}
