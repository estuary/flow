use crate::protocol::{ExecuteRequest, ExecuteResponse};
use crate::shared::logs;
use axum::{Json, extract::State, http::StatusCode};

pub type LogsTx = logs::Tx;

/// Handler for POST /execute endpoint.
/// Receives an ExecuteRequest, executes the work, and returns an ExecuteResponse.
#[tracing::instrument(
    skip(logs_tx, request),
    fields(
        task_id = %request.task_id,
        data_plane_id = %request.data_plane_id,
        action = ?request.action,
    ),
)]
pub async fn handle_execute(
    State(logs_tx): State<LogsTx>,
    Json(request): Json<ExecuteRequest>,
) -> Result<Json<ExecuteResponse>, (StatusCode, String)> {
    tracing::info!("received execute request");

    match super::worker::execute_action(request, logs_tx).await {
        Ok(response) => {
            tracing::info!(success = response.success, "execute request completed");
            Ok(Json(response))
        }
        Err(err) => {
            tracing::error!(?err, "execute request failed");
            let response = ExecuteResponse::error(format!("{:#}", err));
            Ok(Json(response))
        }
    }
}

/// Handler for GET /health endpoint.
pub async fn handle_health() -> &'static str {
    "OK"
}
