use crate::shared::stack::{State, Status};
use sqlx::types::uuid;

/// Action represents a state machine transition to execute.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum Action {
    SetEncryption,
    PulumiPreview,
    PulumiRefresh,
    PulumiUp1,
    AwaitDNS1,
    Ansible,
    PulumiUp2,
    AwaitDNS2,
}

impl Action {
    pub fn from_status(status: Status) -> Option<Self> {
        match status {
            Status::Idle => None,
            Status::SetEncryption => Some(Action::SetEncryption),
            Status::PulumiPreview => Some(Action::PulumiPreview),
            Status::PulumiRefresh => Some(Action::PulumiRefresh),
            Status::PulumiUp1 => Some(Action::PulumiUp1),
            Status::AwaitDNS1 => Some(Action::AwaitDNS1),
            Status::Ansible => Some(Action::Ansible),
            Status::PulumiUp2 => Some(Action::PulumiUp2),
            Status::AwaitDNS2 => Some(Action::AwaitDNS2),
        }
    }
}

/// ControllerConfig contains configuration needed by the service worker.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ControllerConfig {
    pub dns_ttl: std::time::Duration,
    pub dry_dock_remote: String,
    pub ops_remote: String,
    pub secrets_provider: String,
    pub state_backend: url::Url,
    pub dry_run: bool,
}

/// ExecuteRequest is sent from the Job to the Service to execute work.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExecuteRequest {
    pub task_id: models::Id,
    pub data_plane_id: models::Id,
    pub logs_token: uuid::Uuid,
    pub state: State,
    pub action: Action,
    pub controller_config: ControllerConfig,
}

/// ExecuteResponse is returned from the Service to the Job after executing work.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExecuteResponse {
    pub success: bool,
    pub next_state: Option<State>,
    pub sleep_duration_ms: u64,
    pub error: Option<String>,
}

impl ExecuteResponse {
    pub fn success(state: State, sleep_duration: std::time::Duration) -> Self {
        Self {
            success: true,
            next_state: Some(state),
            sleep_duration_ms: sleep_duration.as_millis() as u64,
            error: None,
        }
    }

    pub fn error(err: String) -> Self {
        Self {
            success: false,
            next_state: None,
            sleep_duration_ms: 0,
            error: Some(err),
        }
    }
}
