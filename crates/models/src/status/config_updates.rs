use crate::datetime_schema;
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Information on the config updates performed by the controller.
/// This does not include any information on user-initiated config updates.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct PendingConfigUpdateStatus {
    #[schemars(schema_with = "datetime_schema")]
    pub next_attempt: Option<DateTime<Utc>>,
}
