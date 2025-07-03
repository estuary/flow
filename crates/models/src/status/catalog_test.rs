use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{publications::PublicationStatus, Alerts};

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
pub struct TestStatus {
    pub passing: bool,
    #[serde(default)]
    pub publications: PublicationStatus,
    #[serde(default, skip_serializing_if = "Alerts::is_empty")]
    pub alerts: Alerts,
}
