use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::publications::PublicationStatus;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
pub struct TestStatus {
    pub passing: bool,
    #[serde(default)]
    pub publications: PublicationStatus,
}
