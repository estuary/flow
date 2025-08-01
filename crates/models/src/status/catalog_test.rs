use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{publications::PublicationStatus, Alerts};

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
#[cfg_attr(feature = "async-graphql", derive(async_graphql::SimpleObject))]
pub struct TestStatus {
    pub passing: bool,
    #[serde(default)]
    pub publications: PublicationStatus,
    #[serde(default, skip_serializing_if = "Alerts::is_empty")]
    pub alerts: Alerts,
}
