use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::publications::{ActivationStatus, PublicationStatus};

/// The status of a collection controller
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
pub struct CollectionStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inferred_schema: Option<InferredSchemaStatus>,
    #[serde(default)]
    pub publications: PublicationStatus,
    #[serde(default)]
    pub activation: ActivationStatus,
}

/// Status of the inferred schema
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, JsonSchema)]
pub struct InferredSchemaStatus {
    /// The time at which the inferred schema was last published. This will only
    /// be present if the inferred schema was published at least once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "crate::datetime_schema")]
    pub schema_last_updated: Option<DateTime<Utc>>,
    /// The md5 sum of the inferred schema that was last published.
    /// Because the publications handler updates the model instead of the controller, it's
    /// technically possible for the published inferred schema to be more recent than the one
    /// corresponding to this hash. If that happens, we would expect a subsequent publication
    /// on the next controller run, which would update the hash but not actually modify the schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_md5: Option<String>,
}
