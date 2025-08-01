use crate::discovers::Changed;
use crate::draft_error;
use crate::evolutions::EvolvedCollection;
use crate::publications;
use crate::status::{
    activation::ActivationStatus, publications::PublicationStatus, PendingConfigUpdateStatus,
};
use crate::ResourcePath;
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::Alerts;

/// Status of a capture controller
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
#[cfg_attr(feature = "async-graphql", derive(async_graphql::SimpleObject))]
pub struct CaptureStatus {
    #[serde(default)]
    pub publications: PublicationStatus,
    #[serde(default)]
    pub activation: ActivationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_updates: Option<PendingConfigUpdateStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_discover: Option<AutoDiscoverStatus>,
    #[serde(default, skip_serializing_if = "Alerts::is_empty")]
    pub alerts: Alerts,
}

/// A capture binding that has changed as a result of a discover
#[derive(Debug, Serialize, Deserialize, PartialEq, JsonSchema, Clone)]
#[cfg_attr(feature = "async-graphql", derive(async_graphql::SimpleObject))]
pub struct DiscoverChange {
    /// Identifies the resource in the source system that this change pertains to.
    pub resource_path: ResourcePath,
    /// The target collection of the capture binding that was changed.
    pub target: crate::Collection,
    /// Whether the capture binding is disabled.
    pub disable: bool,
}

impl DiscoverChange {
    pub fn new(resource_path: ResourcePath, Changed { target, disable }: Changed) -> Self {
        Self {
            resource_path,
            target,
            disable,
        }
    }
}

/// The results of an auto-discover attempt
#[derive(Debug, Serialize, Deserialize, PartialEq, JsonSchema, Clone)]
#[cfg_attr(feature = "async-graphql", derive(async_graphql::SimpleObject))]
pub struct AutoDiscoverOutcome {
    /// Time at which the disocver was attempted
    #[schemars(schema_with = "crate::datetime_schema")]
    pub ts: DateTime<Utc>,
    /// Bindings that were added to the capture.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added: Vec<DiscoverChange>,
    /// Bindings that were modified, either to change the schema or the collection key.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub modified: Vec<DiscoverChange>,
    /// Bindings that were removed because they no longer appear in the source system.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed: Vec<DiscoverChange>,
    /// Errors that occurred during the discovery or evolution process.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<draft_error::Error>,
    /// Collections that were re-created due to the collection key having changed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub re_created_collections: Vec<EvolvedCollection>,
    /// The result of publishing the discovered changes, if a publication was attempted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish_result: Option<publications::JobStatus>,
}

impl AutoDiscoverOutcome {
    /// Returns true if this represents a successfull auto-discover, meaning
    /// that the discover itself was successful, and either we were able to
    /// publish the changes, or there was no publication necessary.
    pub fn is_successful(&self) -> bool {
        self.get_result().is_ok()
    }

    /// Returns an `Err` if any part of the auto-discover failed. Returns `Ok`
    /// only if the auto-discover was successful.
    pub fn get_result(&self) -> anyhow::Result<()> {
        if let Some(first_err) = self.errors.get(0) {
            anyhow::bail!("auto-discover failed: {}", &first_err.detail);
        }
        if let Some(pub_result) = self
            .publish_result
            .as_ref()
            .filter(|r| !(r.is_success() || r.is_empty_draft()))
        {
            anyhow::bail!("auto-discover publication failed with: {:?}", pub_result)
        };
        Ok(())
    }

    pub fn has_changes(&self) -> bool {
        self.errors.is_empty()
            && (!self.added.is_empty() || !self.modified.is_empty() || !self.removed.is_empty())
    }

    pub fn error(
        ts: DateTime<Utc>,
        capture_name: &str,
        error: &anyhow::Error,
    ) -> AutoDiscoverOutcome {
        let errors = vec![draft_error::Error {
            catalog_name: capture_name.to_string(),
            detail: error.to_string(),
            scope: None,
        }];
        AutoDiscoverOutcome {
            ts,
            errors,
            added: Vec::new(),
            modified: Vec::new(),
            removed: Vec::new(),
            re_created_collections: Vec::new(),
            publish_result: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, JsonSchema, Clone)]
#[cfg_attr(feature = "async-graphql", derive(async_graphql::SimpleObject))]
pub struct AutoDiscoverFailure {
    /// The number of consecutive failures that have been observed.
    pub count: u32,
    /// The timestamp of the first failure in the current sequence.
    #[schemars(schema_with = "crate::datetime_schema")]
    pub first_ts: DateTime<Utc>,
    /// The discover outcome corresponding to the most recent failure. This will
    /// be updated with the results of each retry until an auto-discover
    /// succeeds.
    pub last_outcome: AutoDiscoverOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
#[cfg_attr(feature = "async-graphql", derive(async_graphql::SimpleObject))]
pub struct AutoDiscoverStatus {
    /// The interval at which auto-discovery is run. This is normally unset, which uses
    /// the default interval.
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(schema_with = "interval_schema")]
    #[cfg_attr(feature = "async-graphql", graphql(skip))]
    pub interval: Option<std::time::Duration>,

    /// Time at which the next auto-discover should be run.
    #[serde(default)]
    #[schemars(schema_with = "crate::datetime_schema")]
    pub next_at: Option<DateTime<Utc>>,
    /// The outcome of the a recent discover, which is about to be published.
    /// This will typically only be observed if the publication failed for some
    /// reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_publish: Option<AutoDiscoverOutcome>,
    /// The outcome of the last _successful_ auto-discover. If `failure` is set,
    /// then that will typically be more recent than `last_success`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_success: Option<AutoDiscoverOutcome>,
    /// If auto-discovery has failed, this will include information about that failure.
    /// This field is cleared as soon as a successful auto-discover is run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<AutoDiscoverFailure>,
}

fn interval_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "type": ["string", "null"],
        "pattern": "^\\d+(s|m|h)$"
    }))
    .unwrap()
}
