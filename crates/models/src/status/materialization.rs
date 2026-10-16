use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::Alerts;
use super::PendingConfigUpdateStatus;
use super::activation::ActivationStatus;
use super::publications::PublicationStatus;

/// Status of a materialization controller
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
pub struct MaterializationStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_capture: Option<SourceCaptureStatus>,
    #[serde(default)]
    pub publications: PublicationStatus,
    #[serde(default)]
    pub activation: ActivationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_updates: Option<PendingConfigUpdateStatus>,
    #[serde(default, skip_serializing_if = "Alerts::is_empty")]
    pub alerts: Alerts,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub abandon: Option<super::AbandonStatus>,
    /// Bindings which Flow disabled, and which remain disabled.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub auto_disabled_bindings: Vec<AutoDisabledBinding>,
}

/// A binding which Flow disabled, and why.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[cfg_attr(feature = "async-graphql", derive(async_graphql::SimpleObject))]
pub struct AutoDisabledBinding {
    /// Resource path of the disabled binding.
    pub resource_path: Vec<String>,
    /// Source collection of the disabled binding.
    pub collection: crate::Collection,
    /// Why Flow disabled the binding.
    pub reason: AutoDisabledReason,
    /// Description of the change that caused Flow to disable the binding.
    pub detail: String,
    /// The publication which disabled the binding.
    pub pub_id: crate::Id,
}

impl AutoDisabledBinding {
    /// Summarizes `bindings` for status summaries and alerts.
    pub fn describe(bindings: &[AutoDisabledBinding]) -> String {
        match bindings.len() {
            1 => "1 binding disabled because of an incompatible schema change".to_string(),
            n => format!("{n} bindings disabled because of incompatible schema changes"),
        }
    }
}

/// Reason that Flow disabled a binding.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[cfg_attr(feature = "async-graphql", derive(async_graphql::Enum))]
#[serde(rename_all = "snake_case")]
pub enum AutoDisabledReason {
    /// A schema change was incompatible with the binding's target resource,
    /// and its `onIncompatibleSchemaChange` is `disableBinding`.
    IncompatibleSchemaChange,
}

/// Status information about the `sourceCapture`
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
#[cfg_attr(feature = "async-graphql", derive(async_graphql::SimpleObject))]
pub struct SourceCaptureStatus {
    /// Whether the materialization bindings are up-to-date with respect to
    /// the `sourceCapture` bindings. In normal operation, this should always
    /// be `true`. Otherwise, there will be a controller `error` and the
    /// publication status will contain details of why the update failed.
    #[serde(default)]
    pub up_to_date: bool,
    /// If `up_to_date` is `false`, then this will contain the set of
    /// `sourceCapture` collections that need to be added. This is provided
    /// simply to aid in debugging in case the publication to add the bindings
    /// fails.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub add_bindings: BTreeSet<crate::Collection>,
}
