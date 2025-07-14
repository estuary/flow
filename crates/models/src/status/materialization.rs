use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::activation::ActivationStatus;
use super::publications::PublicationStatus;
use super::Alerts;
use super::PendingConfigUpdateStatus;

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
}

/// Status information about the `sourceCapture`
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
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
