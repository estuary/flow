use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    schemars::JsonSchema,
)]
#[cfg_attr(
    feature = "async-graphql",
    derive(async_graphql::Enum),
    graphql(rename_items = "snake_case")
)]
#[serde(rename_all = "snake_case")]
pub enum AlertType {
    AutoDiscoverFailed,
    DataMovementStalled,
    FreeTrial,
    FreeTrialEnding,
    FreeTrialStalled,
    MissingPaymentMethod,
    ShardFailed,
    TaskChronicallyFailing,
    TaskAutoDisabledFailing,
    TaskIdle,
    TaskAutoDisabledIdle,
    BackgroundPublicationFailed,
}

impl std::fmt::Display for AlertType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl AlertType {
    pub fn name(&self) -> &'static str {
        match self {
            AlertType::AutoDiscoverFailed => "auto_discover_failed",
            AlertType::ShardFailed => "shard_failed",
            AlertType::DataMovementStalled => "data_movement_stalled",
            AlertType::FreeTrial => "free_trial",
            AlertType::FreeTrialEnding => "free_trial_ending",
            AlertType::FreeTrialStalled => "free_trial_stalled",
            AlertType::MissingPaymentMethod => "missing_payment_method",
            AlertType::TaskChronicallyFailing => "task_chronically_failing",
            AlertType::TaskAutoDisabledFailing => "task_auto_disabled_failing",
            AlertType::TaskIdle => "task_idle",
            AlertType::TaskAutoDisabledIdle => "task_auto_disabled_idle",
            AlertType::BackgroundPublicationFailed => "background_publication_failed",
        }
    }

    pub fn all() -> &'static [AlertType] {
        &[
            AlertType::AutoDiscoverFailed,
            AlertType::ShardFailed,
            AlertType::TaskChronicallyFailing,
            AlertType::TaskAutoDisabledFailing,
            AlertType::TaskIdle,
            AlertType::TaskAutoDisabledIdle,
            AlertType::DataMovementStalled,
            AlertType::FreeTrial,
            AlertType::FreeTrialEnding,
            AlertType::FreeTrialStalled,
            AlertType::MissingPaymentMethod,
            AlertType::BackgroundPublicationFailed,
        ]
    }

    /// For alerts that are evaluated by querying a database view, this returns the
    /// view name. Returns None for alerts that are evaluated by controllers.
    pub fn view_name(&self) -> Option<&'static str> {
        match *self {
            AlertType::AutoDiscoverFailed => None,
            AlertType::DataMovementStalled => Some("alert_data_movement_stalled"),
            AlertType::FreeTrial => Some("tenant_alerts"),
            AlertType::FreeTrialEnding => Some("tenant_alerts"),
            AlertType::FreeTrialStalled => Some("tenant_alerts"),
            AlertType::MissingPaymentMethod => Some("tenant_alerts"),
            AlertType::ShardFailed => None,
            AlertType::TaskChronicallyFailing => None,
            AlertType::TaskAutoDisabledFailing => None,
            AlertType::TaskIdle => None,
            AlertType::TaskAutoDisabledIdle => None,
            AlertType::BackgroundPublicationFailed => None,
        }
    }

    /// A short, user-facing alert type name.
    pub fn display_name(&self) -> &'static str {
        match self {
            AlertType::AutoDiscoverFailed => "Auto-Discovery Failed",
            AlertType::DataMovementStalled => "Data Movement Stalled",
            AlertType::FreeTrial => "Free Trial",
            AlertType::FreeTrialEnding => "Free Trial Ending",
            AlertType::FreeTrialStalled => "Free Trial Stalled",
            AlertType::MissingPaymentMethod => "Missing Payment Method",
            AlertType::ShardFailed => "Task Failed",
            AlertType::TaskChronicallyFailing => "Task Chronically Failing",
            AlertType::TaskAutoDisabledFailing => "Task Auto-Disabled (Failing)",
            AlertType::TaskIdle => "Task Idle",
            AlertType::TaskAutoDisabledIdle => "Task Auto-Disabled (Idle)",
            AlertType::BackgroundPublicationFailed => "Background Publication Failed",
        }
    }

    /// A user-facing description of what this alert type means.
    pub fn description(&self) -> &'static str {
        match self {
            AlertType::AutoDiscoverFailed => {
                "Triggers when a capture's automated schema discovery fails. The capture may be unable to respond to schema changes in the source system."
            }
            AlertType::DataMovementStalled => {
                "Triggers when a task has not processed any data during its configured alert interval."
            }
            AlertType::FreeTrial => {
                "Triggers when a free trial begins, and resolves when the trial period ends."
            }
            AlertType::FreeTrialEnding => {
                "Triggers when a free trial is getting close to expiring."
            }
            AlertType::FreeTrialStalled => {
                "Triggers when a free trial has expired and no payment method has been added."
            }
            AlertType::MissingPaymentMethod => {
                "Triggers when no payment method is on file, and resolves when one is added."
            }
            AlertType::ShardFailed => {
                "Triggers when a task has experienced repeated failures. It may still make progress, but performance is degraded."
            }
            AlertType::TaskChronicallyFailing => {
                "Triggers when a task has been unable to run for an extended period. It will be automatically disabled unless the issue is addressed."
            }
            AlertType::TaskAutoDisabledFailing => {
                "Triggers when a task is automatically disabled after failing continuously for an extended period."
            }
            AlertType::TaskIdle => {
                "Triggers when a task has not processed any data for an extended period and has not been modified recently. It will be automatically disabled unless a new version is published."
            }
            AlertType::TaskAutoDisabledIdle => {
                "Triggers when a task is automatically disabled after being idle for an extended period without any spec changes."
            }
            AlertType::BackgroundPublicationFailed => {
                "Triggers when an automated background publication fails. Affected tasks are unlikely to function until the issue is addressed."
            }
        }
    }

    pub fn from_str(name: &str) -> Option<AlertType> {
        for alert_type in AlertType::all() {
            if name.eq_ignore_ascii_case(alert_type.name()) {
                return Some(*alert_type);
            }
        }
        None
    }

    /// An indication of whether the alert type is subscribed to by default.
    pub fn is_default(&self) -> bool {
        match self {
            AlertType::AutoDiscoverFailed => false,
            AlertType::BackgroundPublicationFailed => false,
            AlertType::DataMovementStalled => true,
            AlertType::FreeTrial => true,
            AlertType::FreeTrialEnding => true,
            AlertType::FreeTrialStalled => true,
            AlertType::MissingPaymentMethod => true,
            AlertType::ShardFailed => false,
            AlertType::TaskAutoDisabledFailing => false,
            AlertType::TaskAutoDisabledIdle => false,
            AlertType::TaskChronicallyFailing => false,
            AlertType::TaskIdle => false,
        }
    }

    /// An indication of whether the alert type is considered to be a system alert.
    pub fn is_system(&self) -> bool {
        match self {
            AlertType::AutoDiscoverFailed => false,
            AlertType::BackgroundPublicationFailed => false,
            AlertType::DataMovementStalled => false,
            AlertType::FreeTrial => true,
            AlertType::FreeTrialEnding => true,
            AlertType::FreeTrialStalled => true,
            AlertType::MissingPaymentMethod => true,
            AlertType::ShardFailed => false,
            AlertType::TaskAutoDisabledFailing => false,
            AlertType::TaskAutoDisabledIdle => false,
            AlertType::TaskChronicallyFailing => false,
            AlertType::TaskIdle => false,
        }
    }
}

#[cfg(feature = "sqlx-support")]
impl sqlx::Type<sqlx::postgres::Postgres> for AlertType {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("alert_type")
    }
}

#[cfg(feature = "sqlx-support")]
impl sqlx::Encode<'_, sqlx::postgres::Postgres> for AlertType {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'_, sqlx::Postgres>>::encode_by_ref(&self.name(), buf)
    }
}

#[cfg(feature = "sqlx-support")]
impl sqlx::Decode<'_, sqlx::postgres::Postgres> for AlertType {
    fn decode(value: sqlx::postgres::PgValueRef<'_>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <&str as sqlx::Decode<sqlx::postgres::Postgres>>::decode(value)?;
        AlertType::from_str(s).ok_or_else(|| format!("Invalid alert_type: {}", s).into())
    }
}

#[cfg(feature = "sqlx-support")]
impl sqlx::postgres::PgHasArrayType for AlertType {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("_alert_type")
    }
}

// TODO(phil): The aliases here can be removed once controllers have run for all currently alerting live specs.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum AlertState {
    #[serde(alias = "Firing")]
    /// The alert is currently firing.
    Firing,
    #[serde(alias = "Resolved")]
    /// The alert has resolved. Resolved alerts may be retained in the status for a short while.
    Resolved,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ControllerAlert {
    /// The current state of the alert.
    pub state: AlertState,
    /// The live spec type
    pub spec_type: crate::CatalogType,
    /// The time when the alert first triggered.
    #[schemars(schema_with = "crate::datetime_schema")]
    pub first_ts: DateTime<Utc>,
    /// The time that the alert condition was last checked or updated.
    #[schemars(schema_with = "crate::option_datetime_schema")]
    pub last_ts: Option<DateTime<Utc>>,
    /// The error message associated with the alert.
    pub error: String,
    /// The number of failures.
    pub count: u32,
    /// The time at which the alert condition resolved. Unset if the alert is firing.
    #[schemars(schema_with = "crate::option_datetime_schema")]
    pub resolved_at: Option<DateTime<Utc>>,
    /// Additional data available as `arguments.<key>` in notification email
    /// templates. Serialized with `#[serde(flatten)]` so keys become top-level
    /// JSON fields alongside `state`, `first_ts`, etc. Avoid using keys that
    /// collide with the struct's own field names.
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

pub type Alerts = BTreeMap<AlertType, ControllerAlert>;
