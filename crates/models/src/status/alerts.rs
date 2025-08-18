use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, schemars::JsonSchema)]
#[cfg_attr(
    feature = "async-graphql",
    derive(async_graphql::Enum),
    graphql(rename_items = "lowercase")
)]
#[serde(rename_all = "snake_case")]
pub enum AlertType {
    AutoDiscoverFailed,
    ShardFailed,
    DataMovementStalled,
    FreeTrial,
    FreeTrialEnding,
    FreeTrialStalled,
    MissingPaymentMethod,
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
        }
    }

    fn all() -> &'static [AlertType] {
        &[
            AlertType::AutoDiscoverFailed,
            AlertType::ShardFailed,
            AlertType::DataMovementStalled,
            AlertType::FreeTrial,
            AlertType::FreeTrialEnding,
            AlertType::FreeTrialStalled,
            AlertType::MissingPaymentMethod,
        ]
    }

    pub fn from_str(name: &str) -> Option<AlertType> {
        for alert_type in AlertType::all() {
            if name.eq_ignore_ascii_case(alert_type.name()) {
                return Some(*alert_type);
            }
        }
        None
    }
}

// These custom serde impls exist only to ensure that the serde
// representations are consistent with the `name()`.
impl serde::Serialize for AlertType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.name().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for AlertType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let str_val = String::deserialize(deserializer)?;
        AlertType::from_str(&str_val).ok_or(serde::de::Error::custom(format!(
            "invalid alert type: '{str_val}'"
        )))
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
    fn encode_by_ref(&self, buf: &mut sqlx::postgres::PgArgumentBuffer) -> sqlx::encode::IsNull {
        self.name().encode_by_ref(buf)
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, schemars::JsonSchema)]
pub enum AlertState {
    /// The alert is currently firing.
    Firing,
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
    // Allows passing arbitrary data as alert arguments, which will be available in alert message templates.
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

pub type Alerts = HashMap<AlertType, ControllerAlert>;
