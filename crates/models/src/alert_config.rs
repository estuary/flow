use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Per-prefix or per-task alert configuration.
///
/// All fields are optional. Unset fields inherit from broader matching
/// prefixes and then from controller defaults.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AlertConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_movement_stalled: Option<DataMovementStalledConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_failed: Option<ShardFailedConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_chronically_failing: Option<TaskChronicallyFailingConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_idle: Option<TaskIdleConfig>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DataMovementStalledConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<DataMovementStalledCondition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DataMovementStalledCondition {
    /// How long to wait for data movement before the alert fires.
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(schema_with = "crate::duration_schema")]
    pub stalled_for: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ShardFailedConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<ShardFailedCondition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ShardFailedCondition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failures: Option<u32>,
    /// Time window over which `failures` is evaluated; failures older than
    /// this are discarded.
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(schema_with = "crate::duration_schema")]
    pub per: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskChronicallyFailingConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Whether to disable the failing task
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_disable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<TaskChronicallyFailingCondition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskChronicallyFailingCondition {
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(schema_with = "crate::duration_schema")]
    pub failing_for: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskIdleConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Whether to disable the failing task
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_disable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<TaskIdleCondition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskIdleCondition {
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(schema_with = "crate::duration_schema")]
    pub idle_for: Option<Duration>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn roundtrip_full() {
        let cfg = AlertConfig {
            data_movement_stalled: Some(DataMovementStalledConfig {
                enabled: Some(true),
                condition: Some(DataMovementStalledCondition {
                    stalled_for: Some(Duration::from_secs(7200)),
                }),
            }),
            shard_failed: Some(ShardFailedConfig {
                enabled: Some(true),
                condition: Some(ShardFailedCondition {
                    failures: Some(5),
                    per: Some(Duration::from_secs(14400)),
                }),
            }),
            task_chronically_failing: Some(TaskChronicallyFailingConfig {
                enabled: Some(false),
                auto_disable: Some(false),
                condition: Some(TaskChronicallyFailingCondition {
                    failing_for: Some(Duration::from_secs(60 * 60 * 24 * 7)),
                }),
            }),
            task_idle: Some(TaskIdleConfig {
                enabled: None,
                auto_disable: Some(true),
                condition: Some(TaskIdleCondition {
                    idle_for: Some(Duration::from_secs(60 * 60 * 24 * 14)),
                }),
            }),
        };
        let json = serde_json::to_value(&cfg).unwrap();
        insta::assert_json_snapshot!(json, @r#"
        {
          "dataMovementStalled": {
            "condition": {
              "stalledFor": "2h"
            },
            "enabled": true
          },
          "shardFailed": {
            "condition": {
              "failures": 5,
              "per": "4h"
            },
            "enabled": true
          },
          "taskChronicallyFailing": {
            "autoDisable": false,
            "condition": {
              "failingFor": "7days"
            },
            "enabled": false
          },
          "taskIdle": {
            "autoDisable": true,
            "condition": {
              "idleFor": "14days"
            }
          }
        }
        "#);

        let round: AlertConfig = serde_json::from_value(json).unwrap();
        assert_eq!(round, cfg);
    }

    #[test]
    fn enabled_roundtrips_at_each_sub_config() {
        let json = r#"{
            "shardFailed": {"enabled": false},
            "dataMovementStalled": {"enabled": true, "condition": {"stalledFor": "1h"}},
            "taskIdle": {"enabled": false},
            "taskChronicallyFailing": {"enabled": true}
        }"#;
        let cfg: AlertConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.shard_failed.as_ref().unwrap().enabled, Some(false));
        assert_eq!(
            cfg.data_movement_stalled.as_ref().unwrap().enabled,
            Some(true)
        );
        assert_eq!(cfg.task_idle.as_ref().unwrap().enabled, Some(false));
        assert_eq!(
            cfg.task_chronically_failing.as_ref().unwrap().enabled,
            Some(true)
        );
    }

    #[test]
    fn empty_config_is_valid() {
        let cfg: AlertConfig =
            serde_json::from_str("{}").expect("empty object is a valid AlertConfig");
        assert_eq!(cfg, AlertConfig::default());
    }

    #[test]
    fn unknown_field_rejected() {
        let err = serde_json::from_str::<AlertConfig>(r#"{"unknownField": {}}"#)
            .expect_err("unknown field should be rejected");
        assert!(err.to_string().contains("unknown field"), "got {err}",);
    }

    #[test]
    fn unknown_nested_field_rejected() {
        let err = serde_json::from_str::<AlertConfig>(r#"{"taskIdle": {"typoField": "60d"}}"#)
            .expect_err("unknown nested field should be rejected");
        assert!(err.to_string().contains("unknown field"), "got {err}",);
    }

    #[test]
    fn partial_config_parses() {
        let cfg: AlertConfig =
            serde_json::from_str(r#"{"taskIdle": {"condition": {"idleFor": "14d"}}}"#).unwrap();
        assert_eq!(
            cfg.task_idle.unwrap().condition.unwrap().idle_for,
            Some(Duration::from_secs(60 * 60 * 24 * 14))
        );
        assert!(cfg.shard_failed.is_none());
    }

    #[test]
    fn humantime_formats_accepted() {
        let cfg: AlertConfig = serde_json::from_str(
            r#"{"dataMovementStalled": {"condition": {"stalledFor": "2h"}}, "shardFailed": {"condition": {"per": "8h"}}}"#,
        )
        .unwrap();
        assert_eq!(
            cfg.data_movement_stalled
                .unwrap()
                .condition
                .unwrap()
                .stalled_for,
            Some(Duration::from_secs(7200))
        );
        assert_eq!(
            cfg.shard_failed.unwrap().condition.unwrap().per,
            Some(Duration::from_secs(8 * 3600))
        );
    }
}
