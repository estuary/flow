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

/// Configuration for the `DataMovementStalled` alert.
///
/// This alert is evaluated only when `threshold` is set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DataMovementStalledConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    /// How long the task must go with no data movement before the alert fires.
    #[schemars(schema_with = "crate::duration_schema")]
    pub threshold: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ShardFailedConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Number of shard failures within `retention_window` required to fire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_threshold: Option<u32>,
    /// Time window over which `failure_threshold` is evaluated; failures
    /// older than this are discarded.
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(schema_with = "crate::duration_schema")]
    pub retention_window: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskChronicallyFailingConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(schema_with = "crate::duration_schema")]
    pub threshold: Option<Duration>,
    /// When the grace period expires after this alert first fires, disable
    /// the task's shards via a controller publication. Defaults to the
    /// global `DISABLE_FAILING_TASKS` env.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_disable: Option<bool>,
}

/// data for `threshold`
/// `enabled: false` to silence entirely.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
    /// defers to the alert's baseline (firing).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(schema_with = "crate::duration_schema")]
    pub threshold: Option<Duration>,
    /// When the grace period expires after this alert first fires, disable
    /// the task's shards via a controller publication. Defaults to the
    /// global `DISABLE_IDLE_TASKS` env.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_disable: Option<bool>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn roundtrip_full() {
        let cfg = AlertConfig {
            data_movement_stalled: Some(DataMovementStalledConfig {
                enabled: Some(true),
                threshold: Some(Duration::from_secs(7200)),
            }),
            shard_failed: Some(ShardFailedConfig {
                enabled: Some(true),
                failure_threshold: Some(5),
                retention_window: Some(Duration::from_secs(14400)),
            }),
            task_chronically_failing: Some(TaskChronicallyFailingConfig {
                enabled: Some(false),
                threshold: Some(Duration::from_secs(60 * 60 * 24 * 7)),
                auto_disable: Some(false),
            }),
            task_idle: Some(TaskIdleConfig {
                enabled: None,
                threshold: Some(Duration::from_secs(60 * 60 * 24 * 14)),
                auto_disable: Some(true),
            }),
        };
        let json = serde_json::to_value(&cfg).unwrap();
        insta::assert_json_snapshot!(json, @r#"
        {
          "dataMovementStalled": {
            "enabled": true,
            "threshold": "2h"
          },
          "shardFailed": {
            "enabled": true,
            "failureThreshold": 5,
            "retentionWindow": "4h"
          },
          "taskChronicallyFailing": {
            "autoDisable": false,
            "enabled": false,
            "threshold": "7days"
          },
          "taskIdle": {
            "autoDisable": true,
            "threshold": "14days"
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
            "dataMovementStalled": {"enabled": true, "threshold": "1h"},
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
            serde_json::from_str(r#"{"taskIdle": {"threshold": "14d"}}"#).unwrap();
        assert_eq!(
            cfg.task_idle.unwrap().threshold,
            Some(Duration::from_secs(60 * 60 * 24 * 14))
        );
        assert!(cfg.shard_failed.is_none());
    }

    #[test]
    fn humantime_formats_accepted() {
        let cfg: AlertConfig = serde_json::from_str(
            r#"{"dataMovementStalled": {"threshold": "2h"}, "shardFailed": {"retentionWindow": "8h"}}"#,
        )
        .unwrap();
        assert_eq!(
            cfg.data_movement_stalled.unwrap().threshold,
            Some(Duration::from_secs(7200))
        );
        assert_eq!(
            cfg.shard_failed.unwrap().retention_window,
            Some(Duration::from_secs(8 * 3600))
        );
    }
}
