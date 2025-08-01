use crate::{
    status::{connector::ConnectorStatus, ControllerStatus},
    Id,
};

use super::activation::ShardsStatus;

/// A machine-readable summary of the status
///
/// This summary is derived from multiple different sources of information about
/// a catalog item, and it attempts to coalesce all that information into a
/// single, simple characterization. The term "status" can mean different
/// things, but here we're primarily concerned with answering the question: "do
/// we see any problems that might be affecting the correct operation of the
/// task".
#[derive(Debug, PartialEq, Eq, Clone, Copy, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[cfg_attr(feature = "async-graphql", derive(async_graphql::Enum))]
#[serde(rename_all = "camelCase")]
pub enum StatusSummaryType {
    /// Things seem ...not bad
    Ok,
    /// The task is currently disabled. Only pertains to captures, derivations,
    /// and materializations.
    TaskDisabled,
    /// Something isn't fully working, but the condition is expected to clear
    /// automatically soon. Nothing to worry about as long as the condition
    /// doesn't persist for too long.
    Warning,
    /// There's some sort of error with this catalog spec.
    Error,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[cfg_attr(feature = "async-graphql", derive(async_graphql::SimpleObject))]
#[serde(rename_all = "camelCase")]
pub struct Summary {
    pub status: StatusSummaryType,
    pub message: String,
}

impl Summary {
    pub fn of(
        disabled: bool,
        last_build_id: Id,
        controller_error: Option<&str>,
        controller_status: Option<&ControllerStatus>,
        connector_status: Option<&ConnectorStatus>,
    ) -> Summary {
        let Some(controller_status) = controller_status else {
            return Summary::warning("Pending controller initialization");
        };
        // If there's a controller error, return that first since it is likely
        // to be the most relevant and actionable. These errors would normally
        // include things like shard failures that haven't been handled yet, or
        // failed activations. But we also handle those specific cases
        // separately here, in case the controller hasn't had a chance to run
        // yet.
        if let Some(err) = controller_error {
            return Summary::error(err);
        }

        // Return early if there's no activation status
        let activation_status = match controller_status {
            ControllerStatus::Test(test_status) => {
                if test_status.passing {
                    return Summary {
                        status: StatusSummaryType::Ok,
                        message: "Test passed".to_string(),
                    };
                } else {
                    return Summary::error("Test failed");
                }
            }
            ControllerStatus::Uninitialized => {
                return Summary::warning("pending controller initialization");
            }
            ControllerStatus::Capture(s) => &s.activation,
            ControllerStatus::Collection(s) => &s.activation,
            ControllerStatus::Materialization(s) => &s.activation,
        };

        // Check whether the activated version is current with respect to the
        // build id of the most recent publication.
        if last_build_id != activation_status.last_activated
            || activation_status.last_activated_at.is_none()
        {
            return Summary::warning("pending data-plane activation");
        }
        let last_activation_ts = activation_status.last_activated_at.unwrap();

        let is_collection = matches!(controller_status, ControllerStatus::Collection(_));
        if disabled && !is_collection {
            return Summary {
                status: StatusSummaryType::TaskDisabled,
                message: "Task shards are disabled".to_string(),
            };
        }

        // Has there been a shard failure that hasn't been re-tried yet?
        if let Some(next) = activation_status.next_retry {
            let fail_ts = activation_status
                .last_failure
                .as_ref()
                .map(|fail| format!(" at {}", fail.ts))
                .unwrap_or_default();
            return Summary::error(format!("task shard failed{fail_ts}, next retry at {next}"));
        }

        // The `shard_health` will be `None` if this spec does not have task shards.
        if let Some(shard_health) = activation_status.shard_status.as_ref() {
            match shard_health.status {
                ShardsStatus::Ok => { /* pass */ }
                ShardsStatus::Pending if shard_health.count <= 20 => {
                    return Summary::warning("waiting for task shards to be ready");
                }
                ShardsStatus::Pending => {
                    return Summary::error("task shards have been pending for a long time");
                }
                ShardsStatus::Failed => {
                    return Summary::error("one or more task shards are failed");
                }
            }
        }

        // Not all tasks will have a connector status, but if they do then we
        // want to warn if we haven't observed one yet. If any connector status
        // has ever been materialized, then we'll warn if that status is stale,
        // since we'll expect a fresh status to have been emitted after the last
        // activation.
        // If the connector status is present, this will be the message of the
        // status summary. Otherwise, we'll just say "Ok". Yes, this means
        // that when a new task is first created, the status summary will show
        // as "Ok" before it ever writes a ConnectorStatus event. This seems
        // acceptable, since we have no better means of determining whether
        // to expect a connector status for a given task.
        let message = if let Some(conn_status) = connector_status.filter(|_| !disabled) {
            if !conn_status.is_current(last_build_id, last_activation_ts) {
                return Summary::warning("waiting on connector status");
            }
            conn_status.message.clone()
        } else {
            "Ok".to_string()
        };

        Summary {
            status: StatusSummaryType::Ok,
            message,
        }
    }

    fn warning(message: impl Into<String>) -> Summary {
        Summary {
            status: StatusSummaryType::Warning,
            message: message.into(),
        }
    }

    fn error(message: impl Into<String>) -> Summary {
        Summary {
            status: StatusSummaryType::Error,
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::status::{
        activation::{ActivationStatus, ShardFailure, ShardStatusCheck, ShardsStatus},
        capture::CaptureStatus,
        catalog_test::TestStatus,
        materialization::MaterializationStatus,
        ControllerStatus, ShardRef,
    };

    #[test]
    fn test_status() {
        let last_build = crate::Id::new([3u8; 8]);
        let some_error = Some("some error");
        let no_error: Option<&str> = None;

        let started_disabled = Summary::of(true, last_build, no_error, None, None);
        insta::assert_debug_snapshot!(started_disabled, @r###"
        Summary {
            status: Warning,
            message: "Pending controller initialization",
        }
        "###);

        let blank_capture = ControllerStatus::Capture(CaptureStatus::default());
        let controller_error =
            Summary::of(true, last_build, some_error, Some(&blank_capture), None);
        insta::assert_debug_snapshot!(controller_error, @r###"
        Summary {
            status: Error,
            message: "some error",
        }
        "###);

        let not_activated = ControllerStatus::Materialization(MaterializationStatus {
            activation: ActivationStatus {
                last_activated: crate::Id::new([2u8; 8]),
                last_activated_at: Some("2024-02-03T05:06:07Z".parse().unwrap()),
                last_failure: None,
                recent_failure_count: 999, // should be ignored
                next_retry: None,
                shard_status: None,
            },
            ..Default::default()
        });
        let pending_activate_new_build =
            Summary::of(false, last_build, no_error, Some(&not_activated), None);
        insta::assert_debug_snapshot!(pending_activate_new_build, @r###"
        Summary {
            status: Warning,
            message: "pending data-plane activation",
        }
        "###);

        let a_shard = ShardRef {
            name: "test/foo".to_string(),
            key_begin: "0000000000000000".to_string(),
            r_clock_begin: "0000000000000000".to_string(),
            build: last_build,
        };
        let activated_ok = ControllerStatus::Materialization(MaterializationStatus {
            activation: ActivationStatus {
                last_activated: last_build,
                last_activated_at: Some("2024-02-03T09:10:11Z".parse().unwrap()),
                last_failure: Some(ShardFailure {
                    shard: a_shard.clone(),
                    ts: "2024-02-03T06:07:08Z".parse().unwrap(),
                    message: "oh no it failed".to_string(),
                    fields: Default::default(),
                }),
                recent_failure_count: 999, // should be ignored
                next_retry: None,
                shard_status: Some(ShardStatusCheck {
                    count: 1,
                    first_ts: "2024-02-03T09:10:11Z".parse().unwrap(),
                    last_ts: "2024-02-03T09:10:11Z".parse().unwrap(),
                    status: ShardsStatus::Ok,
                }),
            },
            ..Default::default()
        });
        let no_connector_status =
            Summary::of(false, last_build, no_error, Some(&activated_ok), None);
        insta::assert_debug_snapshot!(no_connector_status, @r###"
        Summary {
            status: Ok,
            message: "Ok",
        }
        "###);
        let disabled = Summary::of(true, last_build, no_error, Some(&activated_ok), None);
        insta::assert_debug_snapshot!(disabled, @r###"
        Summary {
            status: TaskDisabled,
            message: "Task shards are disabled",
        }
        "###);

        let old_connector_status = ConnectorStatus {
            shard: ShardRef {
                build: crate::Id::new([2u8; 8]),
                ..a_shard.clone()
            },
            ts: "2024-02-03T09:11:12Z".parse().unwrap(),
            message: "connector is ready".to_string(),
            fields: Default::default(),
        };

        let pending_connector_ok = Summary::of(
            false,
            last_build,
            no_error,
            Some(&activated_ok),
            Some(&old_connector_status),
        );
        insta::assert_debug_snapshot!(pending_connector_ok, @r###"
        Summary {
            status: Warning,
            message: "waiting on connector status",
        }
        "###);

        let ok_connector_status = ConnectorStatus {
            shard: a_shard.clone(),
            ts: "2024-02-03T09:11:12Z".parse().unwrap(),
            message: "connector is ready".to_string(),
            fields: Default::default(),
        };

        let ok_status = Summary::of(
            false,
            last_build,
            no_error,
            Some(&activated_ok),
            Some(&ok_connector_status),
        );
        insta::assert_debug_snapshot!(ok_status, @r###"
        Summary {
            status: Ok,
            message: "connector is ready",
        }
        "###);

        // Everything else looks good, except there's a controller error
        let controller_error_two = Summary::of(
            false,
            last_build,
            some_error,
            Some(&activated_ok),
            Some(&ok_connector_status),
        );
        insta::assert_debug_snapshot!(controller_error_two, @r###"
        Summary {
            status: Error,
            message: "some error",
        }
        "###);

        // Catalog tests have slightly different handling
        let test_ok_status = Summary::of(
            false,
            last_build,
            no_error,
            Some(&ControllerStatus::Test(TestStatus {
                passing: true,
                publications: Default::default(),
                alerts: Default::default(),
            })),
            None,
        );
        insta::assert_debug_snapshot!(test_ok_status, @r###"
        Summary {
            status: Ok,
            message: "Test passed",
        }
        "###);

        let test_fail_status = Summary::of(
            false,
            last_build,
            no_error,
            Some(&ControllerStatus::Test(TestStatus {
                passing: false,
                publications: Default::default(),
                alerts: Default::default(),
            })),
            None,
        );
        insta::assert_debug_snapshot!(test_fail_status, @r###"
        Summary {
            status: Error,
            message: "Test failed",
        }
        "###);
    }
}
