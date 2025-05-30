use crate::{
    status::{connector::ConnectorStatus, ControllerStatus},
    CatalogType, Id,
};

/// A machine-readable summary of the status
///
/// This summary is derived from multiple different sources of information about
/// a catalog item, and it attempts to coalesce all that information into a
/// single, simple characterization. The term "status" can mean different
/// things, but here we're primarily concerned with answering the question: "do
/// we see any problems that might be affecting the correct operation of the
/// task".
#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
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

        if disabled {
            return Summary {
                status: StatusSummaryType::TaskDisabled,
                message: "Task shards are disabled".to_string(),
            };
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

        // Has there been a shard failure that hasn't been re-tried yet?
        if let Some(next) = activation_status.next_retry {
            let fail_ts = activation_status
                .last_failure
                .as_ref()
                .map(|fail| format!(" at {}", fail.ts))
                .unwrap_or_default();
            return Summary::error(format!("task shard failed{fail_ts}, next retry at {next}"));
        }

        // If this is a collection or a derivation, then skip checks of the connector status.
        // We'll need to update this once start emitting connector status for derivations.
        if controller_status.catalog_type() == Some(CatalogType::Collection) {
            return Summary {
                status: StatusSummaryType::Ok,
                message: "Ok".to_string(),
            };
        }

        // Has there been a connector status written since the task was last
        // activated? We ignore health checks from prior builds because
        // technically it's possible for an old version of a task shard to log a
        // connector status after we've activated the new version. This still
        // isn't a guarantee that all task shards have logged a connector
        // status, but it's considered "goodenuf for now" as a simple health
        // check.
        let connector_status_at = connector_status
            .filter(|s| s.shard.build == activation_status.last_activated)
            .map(|s| s.ts);
        if connector_status_at < activation_status.last_activated_at {
            return Summary::warning("waiting on connector health check");
        }

        Summary {
            status: StatusSummaryType::Ok,
            message: "Ok".to_string(),
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
        activation::{ActivationStatus, ShardFailure},
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
        let disabled = Summary::of(true, last_build, no_error, Some(&blank_capture), None);
        insta::assert_debug_snapshot!(disabled, @r###"
        Summary {
            status: TaskDisabled,
            message: "Task shards are disabled",
        }
        "###);

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
            },
            ..Default::default()
        });
        let no_connector_status =
            Summary::of(false, last_build, no_error, Some(&activated_ok), None);
        insta::assert_debug_snapshot!(no_connector_status, @r###"
        Summary {
            status: Warning,
            message: "waiting on connector health check",
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
            message: "waiting on connector health check",
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
            message: "Ok",
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
