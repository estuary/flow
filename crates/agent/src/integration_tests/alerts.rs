use std::usize;

use crate::alerts::EvaluatorState;
use crate::integration_tests::harness::TestHarness;
use chrono::{DateTime, Utc};
use control_plane_api::alerts::{AlertAction, ResolveAlert, apply_alert_actions};
use models::status::AlertType;

#[tokio::test]
async fn test_alert_notifications() {
    let mut harness = TestHarness::init("alert_notifications").await;
    let _user_id = harness.setup_tenant("robins").await;
    let pool = harness.pool.clone();

    let happy_id = models::Id::new([8, 8, 8, 8, 8, 8, 8, 8]);
    sqlx::raw_sql(r#"
        insert into alert_history
          (id, catalog_name, alert_type, fired_at, resolved_at, arguments, resolved_arguments)
          values
            ('0808080808080808', 'robins/happy-path', 'shard_failed', '2025-08-09T10:11:05Z', null, '{
              "recipients": [
                {"email": "alerts@robins.test"},
                {"email": "robin@robins.test", "full_name": "Robin Robin Jr"}
              ],
              "error": "omg",
              "first_ts": "2025-08-09T10:11:05Z",
              "count": 3,
              "spec_type": "collection"
            }', null),
            ('0404040404040404', 'robins/failures', 'shard_failed', '2025-08-09T10:11:05Z', null, '{
                "recipients": [
                  {"email": "alerts@robins.test"},
                  {"email": "robin@robins.test", "full_name": "Robin Robin Jr"}
                ],
                "error": "omg",
                "first_ts": "2025-08-09T10:11:05Z",
                "count": 3,
                "spec_type": "collection"
            }', null),
            ('0202020202020202', 'robins/starting-resolved', 'shard_failed', '2025-08-09T10:11:05Z', '2025-08-09T13:14:15Z', '{
                "recipients": [
                  {"email": "alerts@robins.test"},
                  {"email": "robin@robins.test", "full_name": "Robin Robin Jr"}
                ],
                "error": "omg",
                "first_ts": "2025-08-09T10:11:05Z",
                "count": 3,
                "spec_type": "collection"
            }', null),
            ('0101010101010101', 'robins/undefined-recipients', 'shard_failed', '2012-08-09T10:11:05Z', null, '{
                "error": "omg",
                "first_ts": "2025-08-09T10:11:05Z",
                "count": 3,
                "spec_type": "collection"
            }', null),
            ('0103030303030303', 'robins/empty-recipients', 'shard_failed', '2012-08-09T10:11:05Z', null, '{
                "recipients": [],
                "error": "omg",
                "first_ts": "2025-08-09T10:11:05Z",
                "count": 3,
                "spec_type": "collection"
            }', null),
            ('0105050505050505', 'robins/null-recipients', 'shard_failed', '2012-08-09T10:11:05Z', null, '{
                "recipients": null,
                "error": "omg",
                "first_ts": "2025-08-09T10:11:05Z",
                "count": 3,
                "spec_type": "collection"
            }', null);

            insert into internal.tasks (task_id, task_type)
            select id, 9 from alert_history;
        "#).execute(&pool).await.unwrap();

    // The happy-path alert should just send emails after the alert is fired and subsequently resolved.
    let happy_fired = harness
        .assert_alert_firing("robins/happy-path", AlertType::ShardFailed)
        .await;
    assert!(
        happy_fired
            .expect_notification_state()
            .fired_completed
            .is_some(),
        "expected fired_completed to be Some in: {happy_fired:?}"
    );
    happy_fired.assert_emails_sent(&["alerts@robins.test", "robin@robins.test"]);
    let happy_wake_at = harness.get_task_wake_at(happy_id).await;
    assert!(happy_wake_at.is_none(), "task should be suspended");

    let resolved_at: DateTime<Utc> = "2025-09-10T11:12:13Z".parse().unwrap();
    let resolve = ResolveAlert {
        id: happy_id,
        catalog_name: "robins/happy-path".to_string(),
        alert_type: AlertType::ShardFailed,
        resolved_at,
        base_resolved_arguments: None,
    };
    resolve_alert(resolve, &pool).await;
    let happy_resolved = harness.assert_alert_resolved(happy_id).await;
    happy_resolved.assert_emails_sent(&["alerts@robins.test", "robin@robins.test"]);
    assert_task_deleted(happy_id, &mut harness).await;

    // Test failures in sending notifications
    let failures_id = models::Id::new([4, 4, 4, 4, 4, 4, 4, 4]);
    harness.alert_sender.set_fail_after(0).await;
    let failed_run_1 = harness
        .assert_alert_firing("robins/failures", AlertType::ShardFailed)
        .await;
    assert!(failed_run_1.notifications.is_empty());
    assert_eq!(failures_id, failed_run_1.alert.id);
    insta::assert_json_snapshot!(failed_run_1.notification_state, @r#"
    {
      "failures": 1,
      "fired_completed": null,
      "last_error": "sending alert notification with idempotency key '0404040404040404-fired-0': mock error sending alert email '0404040404040404-fired-0'",
      "max_idempotency_key": null
    }
    "#);
    assert_wake_at_within_minutes(failures_id, 1, 3, &mut harness).await;

    // Now allow sending one email successfully before failing again.
    harness.alert_sender.set_fail_after(1).await;
    let failed_run_2 = harness
        .assert_alert_firing("robins/failures", AlertType::ShardFailed)
        .await;
    failed_run_2.assert_emails_sent(&["alerts@robins.test"]);
    insta::assert_json_snapshot!(failed_run_2.notification_state, @r#"
    {
      "failures": 2,
      "fired_completed": null,
      "last_error": "sending alert notification with idempotency key '0404040404040404-fired-1': mock error sending alert email '0404040404040404-fired-1'",
      "max_idempotency_key": "0404040404040404-fired-0"
    }
    "#);

    // One more success is all we need in order to finish sending all of the fired emails.
    let fire_success = harness
        .assert_alert_firing("robins/failures", AlertType::ShardFailed)
        .await;
    fire_success.assert_emails_sent(&["robin@robins.test"]);
    let state = fire_success.expect_notification_state();
    assert_eq!(0, state.failures);
    assert!(state.last_error.is_none());
    assert!(state.fired_completed.is_some());

    let nothing_to_do = harness
        .assert_alert_firing("robins/failures", AlertType::ShardFailed)
        .await;
    nothing_to_do.assert_emails_sent(&[]);
    let state = nothing_to_do.expect_notification_state();
    assert_eq!(0, state.failures);
    assert!(state.last_error.is_none());
    assert!(state.fired_completed.is_some());

    resolve_alert(
        ResolveAlert {
            id: failures_id,
            catalog_name: "robins/failures".to_string(),
            alert_type: AlertType::ShardFailed,
            resolved_at,
            base_resolved_arguments: None,
        },
        &pool,
    )
    .await;

    let resolve_fail = harness.assert_alert_resolved(failures_id).await;
    resolve_fail.assert_emails_sent(&["alerts@robins.test"]);
    let state = resolve_fail.expect_notification_state();
    assert_eq!(1, state.failures);
    assert_eq!(
        Some(
            "sending alert notification with idempotency key '0404040404040404-resolved-1': mock error sending alert email '0404040404040404-resolved-1'"
        ),
        state.last_error.as_deref()
    );
    assert!(state.fired_completed.is_some(), "should still be Some");
    assert_eq!(
        Some("0404040404040404-resolved-0"),
        state.max_idempotency_key.as_deref()
    );
    assert_wake_at_within_minutes(failures_id, 1, 3, &mut harness).await;

    // Simulate a repeated failure trying to send the second email
    harness.alert_sender.set_fail_after(0).await;
    let resolve_fail_2 = harness.assert_alert_resolved(failures_id).await;
    let fail_2_state = resolve_fail_2.expect_notification_state();
    assert_eq!(state.last_error, fail_2_state.last_error);
    assert_eq!(2, fail_2_state.failures);
    assert_wake_at_within_minutes(failures_id, 2, 6, &mut harness).await;

    // One more successful send gets it across the finish line
    harness.alert_sender.set_fail_after(1).await;
    let resolve_success = harness.assert_alert_resolved(failures_id).await;
    resolve_success.assert_emails_sent(&["robin@robins.test"]);
    assert_task_deleted(failures_id, &mut harness).await;

    // Test an alert which is already resolved the first time it's observed.
    // This might happen due to some delay in running our automations, for example.
    harness.alert_sender.set_fail_after(usize::MAX).await;
    let starting_resolved_id = models::Id::new([2, 2, 2, 2, 2, 2, 2, 2]);
    let started_resolved = harness.assert_alert_resolved(starting_resolved_id).await;
    started_resolved.assert_emails_sent(&["alerts@robins.test", "robin@robins.test"]);
    assert_task_deleted(starting_resolved_id, &mut harness).await;

    /*
     * It's common for a given alert to not have any subscribers, which results
     * in an alert_history row with arguments that have no `recipients`. Test
     * various representations of that, and assert that the notification task
     * moves through the normal state transitions, just without sending any
     * emails.
     */
    let no_subscriber_ids = [
        (models::Id::new([1; 8]), "robins/undefined-recipients"),
        (
            models::Id::new([1, 3, 3, 3, 3, 3, 3, 3]),
            "robins/empty-recipients",
        ),
        (
            models::Id::new([1, 5, 5, 5, 5, 5, 5, 5]),
            "robins/null-recipients",
        ),
    ];
    for (task_id, catalog_name) in no_subscriber_ids {
        let fired_state = harness
            .assert_alert_firing(catalog_name, AlertType::ShardFailed)
            .await;
        fired_state.assert_emails_sent(&[]);
        let state = fired_state.expect_notification_state();
        assert!(state.fired_completed.is_some());
        assert!(state.last_error.is_none());
        assert_eq!(0, state.failures);

        resolve_alert(
            ResolveAlert {
                id: task_id,
                catalog_name: catalog_name.to_string(),
                alert_type: AlertType::ShardFailed,
                resolved_at,
                base_resolved_arguments: None,
            },
            &pool,
        )
        .await;
        let resolved_state = harness.assert_alert_resolved(task_id).await;
        resolved_state.assert_emails_sent(&[]);
        assert_task_deleted(task_id, &mut harness).await;
    }
}

async fn assert_task_deleted(task_id: models::Id, harness: &mut TestHarness) {
    let rows = sqlx::query_scalar!(
        r#"select count(*) as "count!: i64" from internal.tasks where task_id = $1"#,
        task_id as models::Id
    )
    .fetch_one(&harness.pool)
    .await
    .expect("failed to query tasks");
    assert_eq!(0, rows, "expected task {task_id} to be deleted");
}

async fn assert_wake_at_within_minutes(
    task_id: models::Id,
    min_minutes: i64,
    max_minutes: i64,
    harness: &mut TestHarness,
) {
    let Some(wake_at) = harness.get_task_wake_at(task_id).await else {
        panic!("expected non-null wake_at for task {task_id}");
    };
    let diff = wake_at - Utc::now();
    assert!(
        diff <= chrono::Duration::minutes(max_minutes),
        "expected wake_at to be within {max_minutes} minutes, but was at {wake_at} ({:?})",
        diff
    );
    assert!(
        diff >= chrono::Duration::minutes(min_minutes),
        "expected wake_at to be at least {min_minutes} minutes, but was at {wake_at} ({:?})",
        diff
    );
}

// Updates the alert_history table to resolve the given alert and send a resolved message to the
// notification task.
async fn resolve_alert(resolve: ResolveAlert, pool: &sqlx::PgPool) {
    let mut txn = pool.begin().await.unwrap();
    apply_alert_actions(vec![AlertAction::Resolve(resolve)], &mut *txn)
        .await
        .expect("failed to resolve alert");
    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_data_movement_stalled() {
    let mut harness = TestHarness::init("test_data_movement_stalled").await;
    let pool = harness.pool.clone();

    sqlx::raw_sql(r#"
        insert into alert_subscriptions (catalog_prefix, email, include_alert_types)
        values ('aliceCo/', 'alice@example.com', array['data_movement_stalled'::alert_type]),
        ('aliceCo/', 'bob@example.com', array['data_movement_stalled'::alert_type]),
        ('aliceCo/', 'carol@example.com', array['shard_failed'::alert_type]);

        insert into alert_data_processing (catalog_name, evaluation_interval) values
          ('aliceCo/capture/three-hours', '2 hours'),
          ('aliceCo/capture/two-hours', '2 hours'),
          ('aliceCo/capture/deleted', '2 hours'),
          ('aliceCo/materialization/four-hours', '4 hours'),
          ('aliceCo/materialization/legacy', '2 hours'),
          ('aliceCo/materialization/disabled', '2 hours');

          with insert_live as (
          insert into live_specs (catalog_name, spec_type, spec, created_at) values
              ('aliceCo/capture/three-hours', 'capture', '{
                  "endpoint": {
                  "connector": {
                      "image": "some image",
                      "config": {"some": "config"}
                      }
                  },
                  "bindings": []
              }', now() - '3h'::interval),
              ('aliceCo/capture/two-hours', 'capture', '{
                  "endpoint": {
                  "connector": {
                      "image": "some image",
                      "config": {"some": "config"}
                      }
                  },
                  "bindings": []
              }', now() - '2h'::interval),
              ('aliceCo/capture/deleted', 'capture', null, now() - '3h'::interval),
              ('aliceCo/materialization/legacy', 'materialization', '{
                  "endpoint": {
                  "connector": {
                      "image": "some image",
                      "config": {"some": "config"}
                      }
                  },
                  "bindings": []
              }', now() - '4h'::interval),
              ('aliceCo/materialization/four-hours', 'materialization', '{
                  "endpoint": {
                  "connector": {
                      "image": "some image",
                      "config": {"some": "config"}
                      }
                  },
                  "bindings": []
              }', now() - '4h'::interval),
              ('aliceCo/materialization/disabled', 'materialization', '{
                  "endpoint": {
                  "connector": {
                      "image": "some image",
                      "config": {"some": "config"}
                      }
                  },
                  "bindings": [],
                  "shards": { "disable": true }
              }', now() - '3h'::interval)
          returning controller_task_id
          )
          insert into internal.tasks (task_id, task_type)
          select controller_task_id, 2 from insert_live;

        insert into catalog_stats_hourly (catalog_name, grain, ts, flow_document, bytes_written_by_me, bytes_written_to_me, bytes_read_by_me) values
          ('aliceCo/capture/three-hours', 'hourly', date_trunc('hour', now()), '{}', 0, 0, 0),
          ('aliceCo/capture/three-hours', 'hourly', date_trunc('hour', now() - '1h'::interval), '{}', 0, 0, 0),
          ('aliceCo/capture/three-hours', 'hourly', date_trunc('hour', now() - '2h'::interval), '{}', 0, 0, 0),
          ('aliceCo/capture/three-hours', 'hourly', date_trunc('hour', now() - '3h'::interval), '{}', 1024, 0, 0),
          ('aliceCo/capture/two-hours', 'hourly', date_trunc('hour', now()), '{}', 0, 0, 0),
          ('aliceCo/capture/two-hours', 'hourly', date_trunc('hour', now() - '1h'::interval), '{}', 0, 0, 0),
          ('aliceCo/capture/two-hours', 'hourly', date_trunc('hour', now() - '2h'::interval), '{}', 0, 0, 0),
          ('aliceCo/capture/deleted', 'hourly', date_trunc('hour', now()), '{}', 0, 0, 0),
          ('aliceCo/capture/deleted', 'hourly', date_trunc('hour', now() - '1h'::interval), '{}', 0, 0, 0),
          ('aliceCo/capture/deleted', 'hourly', date_trunc('hour', now() - '2h'::interval), '{}', 0, 0, 0),
          ('aliceCo/capture/deleted', 'hourly', date_trunc('hour', now() - '3h'::interval), '{}', 0, 0, 0),
          ('aliceCo/materialization/four-hours', 'hourly', date_trunc('hour', now() - '1h'::interval), '{}', 0, 0, 0),
          ('aliceCo/materialization/four-hours', 'hourly', date_trunc('hour', now() - '2h'::interval), '{}', 0, 0, 0),
          ('aliceCo/materialization/four-hours', 'hourly', date_trunc('hour', now() - '3h'::interval), '{}', 0, 0, 0),
          ('aliceCo/materialization/four-hours', 'hourly', date_trunc('hour', now() - '4h'::interval), '{}', 0, 0, 0),
          ('aliceCo/materialization/disabled', 'hourly', date_trunc('hour', now()), '{}', 0, 0, 0),
          ('aliceCo/materialization/disabled', 'hourly', date_trunc('hour', now() - '1h'::interval), '{}', 0, 0, 0),
          ('aliceCo/materialization/disabled', 'hourly', date_trunc('hour', now() - '2h'::interval), '{}', 0, 0, 0),
          ('aliceCo/materialization/disabled', 'hourly', date_trunc('hour', now() - '3h'::interval), '{}', 0, 0, 0);

          -- Simulate an alert that was already firing prior to the evaluation
          with existing as (
            insert into alert_history (id, catalog_name, alert_type, fired_at, arguments)
            values
                ('0000000000000001', 'aliceCo/materialization/legacy', 'data_movement_stalled'::alert_type, '2025-01-01T01:02:03Z', '{
                    "bytes_processed": 0,
                    "evaluation_interval": "02:00:00",
                    "recipients": [
                    {
                        "email": "legacy@example.com",
                        "full_name": "Ted Dancin"
                    }
                    ],
                    "spec_type": "materialization"
                }'),
                ('0000000000000002', 'aliceCo/capture/deleted', 'data_movement_stalled'::alert_type, '2025-01-01T01:02:03Z', '{
                    "bytes_processed": 0,
                    "evaluation_interval": "02:00:00",
                    "recipients": [
                    {
                        "email": "old.email@example.com",
                        "full_name": "Robert Frowny Jr :("
                    }
                    ],
                    "spec_type": "capture"
                }')
            returning id
          )
          insert into internal.tasks(task_id, task_type, inner_state)
          select id, 9, '{"fired_completed": "2025-12-31T12:59:59Z"}'::json
          from existing;
        "#)
    .execute(&pool).await.expect("setup sql failed");

    let data_movement_task_id = harness
        .run_automation_task(automations::task_types::DATA_MOVEMENT_ALERT_EVALS)
        .await
        .expect("alert task must have run");

    let fired = harness
        .assert_alert_firing(
            "aliceCo/capture/three-hours",
            AlertType::DataMovementStalled,
        )
        .await;
    let fired_notification_task_id = fired.alert.id;

    let fired_emails = fired
        .notifications
        .iter()
        .map(|n| n.recipient.email.as_str())
        .collect::<Vec<_>>();
    assert_eq!(vec!["alice@example.com", "bob@example.com"], fired_emails);

    let open_alerts =
        control_plane_api::alerts::fetch_open_alerts_by_type(AlertType::all(), &harness.pool)
            .await
            .unwrap();
    assert_eq!(2, open_alerts.len(), "expected 2 alerts, got: {fired:?}");
    let new_alert = open_alerts
        .iter()
        .find(|a| a.catalog_name == "aliceCo/capture/three-hours")
        .expect("expected alert for three-hours");

    assert_eq!(AlertType::DataMovementStalled, new_alert.alert_type);
    assert!(new_alert.resolved_at.is_none());
    assert!(new_alert.resolved_arguments.is_none());
    insta::assert_json_snapshot!(new_alert.arguments, @r#"
    {
      "bytes_processed": 0,
      "evaluation_interval": "02:00:00",
      "recipients": [
        {
          "email": "alice@example.com",
          "full_name": null
        },
        {
          "email": "bob@example.com",
          "full_name": null
        }
      ],
      "spec_type": "capture"
    }
    "#);

    // Assert that the legacy alert for the materialization is still firing
    let legacy_alert = open_alerts
        .iter()
        .find(|a| a.catalog_name == "aliceCo/materialization/legacy")
        .expect("expected a legacy alert to still be firing");
    assert!(legacy_alert.resolved_at.is_none());

    // Assert that the legacy alert for the deleted spec got resolved
    let emails = harness
        .assert_alert_resolved(models::Id::new([0, 0, 0, 0, 0, 0, 0, 2]))
        .await;
    // The notifications should be sent only to the recipients that had
    // previously been determined and already exist in the `arguments`.
    emails.assert_emails_sent(&["old.email@example.com"]);

    let task_state: serde_json::Value = harness.get_task_state(data_movement_task_id).await;
    insta::assert_json_snapshot!(task_state, { ".last_evaluation_time" => "[redacted]" }, @r#"
    {
      "failures": 0,
      "last_evaluation_time": "[redacted]",
      "last_result": {
        "fired": {
          "data_movement_stalled": 1
        },
        "resolved": {
          "data_movement_stalled": 1
        },
        "starting_open": {
          "data_movement_stalled": 2
        },
        "view_evaluated": {
          "data_movement_stalled": 2
        }
      },
      "open_alerts": {
        "data_movement_stalled": 2
      },
      "paused_at": null
    }
    "#);
    let alerts_state: EvaluatorState = serde_json::from_value(task_state).unwrap();

    // Now add stats, so the alerts will eventually resolve. But not before we test pausing evaluation.
    sqlx::raw_sql(
        r#"insert into catalog_stats_hourly
        (catalog_name, grain, ts, bytes_read_by_me, flow_document)
        values
            ('aliceCo/capture/three-hours', 'hourly', date_trunc('hour', now()), 5, '{}'),
            ('aliceCo/materialization/legacy', 'hourly', date_trunc('hour', now()), 5, '{}')
        on conflict(catalog_name, grain, ts) do update set bytes_read_by_me = 5;"#,
    )
    .execute(&pool)
    .await
    .unwrap();

    // Pause the alert evaluation and run the task a few times to make sure nothing changes
    harness
        .send_automation_message(
            data_movement_task_id,
            models::Id::zero(),
            crate::alerts::EvaluatorMessage::Pause,
        )
        .await;
    for i in 0..3 {
        tracing::info!(%i, "fidna run paused alert eval task");
        harness.set_min_task_wake_at(data_movement_task_id).await;
        harness
            .run_automation_task(automations::task_types::DATA_MOVEMENT_ALERT_EVALS)
            .await
            .expect("alert task must have run");
        let paused_state: EvaluatorState = harness.get_task_state(data_movement_task_id).await;
        assert!(paused_state.paused_at.is_some());
        assert_eq!(paused_state.open_alerts, alerts_state.open_alerts);
        assert_eq!(paused_state.last_result, alerts_state.last_result);
        assert_eq!(
            paused_state.last_evaluation_time,
            alerts_state.last_evaluation_time
        );
    }

    // Resume alert evaluation and expect that the alerts now resolve
    harness
        .send_automation_message(
            data_movement_task_id,
            models::Id::zero(),
            crate::alerts::EvaluatorMessage::Resume,
        )
        .await;
    harness
        .run_automation_task(automations::task_types::DATA_MOVEMENT_ALERT_EVALS)
        .await
        .expect("alert task must have run");

    let resolved = harness
        .assert_alert_resolved(fired_notification_task_id)
        .await;

    let resolved_emails = resolved
        .notifications
        .iter()
        .map(|n| n.recipient.email.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        vec!["alice@example.com", "bob@example.com"],
        resolved_emails
    );

    let firing =
        control_plane_api::alerts::fetch_open_alerts_by_type(AlertType::all(), &harness.pool)
            .await
            .unwrap();
    assert!(
        firing.is_empty(),
        "expected no more open alerts, got: {firing:?}"
    );

    let task_state: serde_json::Value = harness.get_task_state(data_movement_task_id).await;
    insta::assert_json_snapshot!(task_state, { ".last_evaluation_time" => "[redacted]" }, @r#"
    {
      "failures": 0,
      "last_evaluation_time": "[redacted]",
      "last_result": {
        "resolved": {
          "data_movement_stalled": 2
        },
        "starting_open": {
          "data_movement_stalled": 2
        }
      },
      "open_alerts": {
        "data_movement_stalled": 0
      },
      "paused_at": null
    }
    "#);
}
