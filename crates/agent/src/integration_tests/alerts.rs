use std::usize;

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

/// Inserts a minimal `live_specs` + `controller_jobs` + `internal.tasks`
/// trio for a capture with the given age and initial status JSON. Skips
/// publication machinery so that tests control exactly which controller
/// phases run and what state they observe.
async fn insert_capture_for_controller(
    pool: &sqlx::PgPool,
    catalog_name: &str,
    age: chrono::Duration,
) {
    insert_task(
        pool,
        catalog_name,
        "capture",
        r#"{"endpoint": {"connector": {"image": "src/test:test", "config": {}}}, "bindings": []}"#,
        age,
        0,
        serde_json::json!({"type": "Uninitialized"}),
    )
    .await;
}

async fn insert_materialization_for_controller(
    pool: &sqlx::PgPool,
    catalog_name: &str,
    age: chrono::Duration,
) {
    insert_task(
        pool,
        catalog_name,
        "materialization",
        r#"{"endpoint": {"connector": {"image": "src/test:test", "config": {}}}, "bindings": []}"#,
        age,
        0,
        serde_json::json!({"type": "Uninitialized"}),
    )
    .await;
}

async fn insert_collection_without_task_for_controller(
    pool: &sqlx::PgPool,
    catalog_name: &str,
    age: chrono::Duration,
) {
    insert_task(
        pool,
        catalog_name,
        "collection",
        r#"{"schema": {"type": "object"}, "key": ["/id"]}"#,
        age,
        0,
        serde_json::json!({"type": "Uninitialized"}),
    )
    .await;
}

async fn insert_dekaf_materialization_for_controller(
    pool: &sqlx::PgPool,
    catalog_name: &str,
    age: chrono::Duration,
) {
    insert_task(
        pool,
        catalog_name,
        "materialization",
        r#"{"endpoint": {"dekaf": {"variant": "test", "config": {}}}, "bindings": []}"#,
        age,
        0,
        serde_json::json!({"type": "Uninitialized"}),
    )
    .await;
}

async fn insert_disabled_capture_for_controller(
    pool: &sqlx::PgPool,
    catalog_name: &str,
    age: chrono::Duration,
) {
    insert_task(
        pool,
        catalog_name,
        "capture",
        r#"{"endpoint": {"connector": {"image": "src/test:test", "config": {}}}, "bindings": [], "shards": {"disable": true}}"#,
        age,
        0,
        serde_json::json!({"type": "Uninitialized"}),
    )
    .await;
}

/// Same as `insert_capture_for_controller` but seeds a specific
/// `controller_jobs.status` blob at the current schema version. Used to
/// pre-populate alert state that the test wants the controller to observe.
async fn insert_capture_with_status(
    pool: &sqlx::PgPool,
    catalog_name: &str,
    age: chrono::Duration,
    status: serde_json::Value,
) {
    insert_task(
        pool,
        catalog_name,
        "capture",
        r#"{"endpoint": {"connector": {"image": "src/test:test", "config": {}}}, "bindings": []}"#,
        age,
        crate::controllers::CONTROLLER_VERSION,
        status,
    )
    .await;
}

async fn insert_task(
    pool: &sqlx::PgPool,
    catalog_name: &str,
    spec_type: &str,
    spec_json: &str,
    age: chrono::Duration,
    controller_version: i32,
    status: serde_json::Value,
) {
    sqlx::query(
        r#"
        with new_spec as (
            insert into live_specs (catalog_name, spec_type, spec, built_spec, created_at, last_pub_id, last_build_id)
            values (
                ($1::text)::catalog_name,
                $5::catalog_spec_type,
                $6::json,
                '{}'::json,
                now() - make_interval(secs => $2::float8),
                '1111111111111111'::flowid,
                '1111111111111111'::flowid
            )
            returning id, controller_task_id
        ),
        new_cj as (
            insert into controller_jobs (live_spec_id, controller_version, status)
            select id, $3::int4, $4::jsonb from new_spec
        )
        insert into internal.tasks (task_id, task_type, wake_at)
        select controller_task_id, 2, now() - '1 minute'::interval from new_spec
        "#,
    )
    .bind(catalog_name)
    .bind(age.num_seconds() as f64)
    .bind(controller_version)
    .bind(sqlx::types::Json(&status))
    .bind(spec_type)
    .bind(spec_json)
    .execute(pool)
    .await
    .expect("failed to insert task for controller test");
}

#[tokio::test]
async fn test_data_movement_stalled_end_to_end() {
    let mut harness = TestHarness::init("test_data_movement_stalled_end_to_end").await;
    harness.setup_tenant("acme").await;
    let pool = harness.pool.clone();

    insert_capture_for_controller(&pool, "acme/team-a/stalled", chrono::Duration::hours(3)).await;
    insert_capture_for_controller(&pool, "acme/team-a/special", chrono::Duration::hours(3)).await;
    insert_capture_for_controller(&pool, "acme/legacy", chrono::Duration::hours(3)).await;
    insert_capture_for_controller(&pool, "acme/no-config", chrono::Duration::hours(3)).await;
    insert_capture_for_controller(&pool, "acme/team-a/young", chrono::Duration::minutes(30)).await;
    insert_collection_without_task_for_controller(
        &pool,
        "acme/team-a/no-task-collection",
        chrono::Duration::hours(3),
    )
    .await;
    insert_dekaf_materialization_for_controller(
        &pool,
        "acme/team-a/dekaf-mat",
        chrono::Duration::hours(3),
    )
    .await;
    insert_materialization_for_controller(
        &pool,
        "acme/team-a/mat-stalled",
        chrono::Duration::hours(3),
    )
    .await;
    insert_disabled_capture_for_controller(
        &pool,
        "acme/team-a/disabled-cap",
        chrono::Duration::hours(3),
    )
    .await;

    // A subscriber so that the fired alert produces recipient rows in the
    // `arguments` JSON (needed for the parity snapshot).
    sqlx::raw_sql(
        r#"
        insert into alert_subscriptions (catalog_prefix, email, include_alert_types)
        values ('acme/', 'ops@acme.test', array['data_movement_stalled'::alert_type]);
        "#,
    )
    .execute(&pool)
    .await
    .expect("failed to insert alert subscription");

    // Prefix config applies to team-a/stalled and team-a/young; exact-name
    // config overrides for team-a/special.
    harness
        .upsert_alert_config(
            "acme/team-a/",
            serde_json::json!({ "dataMovementStalled": { "threshold": "1h" } }),
        )
        .await;
    harness
        .upsert_alert_config(
            "acme/team-a/special",
            serde_json::json!({ "dataMovementStalled": { "threshold": "4h" } }),
        )
        .await;

    // Fallback threshold from `alert_data_processing` for `acme/legacy`.
    sqlx::query!(
        r#"insert into alert_data_processing (catalog_name, evaluation_interval)
           values ('acme/legacy', '2 hours'::interval)"#,
    )
    .execute(&pool)
    .await
    .expect("failed to insert alert_data_processing row");

    for name in [
        "acme/team-a/stalled",
        "acme/team-a/special",
        "acme/legacy",
        "acme/no-config",
        "acme/team-a/young",
        "acme/team-a/no-task-collection",
        "acme/team-a/dekaf-mat",
        "acme/team-a/mat-stalled",
        "acme/team-a/disabled-cap",
    ] {
        harness.run_pending_controller(name).await;
    }

    // Prefix-matched task fires.
    let stalled = harness.get_controller_state("acme/team-a/stalled").await;
    let alerts = current_alerts(&stalled);
    let stalled_alert = alerts
        .get(&AlertType::DataMovementStalled)
        .expect("DataMovementStalled should be firing for team-a/stalled");
    assert_eq!(
        stalled_alert.extra.get("evaluation_interval").unwrap(),
        "1h",
        "prefix threshold is 1h"
    );

    // Exact-name override pushes threshold above the task's age, so it
    // does not fire despite matching the broader prefix.
    let special = harness.get_controller_state("acme/team-a/special").await;
    assert!(
        !current_alerts(&special).contains_key(&AlertType::DataMovementStalled),
        "exact-name override (4h) should suppress firing on a 3h-old spec"
    );

    // No `alert_configs` row, but `alert_data_processing` supplies the
    // threshold.
    let legacy = harness.get_controller_state("acme/legacy").await;
    let legacy_alert = current_alerts(&legacy)
        .get(&AlertType::DataMovementStalled)
        .expect("DataMovementStalled should fire via legacy fallback");
    assert_eq!(
        legacy_alert.extra.get("evaluation_interval").unwrap(),
        "2h",
        "legacy threshold is 2h"
    );

    // No configured threshold and no fallback row means no alert.
    let no_config = harness.get_controller_state("acme/no-config").await;
    assert!(
        !current_alerts(&no_config).contains_key(&AlertType::DataMovementStalled),
        "no threshold source must not fire an alert"
    );

    // Age gate: a spec younger than its configured threshold must not
    // fire, regardless of byte activity.
    let young = harness.get_controller_state("acme/team-a/young").await;
    assert!(
        !current_alerts(&young).contains_key(&AlertType::DataMovementStalled),
        "age gate should suppress a spec younger than its threshold"
    );

    // A non-derivation collection is not a shard-backed task and must not
    // participate in DataMovementStalled.
    let no_task_collection = harness
        .get_controller_state("acme/team-a/no-task-collection")
        .await;
    assert!(
        !current_alerts(&no_task_collection).contains_key(&AlertType::DataMovementStalled),
        "non-derivation collections must not fire DataMovementStalled"
    );

    // Dekaf materializations have no reactor shards but do report
    // data-movement stats, so they are eligible for this alert.
    let dekaf_mat = harness.get_controller_state("acme/team-a/dekaf-mat").await;
    assert!(
        current_alerts(&dekaf_mat).contains_key(&AlertType::DataMovementStalled),
        "dekaf materializations must fire DataMovementStalled; got: {:?}",
        current_alerts(&dekaf_mat).keys().collect::<Vec<_>>(),
    );

    // Materialization fires just like a capture under the same prefix.
    let mat = harness
        .get_controller_state("acme/team-a/mat-stalled")
        .await;
    assert!(
        current_alerts(&mat).contains_key(&AlertType::DataMovementStalled),
        "materialization must also fire DataMovementStalled; got: {:?}",
        current_alerts(&mat).keys().collect::<Vec<_>>(),
    );

    // A capture with `shards.disable=true` must not fire despite matching
    // the prefix threshold.
    let disabled = harness
        .get_controller_state("acme/team-a/disabled-cap")
        .await;
    assert!(
        !current_alerts(&disabled).contains_key(&AlertType::DataMovementStalled),
        "shards.disable=true must suppress DataMovementStalled"
    );

    // Re-running the controller when the alert is already firing must
    // not duplicate or spuriously resolve it.
    let stalled_first_ts = stalled_alert.first_ts;
    harness.run_pending_controller("acme/team-a/stalled").await;
    let stalled_rerun = harness.get_controller_state("acme/team-a/stalled").await;
    let rerun_alert = current_alerts(&stalled_rerun)
        .get(&AlertType::DataMovementStalled)
        .expect("DataMovementStalled must still be firing after re-run");
    assert_eq!(
        rerun_alert.first_ts, stalled_first_ts,
        "first_ts must not change on re-evaluation"
    );

    // `arguments` JSON in `alert_history` must expose `spec_type`,
    // `bytes_processed`, and `evaluation_interval` at the top level so the
    // notification templates can render them.
    let args: serde_json::Value = sqlx::query_scalar!(
        r#"select arguments as "args!: sqlx::types::Json<serde_json::Value>"
           from alert_history
           where catalog_name = 'acme/team-a/stalled'
             and alert_type = 'data_movement_stalled'"#,
    )
    .fetch_one(&pool)
    .await
    .expect("failed to fetch alert_history row")
    .0;
    insta::assert_json_snapshot!(args, {
        ".first_ts" => "[ts]",
        ".last_ts" => "[ts]",
        ".resolved_at" => "[ts]",
    }, @r#"
    {
      "bytes_processed": 0,
      "count": 1,
      "error": "task has not moved data in the last 1h",
      "evaluation_interval": "1h",
      "first_ts": "[ts]",
      "last_ts": "[ts]",
      "recipients": [
        {
          "email": "acme@test_data_movement_stalled_end_to_end.test",
          "full_name": "Full (acme) Name"
        },
        {
          "email": "ops@acme.test",
          "full_name": null
        }
      ],
      "resolved_at": "[ts]",
      "spec_type": "capture",
      "state": "firing"
    }
    "#);

    // Now simulate data movement and re-run to assert resolution.
    sqlx::query!(
        r#"insert into catalog_stats_hourly
             (catalog_name, grain, ts, flow_document,
              bytes_written_by_me, bytes_written_to_me, bytes_read_by_me)
           values
             ('acme/team-a/stalled', 'hourly', date_trunc('hour', now()),
              '{}', 1024, 0, 0)"#,
    )
    .execute(&pool)
    .await
    .expect("failed to insert catalog_stats_hourly row");

    harness.run_pending_controller("acme/team-a/stalled").await;
    let resolved = harness.get_controller_state("acme/team-a/stalled").await;
    assert!(
        !current_alerts(&resolved).contains_key(&AlertType::DataMovementStalled),
        "alert should resolve after bytes start flowing"
    );
    let resolved_at: Option<DateTime<Utc>> = sqlx::query_scalar!(
        r#"select resolved_at from alert_history
           where catalog_name = 'acme/team-a/stalled'
             and alert_type = 'data_movement_stalled'"#,
    )
    .fetch_one(&pool)
    .await
    .expect("failed to fetch alert_history row");
    assert!(
        resolved_at.is_some(),
        "alert_history.resolved_at should be set"
    );
}

/// Extracts the `alerts` map from the controller status regardless of
/// spec type variant.
fn current_alerts(state: &crate::controllers::ControllerState) -> &models::status::Alerts {
    use models::status::ControllerStatus;
    match &state.current_status {
        ControllerStatus::Capture(s) => &s.alerts,
        ControllerStatus::Collection(s) => &s.alerts,
        ControllerStatus::Materialization(s) => &s.alerts,
        ControllerStatus::Test(s) => &s.alerts,
        ControllerStatus::Uninitialized => {
            panic!("controller status is Uninitialized; did the controller run?")
        }
    }
}

/// Schedules the controller task for `catalog_name` to re-run on the
/// next poll, clearing the `abandon_status.last_evaluated` throttle that
/// would otherwise defer re-evaluation until `abandon_check_interval`
/// has elapsed. Used between runs when a test mutates `alert_configs`
/// and wants the next run to observe the change immediately.
async fn clear_abandon_throttle(pool: &sqlx::PgPool, catalog_name: &str) {
    sqlx::query!(
        r#"
        update controller_jobs cj
        set status = jsonb_set(cj.status::jsonb, '{abandon}', 'null'::jsonb, false)::json
        from live_specs ls
        where ls.id = cj.live_spec_id
          and ls.catalog_name = $1
        "#,
        catalog_name,
    )
    .execute(pool)
    .await
    .expect("failed to clear abandon throttle");
    sqlx::query!(
        r#"
        update internal.tasks t
        set wake_at = now() - '1 minute'::interval
        from live_specs ls
        where ls.controller_task_id = t.task_id
          and ls.catalog_name = $1
        "#,
        catalog_name,
    )
    .execute(pool)
    .await
    .expect("failed to rearm controller task");
}

#[tokio::test]
async fn test_abandon_per_task_thresholds() {
    let mut harness = TestHarness::init("test_abandon_per_task_thresholds").await;
    harness.setup_tenant("acme").await;
    let pool = harness.pool.clone();

    let now = chrono::Utc::now();
    let forty_five_days_ago = now - chrono::Duration::days(45);

    // Chronic scenario: seed a ShardFailed alert whose first_ts is 45d
    // old. At a 60d `taskChronicallyFailing.threshold`, the abandon
    // evaluator must NOT fire TaskChronicallyFailing; at 30d it must.
    let chronic_status = serde_json::json!({
        "type": "Capture",
        "publications": {"history": []},
        "activation": {},
        "alerts": {
            "shard_failed": {
                "state": "firing",
                "spec_type": "capture",
                "first_ts": forty_five_days_ago,
                "last_ts": forty_five_days_ago,
                "error": "test-seeded shard failure",
                "count": 10,
                "resolved_at": null,
            }
        }
    });
    insert_capture_with_status(
        &pool,
        "acme/chronic",
        chrono::Duration::days(50),
        chronic_status,
    )
    .await;

    // Idle scenario: fresh status (no pre-existing alerts), with
    // `catalog_stats_daily` showing the last data movement was 45 days
    // ago. `last_user_pub_at` is None because we never published
    // anything — abandon treats that as "no recent user activity".
    let fresh_status = serde_json::json!({
        "type": "Capture",
        "publications": {"history": []},
        "activation": {},
        "alerts": {}
    });
    insert_capture_with_status(&pool, "acme/idle", chrono::Duration::days(50), fresh_status).await;
    sqlx::query!(
        r#"
        insert into catalog_stats_daily
          (catalog_name, grain, ts, flow_document,
           bytes_written_by_me, bytes_written_to_me, bytes_read_by_me, bytes_read_from_me)
        values ($1, 'daily', $2, '{}', 1024, 0, 0, 0)
        "#,
        "acme/idle",
        forty_five_days_ago,
    )
    .execute(&pool)
    .await
    .expect("seed catalog_stats_daily row");

    // First pass: per-task thresholds exceed the observed staleness, so
    // neither alert fires.
    harness
        .upsert_alert_config(
            "acme/chronic",
            serde_json::json!({
                "taskChronicallyFailing": { "threshold": "60d" },
            }),
        )
        .await;
    harness
        .upsert_alert_config(
            "acme/idle",
            serde_json::json!({ "taskIdle": { "threshold": "60d" } }),
        )
        .await;

    harness.run_pending_controller("acme/chronic").await;
    harness.run_pending_controller("acme/idle").await;

    let chronic_state = harness.get_controller_state("acme/chronic").await;
    assert!(
        !current_alerts(&chronic_state).contains_key(&AlertType::TaskChronicallyFailing),
        "60d threshold should suppress TaskChronicallyFailing at 45d ShardFailed age"
    );
    let idle_state = harness.get_controller_state("acme/idle").await;
    assert!(
        !current_alerts(&idle_state).contains_key(&AlertType::TaskIdle),
        "60d threshold should suppress TaskIdle at 45d data staleness"
    );

    // Second pass: shrink both thresholds below the observed staleness;
    // both alerts must fire on the next controller run.
    harness
        .upsert_alert_config(
            "acme/chronic",
            serde_json::json!({
                "taskChronicallyFailing": { "threshold": "30d" },
            }),
        )
        .await;
    harness
        .upsert_alert_config(
            "acme/idle",
            serde_json::json!({ "taskIdle": { "threshold": "30d" } }),
        )
        .await;
    clear_abandon_throttle(&pool, "acme/chronic").await;
    clear_abandon_throttle(&pool, "acme/idle").await;

    harness.run_pending_controller("acme/chronic").await;
    harness.run_pending_controller("acme/idle").await;

    let chronic_state = harness.get_controller_state("acme/chronic").await;
    assert!(
        current_alerts(&chronic_state).contains_key(&AlertType::TaskChronicallyFailing),
        "30d threshold should fire TaskChronicallyFailing at 45d ShardFailed age; got alerts: {:?}",
        current_alerts(&chronic_state).keys().collect::<Vec<_>>(),
    );
    let idle_state = harness.get_controller_state("acme/idle").await;
    assert!(
        current_alerts(&idle_state).contains_key(&AlertType::TaskIdle),
        "30d threshold should fire TaskIdle at 45d data staleness; got alerts: {:?}",
        current_alerts(&idle_state).keys().collect::<Vec<_>>(),
    );
}

/// Hierarchical merge: rows at every ancestor-prefix level plus an
/// exact-name row each contribute their fields independently. Exercised
/// here by inserting three rows at `acme/`, `acme/prod/`, and
/// `acme/prod/source-pg`, each setting distinct fields, and asserting the
/// merged config resolved via `ControlPlane::fetch_alert_config`.
#[tokio::test]
async fn test_alert_configs_hierarchical_merge() {
    let mut harness = TestHarness::init("test_alert_configs_hierarchical_merge").await;
    harness.setup_tenant("acme").await;

    // Tenant-wide default: explicitly enable ShardFailed with a loose
    // threshold (the `enabled` key is exercised here to verify it
    // propagates through the merge; at runtime the baseline is firing).
    harness
        .upsert_alert_config(
            "acme/",
            serde_json::json!({
                "shardFailed": { "enabled": true, "failureThreshold": 3 },
                "taskIdle": { "threshold": "90d" },
            }),
        )
        .await;
    // Prod prefix: override shardFailed threshold; still inherit everything else.
    harness
        .upsert_alert_config(
            "acme/prod/",
            serde_json::json!({ "shardFailed": { "failureThreshold": 10 } }),
        )
        .await;
    // Exact-name: override one taskIdle field only.
    harness
        .upsert_alert_config(
            "acme/prod/source-pg",
            serde_json::json!({ "taskIdle": { "threshold": "30d" } }),
        )
        .await;

    let merged = {
        use crate::controlplane::ControlPlane;
        harness
            .control_plane()
            .fetch_alert_config("acme/prod/source-pg".to_string())
            .await
            .expect("fetch_alert_config")
            .expect("some config matches")
    };

    insta::assert_json_snapshot!(serde_json::to_value(&merged).unwrap(), @r#"
    {
      "shardFailed": {
        "enabled": true,
        "failureThreshold": 10
      },
      "taskIdle": {
        "threshold": "30days"
      }
    }
    "#);
}

/// `dataMovementStalled.enabled: false` at a deeper layer silences the
/// alert even when a prefix supplies a threshold.
#[tokio::test]
async fn test_data_movement_stalled_disabled_overrides_prefix_threshold() {
    let mut harness =
        TestHarness::init("test_data_movement_stalled_disabled_overrides_prefix_threshold").await;
    harness.setup_tenant("acme").await;
    let pool = harness.pool.clone();

    insert_capture_for_controller(&pool, "acme/other", chrono::Duration::hours(3)).await;
    insert_capture_for_controller(&pool, "acme/batch", chrono::Duration::hours(3)).await;

    // Tenant-wide DataMovementStalled opt-in with a 1h threshold.
    harness
        .upsert_alert_config(
            "acme/",
            serde_json::json!({ "dataMovementStalled": { "threshold": "1h" } }),
        )
        .await;
    // …but `acme/batch` explicitly opts out.
    harness
        .upsert_alert_config(
            "acme/batch",
            serde_json::json!({ "dataMovementStalled": { "enabled": false } }),
        )
        .await;

    // No catalog_stats_hourly rows means zero bytes for everyone.
    harness.run_pending_controller("acme/other").await;
    harness.run_pending_controller("acme/batch").await;

    let other = harness.get_controller_state("acme/other").await;
    assert!(
        current_alerts(&other).contains_key(&AlertType::DataMovementStalled),
        "acme/other inherits tenant threshold and must fire; got: {:?}",
        current_alerts(&other).keys().collect::<Vec<_>>(),
    );
    let batch = harness.get_controller_state("acme/batch").await;
    assert!(
        !current_alerts(&batch).contains_key(&AlertType::DataMovementStalled),
        "acme/batch opts out via enabled=false; must NOT fire despite inherited threshold"
    );
}

/// Once a DataMovementStalled alert resolves because bytes were written,
/// the alert should re-fire if data stops flowing again.
#[tokio::test]
async fn test_data_movement_stalled_refires_after_resolution() {
    let mut harness =
        TestHarness::init("test_data_movement_stalled_refires_after_resolution").await;
    harness.setup_tenant("acme").await;
    let pool = harness.pool.clone();

    insert_capture_for_controller(&pool, "acme/refiring", chrono::Duration::hours(3)).await;
    harness
        .upsert_alert_config(
            "acme/",
            serde_json::json!({ "dataMovementStalled": { "threshold": "1h" } }),
        )
        .await;

    // Phase 1: Alert fires (no bytes).
    harness.run_pending_controller("acme/refiring").await;
    let state = harness.get_controller_state("acme/refiring").await;
    assert!(
        current_alerts(&state).contains_key(&AlertType::DataMovementStalled),
        "should fire initially with no data"
    );

    // Phase 2: Data flows, alert resolves.
    sqlx::query!(
        r#"insert into catalog_stats_hourly
             (catalog_name, grain, ts, flow_document,
              bytes_written_by_me, bytes_written_to_me, bytes_read_by_me)
           values
             ('acme/refiring', 'hourly', date_trunc('hour', now()),
              '{}', 512, 0, 0)"#,
    )
    .execute(&pool)
    .await
    .unwrap();

    harness.run_pending_controller("acme/refiring").await;
    let resolved = harness.get_controller_state("acme/refiring").await;
    assert!(
        !current_alerts(&resolved).contains_key(&AlertType::DataMovementStalled),
        "should resolve after bytes flow"
    );

    // Phase 3: Data stops again. Remove the stats row and re-run.
    sqlx::query!("delete from catalog_stats_hourly where catalog_name = 'acme/refiring'")
        .execute(&pool)
        .await
        .unwrap();

    harness.run_pending_controller("acme/refiring").await;
    let refired = harness.get_controller_state("acme/refiring").await;
    assert!(
        current_alerts(&refired).contains_key(&AlertType::DataMovementStalled),
        "should re-fire after data stops again"
    );
}

/// `shardFailed.enabled: false` in alert_configs should suppress the
/// ShardFailed alert while still counting failures and scheduling retries.
#[tokio::test]
async fn test_shard_failed_enabled_false() {
    use super::harness::draft_catalog;
    use crate::ControlPlane;

    let mut harness = TestHarness::init("test_shard_failed_enabled_false").await;
    let _user_id = harness.setup_tenant("foxes").await;

    harness
        .upsert_alert_config(
            "foxes/",
            serde_json::json!({ "shardFailed": { "enabled": false } }),
        )
        .await;

    let draft = draft_catalog(serde_json::json!({
        "collections": {
            "foxes/den": {
                "schema": {
                    "type": "object",
                    "properties": { "id": { "type": "string" } }
                },
                "key": ["/id"]
            }
        },
        "captures": {
            "foxes/capture": {
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    { "resource": { "table": "den" }, "target": "foxes/den" }
                ]
            }
        }
    }));

    let result = harness
        .control_plane()
        .publish(
            Some("initial".to_string()),
            uuid::Uuid::new_v4(),
            draft,
            Some("ops/dp/public/test".to_string()),
        )
        .await
        .expect("publish failed");
    assert!(result.status.is_success());

    harness.run_pending_controllers(None).await;
    harness.control_plane().reset_activations();

    // Trigger enough shard failures to normally fire an alert (threshold is 3).
    let state = harness.get_controller_state("foxes/capture").await;
    let shard = super::shard_failures::shard_ref(state.last_build_id, "foxes/capture");
    for _ in 0..5 {
        harness.fail_shard(&shard).await;
        harness.run_pending_controller("foxes/capture").await;
    }

    // With enabled: false, the alert should NOT fire despite exceeding
    // the failure threshold.
    harness
        .assert_alert_clear("foxes/capture", AlertType::ShardFailed)
        .await;
}
