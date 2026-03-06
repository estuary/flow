use crate::{
    ControlPlane,
    controllers::ControllerState,
    integration_tests::harness::{TestHarness, draft_catalog},
};
use gazette::consumer::replica_status;
use models::{
    CatalogType,
    status::{AlertType, ShardRef},
};
use uuid::Uuid;

#[tokio::test]
async fn test_abandoned_task_detection_and_resolution() {
    let mut harness = TestHarness::init("test_abandoned_tasks").await;
    let _user_id = harness.setup_tenant("pandas").await;

    let draft = draft_catalog(serde_json::json!({
        "collections": {
            "pandas/bamboo": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                },
                "key": ["/id"]
            },
            "pandas/luck": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                },
                "key": ["/id"],
                "derive": {
                    "using": {
                        "sqlite": { "migrations": [] }
                    },
                    "transforms": [
                        {
                            "name": "fromHoots",
                            "source": "pandas/bamboo",
                            "lambda": "select $id;",
                            "shuffle": "any"
                        }
                    ]
                }
            }
        },
        "captures": {
            "pandas/capture": {
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "table": "bamboo" },
                        "target": "pandas/bamboo"
                    }
                ]
            }
        },
        "materializations": {
            "pandas/materialize": {
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "table": "bamboo" },
                        "source": "pandas/bamboo"
                    },
                    {
                        "resource": { "table": "luck" },
                        "source": "pandas/luck"
                    }
                ]
            }
        }
    }));

    let result = harness
        .control_plane()
        .publish(
            Some("initial publication".to_string()),
            Uuid::new_v4(),
            draft,
            Some("ops/dp/public/test".to_string()),
        )
        .await
        .expect("initial publish failed");
    assert!(
        result.status.is_success(),
        "publication failed: {:?}",
        result.draft_errors()
    );

    harness.run_pending_controllers(None).await;
    harness.control_plane().assert_activations(
        "initial activations",
        vec![
            ("pandas/capture", Some(CatalogType::Capture)),
            ("pandas/materialize", Some(CatalogType::Materialization)),
            ("pandas/bamboo", Some(CatalogType::Collection)),
            ("pandas/luck", Some(CatalogType::Collection)),
        ],
    );

    let tasks = &[
        ("pandas/capture", CatalogType::Capture),
        ("pandas/materialize", CatalogType::Materialization),
        ("pandas/luck", CatalogType::Collection),
    ];

    for (catalog_name, task_type) in tasks {
        tracing::info!(%catalog_name, "starting chronically failing scenarios");

        test_shard_failed_under_threshold_no_alert(&mut harness, *task_type, *catalog_name).await;
        test_shard_failed_over_threshold_fires(&mut harness, *task_type, *catalog_name).await;
        test_shard_failed_resolves_clears_alerts(&mut harness, *task_type, *catalog_name).await;
        test_chronically_failing_auto_disable_fires(&mut harness, *task_type, *catalog_name).await;
        test_disable_clears_all_alerts(&mut harness, *task_type, *catalog_name).await;
    }

    // Idle detection tests only on capture (doesn't depend on task type)
    test_idle_fires_when_no_data_and_old(&mut harness, CatalogType::Capture, "pandas/capture")
        .await;
    test_idle_suppressed_by_shard_failed(&mut harness, CatalogType::Capture, "pandas/capture")
        .await;
}

/// ShardFailed firing for < 30 days does NOT trigger TaskChronicallyFailing.
async fn test_shard_failed_under_threshold_no_alert(
    harness: &mut TestHarness,
    task_type: CatalogType,
    catalog_name: &str,
) {
    tracing::info!(%catalog_name, "shard_failed under threshold");
    publish_and_await_ready(harness, task_type, catalog_name).await;

    // Trigger a real ShardFailed alert, then backdate it to 10 days ago (under 30-day threshold).
    trigger_shard_failed_alert(catalog_name, chrono::Duration::days(10), harness).await;

    // Run the controller again so the abandon logic evaluates the backdated ShardFailed.
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;
    let _state = harness.run_pending_controller(catalog_name).await;

    harness
        .assert_alert_clear(catalog_name, AlertType::TaskChronicallyFailing)
        .await;
}

/// ShardFailed firing for > 30 days triggers TaskChronicallyFailing.
async fn test_shard_failed_over_threshold_fires(
    harness: &mut TestHarness,
    task_type: CatalogType,
    catalog_name: &str,
) {
    tracing::info!(%catalog_name, "shard_failed over threshold fires");
    publish_and_await_ready(harness, task_type, catalog_name).await;

    // Trigger a real ShardFailed alert, then backdate it to 35 days ago (over 30-day threshold).
    trigger_shard_failed_alert(catalog_name, chrono::Duration::days(35), harness).await;

    // Run the controller again so the abandon logic evaluates the backdated ShardFailed.
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;
    let _state = harness.run_pending_controller(catalog_name).await;

    let alert = harness
        .assert_alert_firing(catalog_name, AlertType::TaskChronicallyFailing)
        .await;

    let extra = &alert.alert.arguments.0;
    assert!(
        extra.get("disable_at").and_then(|v| v.as_str()).is_some(),
        "expected disable_at in alert arguments, got: {extra:?}"
    );
}

/// When ShardFailed resolves, TaskChronicallyFailing and TaskAutoDisabledFailing clear.
async fn test_shard_failed_resolves_clears_alerts(
    harness: &mut TestHarness,
    task_type: CatalogType,
    catalog_name: &str,
) {
    tracing::info!(%catalog_name, "shard_failed resolves clears alerts");
    publish_and_await_ready(harness, task_type, catalog_name).await;

    // Trigger ShardFailed > 30 days, fire TaskChronicallyFailing.
    trigger_shard_failed_alert(catalog_name, chrono::Duration::days(35), harness).await;
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;
    let _state = harness.run_pending_controller(catalog_name).await;
    harness
        .assert_alert_firing(catalog_name, AlertType::TaskChronicallyFailing)
        .await;

    // Simulate shard recovery: mock Primary and remove the ShardFailed alert.
    harness
        .control_plane()
        .mock_shard_status(catalog_name, vec![replica_status::Code::Primary]);
    remove_alert_from_status(catalog_name, AlertType::ShardFailed, harness).await;
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;
    let _state = harness.run_pending_controller(catalog_name).await;

    harness
        .assert_alert_clear(catalog_name, AlertType::TaskChronicallyFailing)
        .await;
}

/// TaskAutoDisabledFailing fires after the grace period expires.
async fn test_chronically_failing_auto_disable_fires(
    harness: &mut TestHarness,
    task_type: CatalogType,
    catalog_name: &str,
) {
    tracing::info!(%catalog_name, "auto-disable fires after grace period");
    publish_and_await_ready(harness, task_type, catalog_name).await;

    // Trigger ShardFailed > 30 days, fire TaskChronicallyFailing.
    trigger_shard_failed_alert(catalog_name, chrono::Duration::days(35), harness).await;
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;
    let _state = harness.run_pending_controller(catalog_name).await;
    harness
        .assert_alert_firing(catalog_name, AlertType::TaskChronicallyFailing)
        .await;
    harness
        .assert_alert_clear(catalog_name, AlertType::TaskAutoDisabledFailing)
        .await;

    // Push back TaskChronicallyFailing first_ts past the 7-day grace period.
    push_back_alert_first_ts(
        catalog_name,
        AlertType::TaskChronicallyFailing,
        chrono::Duration::days(10),
        harness,
    )
    .await;
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;

    let _state = harness.run_pending_controller(catalog_name).await;

    harness
        .assert_alert_firing(catalog_name, AlertType::TaskAutoDisabledFailing)
        .await;
}

/// Disabling a task clears all abandon alerts silently.
async fn test_disable_clears_all_alerts(
    harness: &mut TestHarness,
    task_type: CatalogType,
    catalog_name: &str,
) {
    tracing::info!(%catalog_name, "disable clears all alerts");
    publish_and_await_ready(harness, task_type, catalog_name).await;

    // Fire TaskChronicallyFailing via ShardFailed > 30 days.
    trigger_shard_failed_alert(catalog_name, chrono::Duration::days(35), harness).await;
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;
    let _state = harness.run_pending_controller(catalog_name).await;
    harness
        .assert_alert_firing(catalog_name, AlertType::TaskChronicallyFailing)
        .await;

    // Disable the task.
    let start = harness.get_controller_state(catalog_name).await;
    let mut draft = tables::DraftCatalog::default();
    let mut spec = start.live_spec.clone().unwrap();
    match &mut spec {
        models::AnySpec::Capture(c) => c.shards.disable = true,
        models::AnySpec::Collection(c) => {
            let derivation = c.derive.as_mut().unwrap();
            derivation.shards.disable = true;
        }
        models::AnySpec::Materialization(m) => m.shards.disable = true,
        models::AnySpec::Test(_) => unreachable!(),
    };
    let scope = tables::synthetic_scope(task_type, catalog_name);
    draft.add_any_spec(catalog_name, scope, Some(start.last_pub_id), spec, false);

    harness
        .control_plane()
        .publish(
            Some(format!("disabling {catalog_name}")),
            Uuid::new_v4(),
            draft,
            None,
        )
        .await
        .expect("failed to publish")
        .error_for_status()
        .expect("publication failed");

    let _disabled_state = harness.run_pending_controller(catalog_name).await;

    harness
        .assert_alert_clear(catalog_name, AlertType::TaskChronicallyFailing)
        .await;

    // Re-enable for next iteration.
    let start = harness.get_controller_state(catalog_name).await;
    let mut draft = tables::DraftCatalog::default();
    let mut spec = start.live_spec.clone().unwrap();
    match &mut spec {
        models::AnySpec::Capture(c) => c.shards.disable = false,
        models::AnySpec::Collection(c) => {
            let derivation = c.derive.as_mut().unwrap();
            derivation.shards.disable = false;
        }
        models::AnySpec::Materialization(m) => m.shards.disable = false,
        models::AnySpec::Test(_) => unreachable!(),
    };
    let scope = tables::synthetic_scope(task_type, catalog_name);
    draft.add_any_spec(catalog_name, scope, Some(start.last_pub_id), spec, false);

    harness
        .control_plane()
        .publish(
            Some(format!("re-enabling {catalog_name}")),
            Uuid::new_v4(),
            draft,
            None,
        )
        .await
        .expect("failed to publish")
        .error_for_status()
        .expect("re-enable publication failed");

    let _re_enabled_state = harness.run_pending_controller(catalog_name).await;
    harness
        .control_plane()
        .assert_activations(
            "after re-enabling",
            vec![(catalog_name, Some(task_type))],
        );
}

/// Idle detection: old task with no data movement and no user publication.
/// In the test DB, catalog_stats_daily is empty and all publications use the
/// system user, so both last_data_movement_ts and last_user_pub_at are NULL.
/// Combined with old created_at, this should trigger TaskIdle.
async fn test_idle_fires_when_no_data_and_old(
    harness: &mut TestHarness,
    task_type: CatalogType,
    catalog_name: &str,
) {
    tracing::info!(%catalog_name, "idle fires for old task with no data");
    publish_and_await_ready(harness, task_type, catalog_name).await;

    // Make the task old enough (past IDLE_THRESHOLD).
    push_back_created_at(catalog_name, chrono::Duration::days(45), harness).await;
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;

    let _state = harness.run_pending_controller(catalog_name).await;

    let alert = harness
        .assert_alert_firing(catalog_name, AlertType::TaskIdle)
        .await;

    let extra = &alert.alert.arguments.0;
    assert!(
        extra.get("disable_at").and_then(|v| v.as_str()).is_some(),
        "expected disable_at in alert arguments, got: {extra:?}"
    );
}

/// Idle detection is suppressed when ShardFailed is active.
async fn test_idle_suppressed_by_shard_failed(
    harness: &mut TestHarness,
    task_type: CatalogType,
    catalog_name: &str,
) {
    tracing::info!(%catalog_name, "idle suppressed by shard_failed");
    publish_and_await_ready(harness, task_type, catalog_name).await;

    push_back_created_at(catalog_name, chrono::Duration::days(45), harness).await;
    // Trigger a real ShardFailed alert (recent, under chronically-failing threshold).
    trigger_shard_failed_alert(catalog_name, chrono::Duration::days(5), harness).await;

    // Run the controller again so the abandon logic evaluates.
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;
    let _state = harness.run_pending_controller(catalog_name).await;

    // Idle should be suppressed due to active ShardFailed.
    harness
        .assert_alert_clear(catalog_name, AlertType::TaskIdle)
        .await;
}

// -- Helper functions --

fn shard_ref(build_id: models::Id, name: &str) -> ShardRef {
    ShardRef {
        name: name.to_string(),
        build: build_id,
        key_begin: "00000000".to_string(),
        r_clock_begin: "00000000".to_string(),
    }
}

async fn publish_and_await_ready(
    harness: &mut TestHarness,
    task_type: CatalogType,
    catalog_name: &str,
) -> ControllerState {
    harness
        .upsert_connector_status(
            catalog_name,
            models::status::ConnectorStatus {
                shard: shard_ref(models::Id::new([1; 8]), catalog_name),
                ts: chrono::Utc::now() - chrono::Duration::minutes(60),
                message: "a stale status".to_string(),
                fields: Default::default(),
            },
        )
        .await;

    republish_spec(harness, task_type, catalog_name).await;

    harness
        .control_plane()
        .mock_shard_status(catalog_name, vec![replica_status::Code::Backfill]);
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(1), harness).await;
    let _first = harness.run_pending_controller(catalog_name).await;

    harness
        .control_plane()
        .mock_shard_status(catalog_name, vec![replica_status::Code::Primary]);
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(1), harness).await;
    let after_primary = harness.run_pending_controller(catalog_name).await;

    harness
        .upsert_connector_status(
            catalog_name,
            models::status::ConnectorStatus {
                shard: shard_ref(after_primary.last_build_id, catalog_name),
                ts: chrono::Utc::now(),
                message: "connector ok".to_string(),
                fields: Default::default(),
            },
        )
        .await;

    let ok_state = harness.run_pending_controller(catalog_name).await;
    assert!(
        ok_state.error.is_none(),
        "{catalog_name} controller failed: {ok_state:?}"
    );
    ok_state
}

async fn republish_spec(
    harness: &mut TestHarness,
    task_type: CatalogType,
    catalog_name: &str,
) {
    let prev_state = harness.get_controller_state(catalog_name).await;
    let mut draft = tables::DraftCatalog::default();
    let scope = tables::synthetic_scope(task_type, catalog_name);
    draft.add_any_spec(
        catalog_name,
        scope,
        Some(prev_state.last_pub_id),
        prev_state.live_spec.clone().unwrap(),
        false,
    );
    let result = harness
        .control_plane()
        .publish(None, Uuid::new_v4(), draft, None)
        .await
        .unwrap();
    assert!(
        result.status.is_success(),
        "{catalog_name} publish failed: {:?}",
        result.draft_errors()
    );

    let after_publish = harness.run_pending_controller(catalog_name).await;
    assert!(
        after_publish.error.is_none(),
        "{catalog_name} controller failed: {after_publish:?}"
    );
    harness
        .control_plane()
        .assert_activations("after republish", vec![(catalog_name, Some(task_type))]);
}

async fn override_shard_status_last_ts(
    catalog_name: &str,
    time_ago: chrono::Duration,
    harness: &mut TestHarness,
) {
    let new_ts = (chrono::Utc::now() - time_ago).to_rfc3339();

    tracing::debug!(%catalog_name, %new_ts, "overriding activation shard_status ts");
    sqlx::query!(
        r#"update controller_jobs set
        status = jsonb_set(status::jsonb, '{activation, shard_status, last_ts}', to_jsonb($2::text))::json
        where live_spec_id = (select id from live_specs where catalog_name = $1)
        and status->'activation'->'shard_status'->>'last_ts' is not null
        returning 1 as "must_exist: bool";"#,
        catalog_name,
        new_ts,
    )
    .fetch_one(&harness.pool)
    .await
    .expect("failed to override activation shard_health ts");
}

async fn push_back_created_at(
    catalog_name: &str,
    duration: chrono::Duration,
    harness: &mut TestHarness,
) {
    sqlx::query!(
        r#"update live_specs set created_at = now() - $2::interval
        where catalog_name = $1
        returning 1 as "must_exist: bool";"#,
        catalog_name,
        duration.to_string() as String,
    )
    .fetch_one(&harness.pool)
    .await
    .expect("failed to push back created_at");
}

/// Triggers a real ShardFailed alert by inserting shard failure events and
/// running the controller to process them. Then pushes back the alert's
/// first_ts to simulate it being `age` old.
async fn trigger_shard_failed_alert(
    catalog_name: &str,
    age: chrono::Duration,
    harness: &mut TestHarness,
) {
    let state = harness.get_controller_state(catalog_name).await;
    let shard = shard_ref(state.last_build_id, catalog_name);

    // Insert enough failures to trigger the alert (ALERT_AFTER_SHARD_FAILURES = 3)
    for _ in 0..3 {
        harness.fail_shard(&shard).await;
    }

    // Run the controller to process the failures and fire ShardFailed.
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;
    let _state = harness.run_pending_controller(catalog_name).await;

    // Verify ShardFailed is now firing.
    let controller_state = harness.get_controller_state(catalog_name).await;
    let alerts = controller_state.current_status.alerts_status().unwrap();
    assert!(
        alerts.contains_key(&AlertType::ShardFailed),
        "expected ShardFailed to be firing after injecting failures for {catalog_name}"
    );

    // Push back the first_ts to simulate the alert being `age` old.
    push_back_alert_first_ts(catalog_name, AlertType::ShardFailed, age, harness).await;
}

/// Removes an alert from the controller status JSONB.
async fn remove_alert_from_status(
    catalog_name: &str,
    alert_type: AlertType,
    harness: &mut TestHarness,
) {
    let alert_key = alert_type.name();
    let query = format!(
        "update controller_jobs set \
         status = (status::jsonb #- '{{alerts,{alert_key}}}')::json \
         where live_spec_id = (select id from live_specs where catalog_name = $1)"
    );
    sqlx::query(&query)
        .bind(catalog_name)
        .execute(&harness.pool)
        .await
        .expect("failed to remove alert from status");
}

async fn push_back_alert_first_ts(
    catalog_name: &str,
    alert_type: AlertType,
    by_duration: chrono::Duration,
    harness: &mut TestHarness,
) {
    let alert_key = alert_type.name();
    let new_ts = (chrono::Utc::now() - by_duration).to_rfc3339();

    let query = format!(
        "update controller_jobs set \
         status = jsonb_set(status::jsonb, '{{alerts,{alert_key},first_ts}}', to_jsonb($2::text))::json \
         where live_spec_id = (select id from live_specs where catalog_name = $1) \
         and status->'alerts'->'{alert_key}'->>'first_ts' is not null"
    );
    tracing::debug!(%catalog_name, %alert_key, %new_ts, %query, "pushing back alert first_ts");
    let result = sqlx::query(&query)
        .bind(catalog_name)
        .bind(&new_ts)
        .execute(&harness.pool)
        .await
        .expect("failed to push back alert first_ts");
    assert!(
        result.rows_affected() > 0,
        "push_back_alert_first_ts for {catalog_name}/{alert_key} updated 0 rows"
    );
}
