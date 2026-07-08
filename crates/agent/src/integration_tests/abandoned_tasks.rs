use crate::{
    ControlPlane,
    integration_tests::harness::{TestHarness, assert_within_minutes, draft_catalog},
    integration_tests::shard_failures::{publish_and_await_ready, shard_ref},
};
use models::{CatalogType, status::AlertType};
use uuid::Uuid;

#[tokio::test]
async fn test_abandoned_task_detection_and_resolution() {
    let mut harness = TestHarness::init("test_abandoned_tasks").await;
    let _user_id = harness.setup_tenant("pandas").await;

    // Disable the check interval so evaluations run on every controller wake.
    let mut config = (*harness.control_plane().controller_config()).clone();
    config.abandon_check_interval = chrono::Duration::zero();
    harness.control_plane().set_controller_config(config);

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

    // Verify has_task_shards works for materialization and derivation task types
    // with real built specs. Capture coverage comes from auto-disable below.
    test_shard_failed_fires_abandonment_alert(
        &mut harness,
        CatalogType::Materialization,
        "pandas/materialize",
    )
    .await;
    test_shard_failed_fires_abandonment_alert(&mut harness, CatalogType::Collection, "pandas/luck")
        .await;

    // Idle detection exercises `fetch_abandonment_timestamps()` with real SQL
    // against `publication_specs` and `catalog_stats_daily`.
    test_idle_fires_when_no_data_and_old(&mut harness, CatalogType::Capture, "pandas/capture")
        .await;

    // Auto-disable runs last because it leaves tasks disabled.
    test_auto_disable_idle(
        &mut harness,
        CatalogType::Materialization,
        "pandas/materialize",
    )
    .await;
    test_auto_disable_failing(&mut harness, CatalogType::Capture, "pandas/capture").await;
}

/// ShardFailed firing for > 30 days triggers TaskChronicallyFailing.
async fn test_shard_failed_fires_abandonment_alert(
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

    // The controller should schedule a future wake within the 24h abandon safety-net,
    // coalesced with the ~2h activation health check interval.
    let wake_at = harness.assert_controller_pending(catalog_name).await;
    assert_within_minutes(wake_at, 25 * 60);
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

    // Make the task old enough (past the idle threshold).
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

    let wake_at = harness.assert_controller_pending(catalog_name).await;
    assert_within_minutes(wake_at, 25 * 60);
}

/// With `taskIdle.autoDisable = true` configured on the task, the
/// controller publishes `shards.disable = true` when the idle grace
/// period expires.
async fn test_auto_disable_idle(
    harness: &mut TestHarness,
    task_type: CatalogType,
    catalog_name: &str,
) {
    tracing::info!(%catalog_name, "idle auto-disable publishes shards.disable");

    harness
        .upsert_alert_config(
            catalog_name,
            serde_json::json!({ "taskIdle": { "autoDisable": true } }),
        )
        .await;

    publish_and_await_ready(harness, task_type, catalog_name).await;

    // Make the task old enough and trigger TaskIdle.
    push_back_created_at(catalog_name, chrono::Duration::days(45), harness).await;
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;
    harness.run_pending_controller(catalog_name).await;
    harness
        .assert_alert_firing(catalog_name, AlertType::TaskIdle)
        .await;

    // Expire the grace period and run the controller to auto-disable.
    set_alert_disable_at(
        catalog_name,
        AlertType::TaskIdle,
        (chrono::Utc::now() - chrono::Duration::days(1))
            .format("%Y-%m-%d")
            .to_string(),
        harness,
    )
    .await;
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;

    harness.run_pending_controller(catalog_name).await;
    harness.run_pending_controllers(None).await;

    let state = harness.get_controller_state(catalog_name).await;
    let spec = state.live_spec.as_ref().expect("spec should exist");
    let is_disabled = match spec {
        models::AnySpec::Capture(c) => c.shards.disable,
        models::AnySpec::Collection(c) => c.derive.as_ref().map_or(false, |d| d.shards.disable),
        models::AnySpec::Materialization(m) => m.shards.disable,
        models::AnySpec::Test(_) => unreachable!(),
    };
    assert!(
        is_disabled,
        "expected {catalog_name} to be disabled after idle auto-disable, but shards.disable is false"
    );

    harness
        .assert_alert_clear(catalog_name, AlertType::TaskIdle)
        .await;
    harness
        .assert_alert_clear(catalog_name, AlertType::TaskAutoDisabledIdle)
        .await;
}

/// With `taskChronicallyFailing.autoDisable = true` configured on the
/// task, the controller publishes `shards.disable = true` when the
/// chronically-failing grace period expires.
async fn test_auto_disable_failing(
    harness: &mut TestHarness,
    task_type: CatalogType,
    catalog_name: &str,
) {
    tracing::info!(%catalog_name, "failing auto-disable publishes shards.disable");

    harness
        .upsert_alert_config(
            catalog_name,
            serde_json::json!({ "taskChronicallyFailing": { "autoDisable": true } }),
        )
        .await;

    publish_and_await_ready(harness, task_type, catalog_name).await;

    // Trigger ShardFailed > 30 days, fire TaskChronicallyFailing.
    trigger_shard_failed_alert(catalog_name, chrono::Duration::days(35), harness).await;
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;
    let _state = harness.run_pending_controller(catalog_name).await;
    harness
        .assert_alert_firing(catalog_name, AlertType::TaskChronicallyFailing)
        .await;

    // Set disable_at to yesterday so the grace period appears expired.
    // The grace period check compares `now.date_naive() >= disable_at`;
    // disable_at is immutable once set, so we must update it directly.
    set_alert_disable_at(
        catalog_name,
        AlertType::TaskChronicallyFailing,
        (chrono::Utc::now() - chrono::Duration::days(1))
            .format("%Y-%m-%d")
            .to_string(),
        harness,
    )
    .await;
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;

    // Run the controller: it should fire TaskAutoDisabledFailing AND publish
    // shards.disable = true, then re-enter and process the disabled spec.
    harness.run_pending_controller(catalog_name).await;
    harness.run_pending_controllers(None).await;

    // Verify the spec is now disabled.
    let state = harness.get_controller_state(catalog_name).await;
    let spec = state.live_spec.as_ref().expect("spec should exist");
    let is_disabled = match spec {
        models::AnySpec::Capture(c) => c.shards.disable,
        models::AnySpec::Collection(c) => c.derive.as_ref().map_or(false, |d| d.shards.disable),
        models::AnySpec::Materialization(m) => m.shards.disable,
        models::AnySpec::Test(_) => unreachable!(),
    };
    assert!(
        is_disabled,
        "expected {catalog_name} to be disabled after auto-disable, but shards.disable is false"
    );

    // All abandon alerts should be cleared on the post-disable controller run.
    harness
        .assert_alert_clear(catalog_name, AlertType::TaskChronicallyFailing)
        .await;
    harness
        .assert_alert_clear(catalog_name, AlertType::TaskAutoDisabledFailing)
        .await;
}

async fn override_shard_status_last_ts(
    catalog_name: &str,
    time_ago: chrono::Duration,
    harness: &mut TestHarness,
) {
    let new_ts = (chrono::Utc::now() - time_ago).to_rfc3339();

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

    // Insert enough failures to trigger the alert (default threshold is 3)
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

/// Sets the `disable_at` date in an alert's flattened `extra` map.
/// `disable_at` is immutable once set by `evaluate_alerts`, so this is
/// the only way to simulate grace period expiry in integration tests.
async fn set_alert_disable_at(
    catalog_name: &str,
    alert_type: AlertType,
    date: String,
    harness: &mut TestHarness,
) {
    let alert_key = alert_type.name();
    let query = format!(
        "update controller_jobs set \
         status = jsonb_set(status::jsonb, '{{alerts,{alert_key},disable_at}}', to_jsonb($2::text))::json \
         where live_spec_id = (select id from live_specs where catalog_name = $1) \
         and status->'alerts'->'{alert_key}'->>'disable_at' is not null"
    );
    tracing::debug!(%catalog_name, %alert_key, %date, "setting alert disable_at");
    let result = sqlx::query(&query)
        .bind(catalog_name)
        .bind(&date)
        .execute(&harness.pool)
        .await
        .expect("failed to set alert disable_at");
    assert!(
        result.rows_affected() > 0,
        "set_alert_disable_at for {catalog_name}/{alert_key} updated 0 rows"
    );
}
