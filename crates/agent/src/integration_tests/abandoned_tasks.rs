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
        tracing::info!(%catalog_name, "starting abandoned task scenarios");

        test_stale_primary_fires(&mut harness, *task_type, *catalog_name).await;
        test_recent_connector_status_prevents_firing(&mut harness, *task_type, *catalog_name).await;
        test_recent_primary_prevents_firing(&mut harness, *task_type, *catalog_name).await;
        test_alert_resolves_on_sustained_primary(&mut harness, *task_type, *catalog_name).await;
        test_disable_leaves_alert_firing(&mut harness, *task_type, *catalog_name).await;
    }
}

/// Scenario 3: Old task with stale PRIMARY fires TaskAbandoned.
async fn test_stale_primary_fires(
    harness: &mut TestHarness,
    task_type: CatalogType,
    catalog_name: &str,
) {
    tracing::info!(%catalog_name, "scenario 3: stale PRIMARY fires");
    publish_and_await_ready(harness, task_type, catalog_name).await;

    push_back_created_at(catalog_name, chrono::Duration::days(20), harness).await;
    set_last_sustained_primary_ts(
        catalog_name,
        Some(chrono::Utc::now() - chrono::Duration::days(20)),
        harness,
    )
    .await;
    set_restarts_since_last_primary(catalog_name, 12, harness).await;
    delete_connector_status(catalog_name, harness).await;
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;

    let _state = harness.run_pending_controller(catalog_name).await;

    let alert = harness
        .assert_alert_firing(catalog_name, AlertType::TaskAbandoned)
        .await;

    // Verify extra fields in the alert_history arguments
    let extra = &alert.alert.arguments.0;
    assert_eq!(
        extra.get("disable_after_days").and_then(|v| v.as_i64()),
        Some(7),
        "expected disable_after_days=7, got: {extra:?}"
    );
    // last_primary_ts should be present because last_sustained_primary_ts was Some
    assert!(
        extra.get("last_primary_ts").is_some(),
        "expected last_primary_ts in alert arguments, got: {extra:?}"
    );
}

/// Scenario 4: Recent connector status prevents firing.
async fn test_recent_connector_status_prevents_firing(
    harness: &mut TestHarness,
    task_type: CatalogType,
    catalog_name: &str,
) {
    tracing::info!(%catalog_name, "scenario 4: recent connector status prevents firing");
    publish_and_await_ready(harness, task_type, catalog_name).await;

    // Make PRIMARY stale, but insert fresh connector status.
    push_back_created_at(catalog_name, chrono::Duration::days(20), harness).await;
    set_last_sustained_primary_ts(
        catalog_name,
        Some(chrono::Utc::now() - chrono::Duration::days(20)),
        harness,
    )
    .await;
    let state = harness.get_controller_state(catalog_name).await;
    upsert_connector_status_at(catalog_name, state.last_build_id, chrono::Utc::now(), harness)
        .await;
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;

    let _state = harness.run_pending_controller(catalog_name).await;

    harness
        .assert_alert_clear(catalog_name, AlertType::TaskAbandoned)
        .await;
}

/// Scenario 5: Recent PRIMARY prevents firing.
async fn test_recent_primary_prevents_firing(
    harness: &mut TestHarness,
    task_type: CatalogType,
    catalog_name: &str,
) {
    tracing::info!(%catalog_name, "scenario 5: recent PRIMARY prevents firing");
    publish_and_await_ready(harness, task_type, catalog_name).await;

    push_back_created_at(catalog_name, chrono::Duration::days(20), harness).await;
    set_last_sustained_primary_ts(
        catalog_name,
        Some(chrono::Utc::now() - chrono::Duration::days(3)),
        harness,
    )
    .await;
    delete_connector_status(catalog_name, harness).await;
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;

    let _state = harness.run_pending_controller(catalog_name).await;

    harness
        .assert_alert_clear(catalog_name, AlertType::TaskAbandoned)
        .await;
}

/// Scenario 6: Alert resolves on sustained PRIMARY.
///
/// Re-publishes to reset shard_status.count to 0, then fires the alert,
/// then runs 3 consecutive Primary health checks to trigger
/// `update_sustained_primary`, which resolves the alert.
async fn test_alert_resolves_on_sustained_primary(
    harness: &mut TestHarness,
    task_type: CatalogType,
    catalog_name: &str,
) {
    tracing::info!(%catalog_name, "scenario 6: alert resolves on sustained PRIMARY");

    // Re-publish to get a fresh activation with shard_status.count = 0.
    // We don't use the full publish_and_await_ready here because we need
    // the task in an abandoned state, not a healthy Ok state.
    republish_spec(harness, task_type, catalog_name).await;

    // Set up abandoned conditions: no sustained primary, old created_at, no connector status.
    set_last_sustained_primary_ts(catalog_name, None, harness).await;
    push_back_created_at(catalog_name, chrono::Duration::days(20), harness).await;
    delete_connector_status(catalog_name, harness).await;

    // Mock Backfill so the first health check produces Pending (not Ok),
    // preventing update_sustained_primary from firing prematurely.
    harness
        .control_plane()
        .mock_shard_status(catalog_name, vec![replica_status::Code::Backfill]);
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;

    let _state = harness.run_pending_controller(catalog_name).await;
    harness
        .assert_alert_firing(catalog_name, AlertType::TaskAbandoned)
        .await;

    // Now mock PRIMARY and run 3 consecutive health checks.
    // The activation controller needs SUSTAINED_PRIMARY_MIN_CHECKS (default 3)
    // consecutive Ok statuses to set last_sustained_primary_ts.
    // After re-publish, shard_status.count was 0. The Backfill check above
    // set it to Pending/count=1. Switching to Primary will transition
    // Pending -> Ok (count=1), then Ok -> Ok (count=2), then Ok -> Ok (count=3).
    harness
        .control_plane()
        .mock_shard_status(catalog_name, vec![replica_status::Code::Primary]);

    for i in 0..3 {
        override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;
        let state = harness.run_pending_controller(catalog_name).await;
        let activation = state
            .current_status
            .activation_status()
            .expect("expected activation status");
        let shard_check = activation.shard_status.as_ref().expect("expected shard status");
        tracing::info!(
            %catalog_name,
            check = i,
            count = shard_check.count,
            status = ?shard_check.status,
            last_sustained_primary_ts = ?activation.last_sustained_primary_ts,
            "health check cycle"
        );
    }

    // Verify last_sustained_primary_ts is now set.
    let final_state = harness.get_controller_state(catalog_name).await;
    let activation = final_state
        .current_status
        .activation_status()
        .expect("expected activation status");
    assert!(
        activation.last_sustained_primary_ts.is_some(),
        "expected last_sustained_primary_ts to be set after 3 consecutive Ok checks"
    );

    // The abandon stage should have resolved the alert since sustained PRIMARY is recent.
    harness
        .assert_alert_clear(catalog_name, AlertType::TaskAbandoned)
        .await;
}

/// Scenario 7: Disabling a task leaves TaskAbandoned alert firing.
async fn test_disable_leaves_alert_firing(
    harness: &mut TestHarness,
    task_type: CatalogType,
    catalog_name: &str,
) {
    tracing::info!(%catalog_name, "scenario 7: disable leaves alert firing");

    // Start from a clean state and make the alert fire.
    publish_and_await_ready(harness, task_type, catalog_name).await;

    set_last_sustained_primary_ts(catalog_name, None, harness).await;
    push_back_created_at(catalog_name, chrono::Duration::days(20), harness).await;
    delete_connector_status(catalog_name, harness).await;
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;

    let _state = harness.run_pending_controller(catalog_name).await;
    harness
        .assert_alert_firing(catalog_name, AlertType::TaskAbandoned)
        .await;

    // Publish with shards disabled.
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

    // The alert should STILL be firing in controller status (not resolved).
    let state = harness.get_controller_state(catalog_name).await;
    let alert_status = state
        .current_status
        .alerts_status()
        .and_then(|alerts| alerts.get(&AlertType::TaskAbandoned));
    assert!(
        alert_status.is_some_and(|a| a.state == models::status::AlertState::Firing),
        "expected TaskAbandoned alert to remain firing after disable, got: {alert_status:?}"
    );

    // Re-enable the task so the next task-type iteration starts clean.
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

// -- Helper functions --

fn shard_ref(build_id: models::Id, name: &str) -> ShardRef {
    ShardRef {
        name: name.to_string(),
        build: build_id,
        key_begin: "00000000".to_string(),
        r_clock_begin: "00000000".to_string(),
    }
}

/// Re-publishes the current spec (no changes), triggering a fresh activation
/// that resets shard_status.count to 0. Then advances the task through
/// Backfill -> Primary -> Ok with fresh connector status.
/// This is the canonical reset function: every scenario should call this
/// (or `republish_spec`) before setting up its conditions.
async fn publish_and_await_ready(
    harness: &mut TestHarness,
    task_type: CatalogType,
    catalog_name: &str,
) -> ControllerState {
    // Add a stale connector status so the controller waits for fresh status.
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

    // After re-publish: shard_status = Pending, count = 0.
    // Advance through Backfill -> Primary -> Ok with fresh connector status.
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

/// Re-publishes the current spec unchanged, triggering a fresh activation
/// that resets shard_status.count to 0. Does NOT advance to Ok state.
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

async fn set_last_sustained_primary_ts(
    catalog_name: &str,
    ts: Option<chrono::DateTime<chrono::Utc>>,
    harness: &mut TestHarness,
) {
    match ts {
        Some(ts) => {
            let ts_str = ts.to_rfc3339();
            sqlx::query!(
                r#"update controller_jobs set
                status = jsonb_set(status::jsonb, '{activation, last_sustained_primary_ts}', to_jsonb($2::text))::json
                where live_spec_id = (select id from live_specs where catalog_name = $1)
                returning 1 as "must_exist: bool";"#,
                catalog_name,
                ts_str,
            )
            .fetch_one(&harness.pool)
            .await
            .expect("failed to set last_sustained_primary_ts");
        }
        None => {
            sqlx::query!(
                r#"update controller_jobs set
                status = (status::jsonb #- '{activation, last_sustained_primary_ts}')::json
                where live_spec_id = (select id from live_specs where catalog_name = $1)
                returning 1 as "must_exist: bool";"#,
                catalog_name,
            )
            .fetch_one(&harness.pool)
            .await
            .expect("failed to remove last_sustained_primary_ts");
        }
    }
}

async fn set_restarts_since_last_primary(
    catalog_name: &str,
    count: u32,
    harness: &mut TestHarness,
) {
    sqlx::query!(
        r#"update controller_jobs set
        status = jsonb_set(status::jsonb, '{activation, restarts_since_last_primary}', to_jsonb($2::int))::json
        where live_spec_id = (select id from live_specs where catalog_name = $1)
        returning 1 as "must_exist: bool";"#,
        catalog_name,
        count as i32,
    )
    .fetch_one(&harness.pool)
    .await
    .expect("failed to set restarts_since_last_primary");
}

async fn delete_connector_status(catalog_name: &str, harness: &mut TestHarness) {
    sqlx::query!(
        r#"delete from connector_status where catalog_name = $1;"#,
        catalog_name,
    )
    .execute(&harness.pool)
    .await
    .expect("failed to delete connector_status");
}

async fn upsert_connector_status_at(
    catalog_name: &str,
    build_id: models::Id,
    ts: chrono::DateTime<chrono::Utc>,
    harness: &mut TestHarness,
) {
    harness
        .upsert_connector_status(
            catalog_name,
            models::status::ConnectorStatus {
                shard: shard_ref(build_id, catalog_name),
                ts,
                message: "connector ok".to_string(),
                fields: Default::default(),
            },
        )
        .await;
}
