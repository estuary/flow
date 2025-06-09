use crate::{
    controllers::ControllerState,
    integration_tests::harness::{draft_catalog, TestHarness},
    ControlPlane,
};
use gazette::consumer::replica_status;
use models::{
    status::{activation::ShardsStatus, ShardRef, StatusSummaryType},
    CatalogType,
};
use uuid::Uuid;

#[tokio::test]
#[serial_test::serial]
async fn test_shard_failures_and_retries() {
    let mut harness = TestHarness::init("test_shard_failures_and_retries").await;
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
        },
        "tests": {
            "pandas/test-test": {
                "description": "a test of testing",
                "steps": [
                    {"ingest": {
                        "collection": "pandas/bamboo",
                        "documents": [{"id": "shooty shoot!"}]
                    }},
                    {"verify": {
                        "collection": "pandas/luck",
                        "documents": [{"id": "wooty woot!"}]
                    }}
                ]
            }
        }
    }));

    let result = harness
        .control_plane()
        .publish(
            Some(format!("initial publication")),
            Uuid::new_v4(),
            draft,
            Some("ops/dp/public/test".to_string()),
        )
        .await
        .expect("initial publish failed");
    assert!(
        result.status.is_success(),
        "publication failed with: {:?}",
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
    // All tasks should be pending because controllers haven't actually observed
    // their shard statuses yet.
    assert_status_shards_pending(&mut harness, "pandas/capture").await;
    assert_status_shards_pending(&mut harness, "pandas/materialize").await;
    assert_status_shards_pending(&mut harness, "pandas/luck").await;
    // Regular collections and tests should be Ok since they have no shards or
    // connector status.
    assert_status_summary_ok(&mut harness, "pandas/bamboo", "Ok").await;
    assert_status_summary_ok(&mut harness, "pandas/test-test", "Test passed").await;

    // We'll run through the same test scenarios for each of the task types
    let tasks = &[
        ("pandas/capture", models::CatalogType::Capture),
        ("pandas/materialize", models::CatalogType::Materialization),
        ("pandas/luck", models::CatalogType::Collection),
    ];
    for (catalog_name, task_type) in tasks {
        // Test task shard failures that are observed as ShardFailed events.
        // This is what happens under normal failure scenarios.
        test_backoff_repeated_shard_failures(&mut harness, *task_type, *catalog_name).await;

        // Test task shard failures that are observed via status checks, without
        // any ShardFailed events.
        test_shard_failed_without_event(&mut harness, *task_type, *catalog_name).await;

        // Test observing ShardFailed events after observing failed shard status
        // via status checks.
        test_shard_failed_event_after_failed_check(&mut harness, *task_type, *catalog_name).await;

        // Lastly, test disabling a task that's in a Failed state
        test_transition_to_disabled(&mut harness, *task_type, *catalog_name).await;
    }
}

async fn test_shard_failed_without_event(
    harness: &mut TestHarness,
    task_type: models::CatalogType,
    catalog_name: &str,
) {
    let starting_state = publish_and_await_ready(harness, task_type, catalog_name).await;

    let last_activation_ts = starting_state
        .current_status
        .activation_status()
        .unwrap()
        .last_activated_at
        .unwrap();

    harness
        .control_plane()
        .mock_shard_status(catalog_name, vec![replica_status::Code::Failed]);

    for i in 0..2 {
        // Simulate the passage of sufficient time that the controller will check the shard status again
        override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(10), harness).await;

        let after_fail_state = harness.run_pending_controller(catalog_name).await;
        // Unlike shard failures that are observed through ShardFailed events, failures that are
        // observed via periodic status checks are not immediately re-activated.
        harness
            .control_plane()
            .assert_activations("after obvserved failure without event", Vec::new());
        let activation = after_fail_state.current_status.activation_status().unwrap();
        let status = activation.shard_status.as_ref().unwrap();
        assert_eq!(ShardsStatus::Failed, status.status);
        assert_eq!(i + 1, status.count);
        // The next_retry should remain unset because shard failures that are
        // observed in this way use a separate retry interval.
        assert!(
            activation.next_retry.is_none(),
            "i={i}, expected no retry to be scheduled"
        );

        assert_status_summary(
            harness,
            catalog_name,
            StatusSummaryType::Error,
            "one or more task shards are failed",
        )
        .await;
    }

    // The last check should result in the controller creating a ShardFailure event
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(3), harness).await;

    let _after_third_check = harness.run_pending_controller(catalog_name).await;
    let failures = harness
        .control_plane()
        .get_shard_failures(catalog_name.to_string())
        .await
        .unwrap();
    assert_eq!(1, failures.len(), "expected 1 failure, got: {failures:?}");
    harness
        .control_plane()
        .assert_activations("after third failed status check", Vec::new());

    let after_activation = harness.run_pending_controller(catalog_name).await;

    harness.control_plane().assert_activations(
        "after ShardFailure event created",
        vec![(catalog_name, Some(task_type))],
    );

    let activation = after_activation.current_status.activation_status().unwrap();
    assert!(
        activation.last_activated_at > Some(last_activation_ts),
        "expect last_activated_at to be updated"
    );
    assert_eq!(activation.last_activated, starting_state.last_build_id);
    let status = activation.shard_status.as_ref().unwrap();
    assert_eq!(ShardsStatus::Pending, status.status);
    assert_eq!(0, status.count);
    assert!(
        activation.next_retry.is_none(),
        "expected no retry to be scheduled"
    );
    assert_status_shards_pending(harness, catalog_name).await;
}

async fn test_transition_to_disabled(
    harness: &mut TestHarness,
    task_type: models::CatalogType,
    catalog_name: &str,
) {
    assert_status_summary(
        harness,
        catalog_name,
        StatusSummaryType::Error,
        "task shard failed",
    )
    .await;

    let start = harness.get_controller_state(catalog_name).await;

    let mut draft = tables::DraftCatalog::default();
    let mut spec = start.live_spec.clone().unwrap();
    match &mut spec {
        models::AnySpec::Capture(ref mut c) => c.shards.disable = true,
        models::AnySpec::Collection(ref mut c) => {
            let derivation = c.derive.as_mut().unwrap();
            derivation.shards.disable = true;
        }
        models::AnySpec::Materialization(ref mut m) => m.shards.disable = true,
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

    let disabled_state = harness.run_pending_controller(catalog_name).await;
    // Controller should have activated, setting the shards to disabled.
    harness
        .control_plane()
        .assert_activations("after disabling", vec![(catalog_name, Some(task_type))]);
    let activation = disabled_state.current_status.activation_status().unwrap();
    assert!(
        activation.shard_status.is_none(),
        "expect shard status check to be removed"
    );

    if task_type == models::CatalogType::Collection {
        assert_status_summary(harness, catalog_name, StatusSummaryType::Ok, "Ok").await;
    } else {
        assert_status_summary(
            harness,
            catalog_name,
            StatusSummaryType::TaskDisabled,
            "Task shards are disabled",
        )
        .await;
    }
}

async fn test_shard_failed_event_after_failed_check(
    harness: &mut TestHarness,
    task_type: models::CatalogType,
    catalog_name: &str,
) {
    let starting_state = publish_and_await_ready(harness, task_type, catalog_name).await;

    // Simulate the passage of sufficient time that the controller _would_ attempt re-activating
    // the shards after the third consecutive status check showing failure, execpt that this time
    // it will observe the shard failure events before that happens.
    override_last_activated_at(catalog_name, chrono::Duration::minutes(16), harness).await;

    harness
        .control_plane()
        .mock_shard_status(catalog_name, vec![replica_status::Code::Failed]);
    for i in 0..2 {
        // Simulate the passage of sufficient time that the controller will check the shard status again
        override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(20), harness).await;

        let after_fail_state = harness.run_pending_controller(catalog_name).await;
        // Unlike shard failures that are observed through ShardFailed events, failures that are
        // observed via periodic status checks are not immediately re-activated.
        harness
            .control_plane()
            .assert_activations("after obvserved failure without event", Vec::new());
        let activation = after_fail_state.current_status.activation_status().unwrap();
        let status = activation.shard_status.as_ref().unwrap();
        assert_eq!(ShardsStatus::Failed, status.status);
        assert_eq!(i + 1, status.count);
        // The next_retry should remain unset because shard failures that are
        // observed in this way use a separate retry interval.
        assert!(
            activation.next_retry.is_none(),
            "i={i}, expected no retry to be scheduled"
        );

        assert_status_summary(
            harness,
            catalog_name,
            StatusSummaryType::Error,
            "one or more task shards are failed",
        )
        .await;
    }

    // Push back the last status check time, so that the controller _would_ re-check the status,
    // except it shouldn't do so this time because of the  ShardFailed events.
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(5), harness).await;

    // Insert a bunch of shard failure events, which should result in a significant backoff
    let shard = shard_ref(starting_state.last_build_id, catalog_name);
    for _ in 0..10 {
        harness.fail_shard(&shard).await;
    }

    // Assert that future controller runs do not re-activate or even check the shard health,
    // since the `next_retry` should take precidence.
    for i in 0..5 {
        override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(10), harness).await;
        let state = harness.run_pending_controller(catalog_name).await;
        let activation = state.current_status.activation_status().unwrap();
        assert!(
            activation.next_retry.is_some(),
            "i={i} expect {catalog_name} next_retry to be Some"
        );
        harness
            .control_plane()
            .assert_activations("after observing shard failure events", Vec::new());

        let shard_check = activation.shard_status.as_ref().unwrap();
        assert_eq!(ShardsStatus::Failed, shard_check.status);
        assert_eq!(3, shard_check.count);
    }
}

async fn test_backoff_repeated_shard_failures(
    harness: &mut TestHarness,
    task_type: models::CatalogType,
    catalog_name: &str,
) {
    let starting_state = publish_and_await_ready(harness, task_type, catalog_name).await;

    let shard = shard_ref(starting_state.last_build_id, catalog_name);
    let mut last_activation = starting_state
        .current_status
        .activation_status()
        .unwrap()
        .last_activated_at;
    for i in 0..7 {
        harness.fail_shard(&shard).await;
        let after_fail = harness.run_pending_controller(catalog_name).await;

        let activation = &after_fail.current_status.activation_status().unwrap();
        assert_eq!(
            activation.last_activated, starting_state.last_build_id,
            "i={i} the activated build id should not have changed"
        );
        assert_eq!(
            i + 1,
            activation.recent_failure_count,
            "i={i} failures should be counted correctly"
        );

        if i <= 1 {
            // The first two failures should be retried immediately
            harness.control_plane().assert_activations(
                &format!("after failure, i={i}"),
                vec![(catalog_name, Some(task_type))],
            );
            assert!(
                activation.next_retry.is_none(),
                "i={i} next_retry should be None because it should have already been re-activated"
            );
            let reactivation_ts = activation.last_activated_at;
            assert!(
                reactivation_ts > last_activation,
                "i={i} expected last_activated_at to be after the inital controller run"
            );
            last_activation = reactivation_ts;

            assert_status_shards_pending(harness, catalog_name).await;
        } else {
            // Expect that nothing was activated
            harness
                .control_plane()
                .assert_activations(&format!("after failure, i={i}"), Vec::new());
            assert_eq!(
                last_activation, activation.last_activated_at,
                "i={i} expect last_activated_at to be the same"
            );
            assert!(activation.next_retry.is_some());

            assert!(after_fail.error.is_none());
            assert_status_summary(
                harness,
                catalog_name,
                StatusSummaryType::Error,
                "task shard failed",
            )
            .await;
        }
    }

    override_next_retry_now(catalog_name, harness).await;
    let after_retry = harness.run_pending_controller(catalog_name).await;
    assert!(
        after_retry.error.is_none(),
        "unexpected controller error: {after_retry:?}"
    );
    harness.control_plane().assert_activations(
        "after backoff elapsed",
        vec![(catalog_name, Some(task_type))],
    );
    assert_status_shards_pending(harness, catalog_name).await;
    task_becomes_ok(harness, task_type, catalog_name).await;

    // One more failure should result in backing off again
    harness.fail_shard(&shard).await;
    let after_fail = harness.run_pending_controller(catalog_name).await;
    harness
        .control_plane()
        .assert_activations("after final failure", Vec::new());
    let activation = after_fail.current_status.activation_status().unwrap();
    assert!(activation.next_retry.is_some());
    assert_eq!(8, activation.recent_failure_count);
    assert_status_summary(
        harness,
        catalog_name,
        StatusSummaryType::Error,
        "task shard failed",
    )
    .await;
}

async fn publish_and_await_ready(
    harness: &mut TestHarness,
    task_type: models::CatalogType,
    catalog_name: &str,
) -> ControllerState {
    // Add a stale status, to ensure that our future status will show us waiting
    // for an up-to-date connector status.
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
        .assert_activations("after publish", vec![(catalog_name, Some(task_type))]);
    assert_status_shards_pending(harness, catalog_name).await;

    let activation_status = after_publish.current_status.activation_status().unwrap();
    let after_publish_shard_status = activation_status.shard_status.as_ref().unwrap();
    assert_eq!(
        0, after_publish_shard_status.count,
        "{catalog_name} count should always be 0 right after activating a new build"
    );
    assert_eq!(
        ShardsStatus::Pending,
        after_publish_shard_status.status,
        "{catalog_name} shards status should always be pending right after activating a new build"
    );
    assert_eq!(
        0, activation_status.recent_failure_count,
        "expect fail count is 0 because a new build was activated"
    );
    let failures = harness
        .control_plane()
        .get_shard_failures(catalog_name.to_string())
        .await
        .unwrap();
    assert!(
        failures.is_empty(),
        "expected no shard failures, got: {failures:?}"
    );

    task_becomes_ok(harness, task_type, catalog_name).await
}

async fn task_becomes_ok(
    harness: &mut TestHarness,
    _task_type: models::CatalogType,
    catalog_name: &str,
) -> ControllerState {
    harness
        .control_plane()
        .mock_shard_status(catalog_name, vec![replica_status::Code::Backfill]);
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(1), harness).await;

    let after_first_check = harness.run_pending_controller(catalog_name).await;
    assert!(
        after_first_check.error.is_none(),
        "{catalog_name} controller failed: {after_first_check:?}"
    );
    assert_status_shards_pending(harness, catalog_name).await;
    let after_first_check_status = after_first_check
        .current_status
        .activation_status()
        .unwrap()
        .shard_status
        .as_ref()
        .unwrap();
    assert_eq!(
        1, after_first_check_status.count,
        "{catalog_name} shards status should have been checked"
    );
    assert_eq!(
        ShardsStatus::Pending,
        after_first_check_status.status,
        "{catalog_name} shards status still be pending"
    );

    harness
        .control_plane()
        .mock_shard_status(catalog_name, vec![replica_status::Code::Primary]);
    override_shard_status_last_ts(catalog_name, chrono::Duration::minutes(1), harness).await;
    let expect_primary_check = harness.run_pending_controller(catalog_name).await;
    assert!(
        expect_primary_check.error.is_none(),
        "{catalog_name} controller failed: {expect_primary_check:?}"
    );
    let expect_primary_check_activation = expect_primary_check
        .current_status
        .activation_status()
        .unwrap();
    assert!(expect_primary_check_activation.next_retry.is_none());
    let shard_status = expect_primary_check_activation
        .shard_status
        .as_ref()
        .unwrap();
    assert_eq!(1, shard_status.count);
    assert_eq!(ShardsStatus::Ok, shard_status.status);

    let summary = harness.status_summary(catalog_name).await;
    assert_eq!(
        StatusSummaryType::Warning,
        summary.status,
        "unexpected status: {summary:?}"
    );
    assert_eq!(
        "waiting on connector status", summary.message,
        "{catalog_name} unexpected status summary"
    );

    harness
        .upsert_connector_status(
            catalog_name,
            models::status::ConnectorStatus {
                shard: shard_ref(expect_primary_check.last_build_id, catalog_name),
                ts: chrono::Utc::now(),
                message: "we doin aight".to_string(),
                fields: Default::default(),
            },
        )
        .await;

    let expect_ok = harness.run_pending_controller(catalog_name).await;
    assert!(
        expect_ok.error.is_none(),
        "{catalog_name} controller failed: {expect_ok:?}"
    );
    let expect_ok_activation = expect_ok.current_status.activation_status().unwrap();
    assert!(expect_ok_activation.next_retry.is_none());
    let shard_status = expect_ok_activation.shard_status.as_ref().unwrap();
    assert_eq!(1, shard_status.count);
    assert_eq!(ShardsStatus::Ok, shard_status.status);

    let summary = harness.status_summary(catalog_name).await;
    assert_eq!(
        StatusSummaryType::Ok,
        summary.status,
        "{catalog_name} unexpected status: {summary:?}"
    );
    assert_eq!(
        "we doin aight", summary.message,
        "{catalog_name} unexpected summary message"
    );
    expect_ok
}

fn shard_ref(build_id: models::Id, name: &str) -> ShardRef {
    ShardRef {
        name: name.to_string(),
        build: build_id,
        key_begin: "00000000".to_string(),
        r_clock_begin: "00000000".to_string(),
    }
}

async fn override_next_retry_now(catalog_name: &str, harness: &mut TestHarness) {
    let retry_at = harness.control_plane().current_time().to_rfc3339();
    tracing::debug!(%catalog_name, %retry_at, "overriding next_retry");
    sqlx::query!(
        r#"update controller_jobs set
        status = jsonb_set(status::jsonb, '{activation, next_retry}', to_jsonb($2::text))::json
        where live_spec_id = (select id from live_specs where catalog_name = $1)
        and status->'activation'->>'next_retry' is not null
        returning 1 as "must_exist: bool";"#,
        catalog_name,
        retry_at,
    )
    .fetch_one(&harness.pool)
    .await
    .expect("failed to override next_retry time");
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

async fn override_last_activated_at(
    catalog_name: &str,
    time_ago: chrono::Duration,
    harness: &mut TestHarness,
) {
    let new_ts = (chrono::Utc::now() - time_ago).to_rfc3339();

    tracing::debug!(%catalog_name, %new_ts, "overriding last_activated_at");
    sqlx::query!(
        r#"update controller_jobs set
        status = jsonb_set(status::jsonb, '{activation, last_activated_at}', to_jsonb($2::text))::json
        where live_spec_id = (select id from live_specs where catalog_name = $1)
        and status->'activation'->>'last_activated_at' is not null
        returning 1 as "must_exist: bool";"#,
        catalog_name,
        new_ts,
    )
    .fetch_one(&harness.pool)
    .await
    .expect("failed to override last_activated_at time");
}

async fn assert_status_summary(
    harness: &mut TestHarness,
    catalog_name: &str,
    expect_status: StatusSummaryType,
    expect_message_prefix: &str,
) {
    let status = harness.status_summary(catalog_name).await;
    assert_eq!(
        expect_status, status.status,
        "unexpected status, expected {expect_status:?}, got: {status:?}"
    );
    assert!(
        &status.message.starts_with(expect_message_prefix),
        "{catalog_name} unexpected status message, expected '{expect_message_prefix}', got: {status:?}"
    );
}

async fn assert_status_summary_ok(
    harness: &mut TestHarness,
    task: &str,
    expect_message_prefix: &str,
) {
    assert_status_summary(harness, task, StatusSummaryType::Ok, expect_message_prefix).await;
}

async fn assert_status_shards_pending(harness: &mut TestHarness, task: &str) {
    assert_status_summary(
        harness,
        task,
        StatusSummaryType::Warning,
        "waiting for task shards to be ready",
    )
    .await;
}
