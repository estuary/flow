use crate::{
    integration_tests::harness::{
        draft_catalog, InjectBuildError, TestHarness,
    },
    ControlPlane,
};
use models::{status::ShardRef, AnySpec, CaptureEndpoint, MaterializationEndpoint};
use uuid::Uuid;

fn initial_config() -> serde_json::Value {
    serde_json::json!({
        "credentials": {
            "client_id": "my_client_id",
            "client_secret": "my_client_secret",
            "refresh_token": "initial_refresh_token"
        },
        "start_date": "2025-05-05T00:00:00Z"
    })
}

fn updated_config() -> serde_json::Value {
    serde_json::json!({
        "credentials": {
            "client_id": "my_client_id",
            "client_secret": "my_client_secret",
            "refresh_token": "updated_refresh_token"
        },
        "start_date": "2025-05-06T00:00:00Z"
    })
}

#[tokio::test]
#[serial_test::serial]
async fn test_config_update_publication_success() {
    // Set up harness & tenant.
    let mut harness = TestHarness::init("test_config_update_publication_success").await;
    let _user_id = harness.setup_tenant("ducks").await;

    const CAPTURE_NAME: &str = "ducks/capture";
    const MATERIALIZATION_NAME: &str = "ducks/materialization";

    // Draft a catalog including both a capture and a materialization.
    let initial_draft = draft_catalog(serde_json::json!({
        "collections": {
            "ducks/pond/quacks": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                },
                "key": ["/id"]
            }
        },
        "captures": {
            CAPTURE_NAME: {
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": initial_config(),
                    }
                },
                "bindings": [
                    {
                        "resource": {
                            "name": "greetings",
                            "prefix": "Howdy {}!"
                        },
                        "target": "ducks/pond/quacks"
                    }
                ]
            }
        },
        "materializations": {
            MATERIALIZATION_NAME: {
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": initial_config(),
                    }
                },
                "bindings": [ ]
            }
        }
    }));

    // Publish the catalog.
    let result = harness
        .control_plane()
        .publish(
            Some(format!("initial publication")),
            Uuid::new_v4(),
            initial_draft,
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

    // Confirm captures republish a new spec with the updated config
    // in response to an insert into the config_updates table.

    // Inject a configUpdate event for the capture.
    let capture_state = harness.get_controller_state(CAPTURE_NAME).await;

    upsert_config_update(
        &mut harness,
        &ShardRef {
            name: CAPTURE_NAME.to_string(),
            build: capture_state.last_build_id,
            key_begin: "00000000".to_string(),
            r_clock_begin: "00000000".to_string(),
        },
        updated_config(),
    )
    .await;
    let capture_state = harness.run_pending_controller(CAPTURE_NAME).await;

    // Assert the capture's config is updated.
    let capture_live_spec = capture_state.live_spec.unwrap();

    if let AnySpec::Capture(capture_def) = capture_live_spec {
        if let CaptureEndpoint::Connector(capture_endpoint) = capture_def.endpoint {
            assert!(updated_config() == capture_endpoint.config.to_value());
        };
    } else {
        panic!("Expected capture spec");
    }

    // Confirm materializations republish a new spec with the updated config
    // in response to an insert into the config_updates table.

    // Inject a configUpdate event for the materialization.
    let materialization_state = harness.get_controller_state(MATERIALIZATION_NAME).await;

    upsert_config_update(
        &mut harness,
        &ShardRef {
            name: MATERIALIZATION_NAME.to_string(),
            build: materialization_state.last_build_id,
            key_begin: "00000000".to_string(),
            r_clock_begin: "00000000".to_string(),
        },
        updated_config(),
    )
    .await;
    let materialization_state = harness.run_pending_controller(MATERIALIZATION_NAME).await;

    // Assert the materialization's config is updated.
    let materialization_live_spec = materialization_state.live_spec.unwrap();

    if let AnySpec::Materialization(materialization_def) = materialization_live_spec {
        if let MaterializationEndpoint::Connector(materialization_endpoint) =
            materialization_def.endpoint
        {
            assert!(updated_config() == materialization_endpoint.config.to_value());
        };
    } else {
        panic!("Expected materialization spec");
    }
}

#[tokio::test]
#[serial_test::serial]
async fn test_config_update_publication_failure() {
    // Set up harness & tenant.
    let mut harness = TestHarness::init("test_config_update_publication_failure").await;
    let _user_id = harness.setup_tenant("ducks").await;

    const CAPTURE_NAME: &str = "ducks/capture";
    const MATERIALIZATION_NAME: &str = "ducks/materialization";

    // Draft a catalog including both a capture and a materialization.
    let initial_draft = draft_catalog(serde_json::json!({
        "collections": {
            "ducks/pond/quacks": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                },
                "key": ["/id"]
            }
        },
        "captures": {
            CAPTURE_NAME: {
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": initial_config(),
                    }
                },
                "bindings": [
                    {
                        "resource": {
                            "name": "greetings",
                            "prefix": "Howdy {}!"
                        },
                        "target": "ducks/pond/quacks"
                    }
                ]
            }
        },
        "materializations": {
            MATERIALIZATION_NAME: {
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": initial_config(),
                    }
                },
                "bindings": [ ]
            }
        }
    }));

    // Publish the catalog.
    let result = harness
        .control_plane()
        .publish(
            Some(format!("initial publication")),
            Uuid::new_v4(),
            initial_draft,
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

    // Confirm captures do not republish a new spec with the updated config
    // in response to an insert into the config_updates table.

    // Inject a configUpdate event for the capture.
    let capture_state = harness.get_controller_state(CAPTURE_NAME).await;

    upsert_config_update(
        &mut harness,
        &ShardRef {
            name: CAPTURE_NAME.to_string(),
            build: capture_state.last_build_id,
            key_begin: "00000000".to_string(),
            r_clock_begin: "00000000".to_string(),
        },
        updated_config(),
    )
    .await;

    // Fail the next capture build.
    harness.control_plane().fail_next_build(
        CAPTURE_NAME,
        InjectBuildError::new(
            tables::synthetic_scope("capture", CAPTURE_NAME),
            anyhow::anyhow!("simulated build failure"),
        ),
    );

    let capture_state = harness.run_pending_controller(CAPTURE_NAME).await;

    // Assert the capture's config is not updated.
    let capture_live_spec = capture_state.live_spec.unwrap();

    if let AnySpec::Capture(capture_def) = capture_live_spec {
        if let CaptureEndpoint::Connector(capture_endpoint) = capture_def.endpoint {
            assert!(initial_config() == capture_endpoint.config.to_value());
        };
    } else {
        panic!("Expected capture spec");
    }

    // Confirm materializations do not republish a new spec with the updated config
    // in response to an insert into the config_updates table.

    // Inject a configUpdate event for the materialization.
    let materialization_state = harness.get_controller_state(MATERIALIZATION_NAME).await;

    upsert_config_update(
        &mut harness,
        &ShardRef {
            name: MATERIALIZATION_NAME.to_string(),
            build: materialization_state.last_build_id,
            key_begin: "00000000".to_string(),
            r_clock_begin: "00000000".to_string(),
        },
        updated_config(),
    )
    .await;

    // Fail the next materialization build.
    harness.control_plane().fail_next_build(
        MATERIALIZATION_NAME,
        InjectBuildError::new(
            tables::synthetic_scope("materialization", MATERIALIZATION_NAME),
            anyhow::anyhow!("simulated build failure"),
        ),
    );

    let materialization_state = harness.run_pending_controller(MATERIALIZATION_NAME).await;

    // Assert the materialization's config is not updated.
    let materialization_live_spec = materialization_state.live_spec.unwrap();

    if let AnySpec::Materialization(materialization_def) = materialization_live_spec {
        if let MaterializationEndpoint::Connector(materialization_endpoint) =
            materialization_def.endpoint
        {
            assert!(initial_config() == materialization_endpoint.config.to_value());
        };
    } else {
        panic!("Expected materialization spec");
    }
}

#[tokio::test]
#[serial_test::serial]
async fn test_config_update_publication_backoff() {
    // Set up harness & tenant.
    let mut harness = TestHarness::init("test_config_update_publication_backoff").await;
    let _user_id = harness.setup_tenant("ducks").await;

    const CAPTURE_NAME: &str = "ducks/capture";

    // Draft a catalog.
    let initial_draft = draft_catalog(serde_json::json!({
        "collections": {
            "ducks/pond/quacks": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                },
                "key": ["/id"]
            }
        },
        "captures": {
            CAPTURE_NAME: {
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": initial_config(),
                    }
                },
                "bindings": [
                    {
                        "resource": {
                            "name": "greetings",
                            "prefix": "Howdy {}!"
                        },
                        "target": "ducks/pond/quacks"
                    }
                ]
            }
        },
    }));

    // Publish the catalog.
    let result = harness
        .control_plane()
        .publish(
            Some(format!("initial publication")),
            Uuid::new_v4(),
            initial_draft,
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

    // Inject a configUpdate event for the capture.
    let capture_state = harness.get_controller_state(CAPTURE_NAME).await;

    upsert_config_update(
        &mut harness,
        &ShardRef {
            name: CAPTURE_NAME.to_string(),
            build: capture_state.last_build_id,
            key_begin: "00000000".to_string(),
            r_clock_begin: "00000000".to_string(),
        },
        updated_config(),
    )
    .await;

    // Fail the next capture build.
    harness.control_plane().fail_next_build(
        CAPTURE_NAME,
        InjectBuildError::new(
            tables::synthetic_scope("capture", CAPTURE_NAME),
            anyhow::anyhow!("simulated build failure"),
        ),
    );

    // Run controllers a few times to trigger the initial
    // failure & subsequent backoffs.
    for i in 0..3 {
        let capture_state = harness.run_pending_controller(CAPTURE_NAME).await;
        let last_entry = capture_state
            .current_status
            .publication_status()
            .unwrap()
            .history
            .front()
            .unwrap();
        assert!(!last_entry.is_success());
        assert!(capture_state.error.is_some());
        if i > 0 {
            assert!(capture_state
                .error
                .as_deref()
                .unwrap()
                .contains("backing off config update publication"));
        }
    }

    // Change the timestamp of the last attempted config update publication to simulate
    // the passage of time, and confirm another publication will be attempted on the next
    // controller run.
    let last_attempt = chrono::Utc::now() - chrono::Duration::hours(4);
    harness
        .push_back_last_config_update_pub_history_ts(CAPTURE_NAME, last_attempt)
        .await;

    let capture_state = harness.run_pending_controller(CAPTURE_NAME).await;

    // Assert the capture's config is updated.
    let capture_live_spec = capture_state.live_spec.unwrap();

    if let AnySpec::Capture(capture_def) = capture_live_spec {
        if let CaptureEndpoint::Connector(capture_endpoint) = capture_def.endpoint {
            assert!(updated_config() == capture_endpoint.config.to_value());
        };
    } else {
        panic!("Expected capture spec");
    }
}

// Upserts a config update event into the config_updates table.
async fn upsert_config_update(
    harness: &mut TestHarness,
    shard: &ShardRef,
    updated_config: serde_json::Value,
) {
    let fields = serde_json::from_value(serde_json::json!({
        "eventType": "configUpdate",
        "eventTarget": shard.name.as_str(),
        "config": updated_config,
    }))
    .unwrap();

    let ts = harness.control_plane().current_time();
    let event = serde_json::to_value(models::status::connector::ConfigUpdate {
        shard: shard.clone(),
        ts,
        message: "test config update".to_string(),
        fields,
    })
    .unwrap();

    sqlx::query!(
        r#"
        INSERT INTO config_updates (catalog_name, build, ts, flow_document)
        VALUES ($1::catalog_name, $2::flowid, $3, $4)
        ON CONFLICT (catalog_name)
        DO UPDATE SET
            build = EXCLUDED.build,
            ts = EXCLUDED.ts,
            flow_document = EXCLUDED.flow_document
        "#,
        shard.name.as_str() as &str,
        shard.build as models::Id,
        ts,
        event,
    )
    .execute(&harness.pool)
    .await
    .expect("failed to insert config update");
}
