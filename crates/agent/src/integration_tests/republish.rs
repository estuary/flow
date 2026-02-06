use control_plane_api::controllers::Message;
use models::status::AlertType;

use crate::{
    ControlPlane,
    integration_tests::harness::{InjectBuildError, TestHarness, draft_catalog},
};

#[tokio::test]
async fn test_republishing_prefix() {
    let mut harness = TestHarness::init("test_republishing_prefix").await;

    let user_id = harness.setup_tenant("storeCo").await;

    let setup = draft_catalog(serde_json::json!({
        "collections": {
            "storeCo/stuff": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                },
                "key": ["/id"]
            },
            "storeCo/junk": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                },
                "key": ["/id"],
                "derive": {
                    "shards": { "disable": true },
                    "using": {
                        "sqlite": { "migrations": [] }
                    },
                    "transforms": [
                        {
                            "name": "fromStuff",
                            "source": "storeCo/stuff",
                            "lambda": "select $id;",
                            "shuffle": "any"
                        }
                    ]
                }
            }
        },
        "captures": {
            "storeCo/disabled-capture": {
                "shards": { "disable": true },
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "table": "stuff" },
                        "target": "storeCo/stuff"
                    }
                ]
            },
            "storeCo/enabled-capture": {
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "table": "stuff" },
                        "target": "storeCo/stuff"
                    }
                ]
            }
        },
        "materializations": {
            "storeCo/disabled-materialize": {
                "shards": { "disable": true },
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "table": "junk" },
                        "source": "storeCo/junk"
                    },
                    {
                        "resource": { "table": "stuff" },
                        "source": "storeCo/stuff"
                    }
                ]
            },
            "storeCo/enabled-materialize": {
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "table": "junk" },
                        "source": "storeCo/junk"
                    },
                    {
                        "resource": { "table": "stuff" },
                        "source": "storeCo/stuff"
                    }
                ]
            }
        },
        "tests": {
            "storeCo/test": {
                "description": "a test",
                "steps": [
                    {"ingest": {
                        "collection": "storeCo/stuff",
                        "documents": [{"id": "bolts"}]
                    }},
                    {"verify": {
                        "collection": "storeCo/junk",
                        "documents": [{"id": "screws"}]
                    }}
                ]
            }
        }
    }));
    let result = harness
        .user_publication(user_id, "initial publication", setup)
        .await;
    assert!(
        result.status.is_success(),
        "setup publication failed: {:?}",
        result.errors
    );
    harness.run_pending_controllers(None).await;

    // First a happy path test, where we broadcast a republish message and
    // expect controllers to publish in response.
    let mut conn = harness.pool.acquire().await.unwrap();
    let happy_path_republish_reason = "ferda gigs";
    let sent_count = control_plane_api::controllers::broadcast_to_prefix(
        "storeCo/",
        Message::Republish {
            reason: String::from(happy_path_republish_reason),
        },
        &mut conn,
    )
    .await
    .expect("failed to broadcast republish message");
    std::mem::drop(conn);
    assert_eq!(5, sent_count, "expected message to be sent to 5 receivers");

    // Assert that the message was not broadcast to the disabled specs.
    let enqueued = harness
        .get_enqueued_controllers(chrono::TimeDelta::seconds(1))
        .await;
    for catalog_name in ["storeCo/disabled-capture", "storeCo/disabled-materialize"] {
        assert!(
            !enqueued.contains(&catalog_name.to_string()),
            "expected {catalog_name} not to be enqueued, but it was"
        );
    }

    // For each applicable spec, run the controller and expect that it's been published.
    for catalog_name in [
        "storeCo/enabled-capture",
        "storeCo/enabled-materialize",
        "storeCo/stuff",
        "storeCo/junk",
        "storeCo/test",
    ] {
        assert!(
            enqueued.contains(&catalog_name.to_string()),
            "expected: '{catalog_name}' to have been enqueued in: {enqueued:?}"
        );
        test_successful_republish(catalog_name, happy_path_republish_reason, &mut harness).await;
    }

    // Now test a publication failure and retry
    let mut conn = harness.pool.acquire().await.unwrap();
    let republish_reason = "ferda testing of publication failures and retries";
    let sent_count = control_plane_api::controllers::broadcast_to_prefix(
        "storeCo/",
        Message::Republish {
            reason: String::from(republish_reason),
        },
        &mut conn,
    )
    .await
    .expect("failed to broadcast republish message");
    std::mem::drop(conn);
    assert_eq!(5, sent_count, "expected message to be sent to 5 receivers");

    // The capture will have the first publication attempt fail, and we'll expect it to be retried and succeed.
    for catalog_name in [
        "storeCo/enabled-capture",
        "storeCo/enabled-materialize",
        "storeCo/stuff",
        "storeCo/junk",
        "storeCo/test",
    ] {
        test_publication_fails_then_succeeds(catalog_name, republish_reason, &mut harness).await;
    }
}

async fn test_successful_republish(
    catalog_name: &str,
    republish_reason: &str,
    harness: &mut TestHarness,
) {
    tracing::info!(%catalog_name, "will run controller and expect a successful republish");
    let starting_state = harness.get_controller_state(catalog_name).await;
    let after_state = harness.run_pending_controller(catalog_name).await;

    let pub_status = after_state
        .current_status
        .publication_status()
        .expect("missing publication statatus");
    assert!(pub_status.history.len() >= 1);
    assert!(
        pub_status.history[0]
            .detail
            .as_ref()
            .is_some_and(|d| d.contains(republish_reason)),
        "expected detail to contain: '{republish_reason}', got: {:?}",
        &pub_status.history[0]
    );
    assert!(
        starting_state.last_build_id < after_state.last_build_id,
        "expected last_build_id to have increased"
    );
    harness
        .assert_alert_clear(catalog_name, AlertType::BackgroundPublicationFailed)
        .await;

    tracing::info!(%catalog_name, "re-publish success confirmed");
}

async fn test_publication_fails_then_succeeds(
    catalog_name: &str,
    republish_reason: &str,
    harness: &mut TestHarness,
) {
    tracing::info!(%catalog_name, "starting sad path test for spec");

    let starting_state = harness.get_controller_state(catalog_name).await;

    for i in 0..3 {
        if i > 0 {
            let new_ts = harness.control_plane().current_time() - chrono::Duration::minutes(10);
            harness
                .push_back_last_pub_history_ts(catalog_name, new_ts)
                .await;
        }

        let error_scope = tables::synthetic_scope("testing", catalog_name);
        let err_msg = format!("mock publish error, iteration {i}");
        harness.control_plane().fail_next_build(
            catalog_name,
            InjectBuildError::new(error_scope, anyhow::Error::msg(err_msg.clone())),
        );
        let after_state = harness.run_pending_controller(catalog_name).await;

        let pub_status = after_state
            .current_status
            .publication_status()
            .expect("missing publication statatus");
        let last_attempt = &pub_status.history[0];
        assert!(
            !last_attempt.is_success(),
            "expected publication (i={i}) to have failed: {last_attempt:?}"
        );
        assert!(
            last_attempt
                .errors
                .iter()
                .any(|e| e.detail.contains(&err_msg)),
            "expected publish error (i={i}) in: {last_attempt:?}"
        );
        assert!(
            last_attempt
                .detail
                .as_ref()
                .is_some_and(|d| d.contains(republish_reason)),
            "unexpected publication (i={i}) detail: {last_attempt:?}"
        );
        assert_eq!(i + 1, last_attempt.count);
        assert_eq!(starting_state.last_build_id, after_state.last_build_id);
    }
    let fired = harness
        .assert_alert_firing(catalog_name, AlertType::BackgroundPublicationFailed)
        .await;

    test_successful_republish(catalog_name, republish_reason, harness).await;

    harness.assert_alert_resolved(fired.alert.id).await;

    tracing::info!(%catalog_name, "test sad path successful for spec");
}
