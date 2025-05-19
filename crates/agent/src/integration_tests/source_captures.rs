use crate::{integration_tests::harness::InjectBuildError, ControlPlane};

use super::harness::{draft_catalog, TestHarness};
use models::Id;
use uuid::Uuid;

#[tokio::test]
#[serial_test::serial]
async fn test_source_captures() {
    let mut harness = TestHarness::init("test_source_captures").await;

    let user_id = harness.setup_tenant("ducks").await;

    let draft = draft_catalog(serde_json::json!({
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
            "ducks/capture": {
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": {
                            "name": "greetings",
                            "prefix": "Hello {}!"
                        },
                        "target": "ducks/pond/quacks"
                    }
                ]
            }
        },
        "materializations": {
            "ducks/materializeA": {
                "sourceCapture": "ducks/capture",
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": {}
                    }
                },
                "bindings": [ ]
            },
            "ducks/materializeNoSource": {
                "sourceCapture": "ducks/notARealCapture",
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": {}
                    }
                },
                "bindings": [ ]
            }
        }
    }));

    let result = harness
        .user_publication(user_id, "test sourceCapture", draft)
        .await;
    assert!(result.status.is_success());

    let scope = tables::synthetic_scope("materialization", "ducks/materializeA");
    harness.control_plane().fail_next_build(
        "ducks/materializeA",
        InjectBuildError::new(scope.clone(), anyhow::anyhow!("simulated build error")),
    );

    // Run the rest of the controllers and expect that sourceCaptures were updated as expected
    harness.run_pending_controllers(None).await;

    // Run A again and expect to see an error about backing off after the publication failed
    let a_state = harness.run_pending_controller("ducks/materializeA").await;
    let error = a_state
        .error
        .as_deref()
        .expect("expected controller error, got None");
    assert_eq!(
        error,
        "backing off adding binding(s) to match the sourceCapture: [ducks/pond/quacks] after 1 failure (will retry)"
    );
    // Ensure that the error was recorded
    let a_pub = &a_state.current_status.publication_status().unwrap().history[0];
    assert!(!a_pub.is_success());
    assert!(a_pub
        .detail
        .as_deref()
        .is_some_and(|d| d.starts_with("adding ")));

    let new_last_pub = harness.control_plane().current_time() - chrono::Duration::minutes(4);
    // Simulate the passage of time to allow the materialization to re-try adding the bindings
    harness
        .push_back_last_pub_history_ts("ducks/materializeA", new_last_pub)
        .await;

    // A should now retry and successfully add the bindings.
    let a_state = harness.run_pending_controller("ducks/materializeA").await;
    assert!(
        a_state.error.is_none(),
        "expected no error, got: {:?}",
        a_state.error
    );
    let a_model = a_state
        .live_spec
        .as_ref()
        .unwrap()
        .as_materialization()
        .unwrap();
    assert_eq!(1, a_model.bindings.len());
    assert_eq!(
        "ducks/pond/quacks",
        a_model.bindings[0].source.collection().as_str()
    );
    // Schema mode not set, so we expect schema to be left empty
    assert_eq!(
        None,
        a_model.bindings[0].resource.to_value().pointer("/schema")
    );
    // Delta updates not set, so we expect delta to be left empty
    assert_eq!(
        None,
        a_model.bindings[0].resource.to_value().pointer("/delta")
    );
    assert_eq!(
        "pond_quacks",
        a_model.bindings[0]
            .resource
            .to_value()
            .pointer("/id")
            .unwrap()
            .as_str()
            .unwrap()
    );
    let a_status = a_state.current_status.unwrap_materialization();
    assert!(a_status.source_capture.as_ref().unwrap().up_to_date);
    assert!(a_status
        .source_capture
        .as_ref()
        .unwrap()
        .add_bindings
        .is_empty());
    assert_eq!(
        Some("adding binding(s) to match the sourceCapture: [ducks/pond/quacks]\nupdated resource /_meta of 1 bindings"),
        a_status.publications.history[0].detail.as_deref()
    );

    let no_source_state = harness
        .get_controller_state("ducks/materializeNoSource")
        .await;
    let no_source_model = no_source_state
        .live_spec
        .as_ref()
        .unwrap()
        .as_materialization()
        .unwrap();
    assert!(no_source_model.bindings.is_empty());
    assert!(no_source_model.source_capture.is_none());
    let no_source_status = no_source_state.current_status.unwrap_materialization();
    assert_eq!(
        Some("in response to deletion one or more depencencies, removed sourceCapture: \"ducks/notARealCapture\" because the capture was deleted"),
        no_source_status.publications.history[0].detail.as_deref()
    );
    assert!(no_source_status.source_capture.is_none());
}

#[tokio::test]
#[serial_test::serial]
async fn test_source_captures_collection_name() {
    let mut harness = TestHarness::init("test_source_captures_collection_name").await;

    let user_id = harness.setup_tenant("ducks").await;

    let draft = draft_catalog(serde_json::json!({
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
            "ducks/capture": {
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": {
                            "name": "greetings",
                            "prefix": "Hello {}!"
                        },
                        "target": "ducks/pond/quacks"
                    }
                ]
            }
        },
        "materializations": {
            "ducks/materializeA": {
                "sourceCapture": {
                    "capture": "ducks/capture",
                    "targetSchema": "fromSourceName",
                    "deltaUpdates": true,
                },
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": {}
                    }
                },
                "bindings": [ ]
            },
            "ducks/materializeNoSource": {
                "sourceCapture": "ducks/notARealCapture",
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": {}
                    }
                },
                "bindings": [ ]
            }
        }
    }));

    let result = harness
        .user_publication(user_id, "test sourceCapture", draft)
        .await;
    assert!(result.status.is_success());

    harness.run_pending_controllers(None).await;
    let a_state = harness.get_controller_state("ducks/materializeA").await;
    let a_model = a_state
        .live_spec
        .as_ref()
        .unwrap()
        .as_materialization()
        .unwrap();
    assert_eq!(1, a_model.bindings.len());
    assert_eq!(
        "ducks/pond/quacks",
        a_model.bindings[0].source.collection().as_str()
    );
    assert_eq!(
        "pond",
        a_model.bindings[0]
            .resource
            .to_value()
            .pointer("/schema")
            .unwrap()
            .as_str()
            .unwrap()
    );
    assert_eq!(
        true,
        a_model.bindings[0]
            .resource
            .to_value()
            .pointer("/delta")
            .unwrap()
            .as_bool()
            .unwrap()
    );
    assert_eq!(
        "quacks",
        a_model.bindings[0]
            .resource
            .to_value()
            .pointer("/id")
            .unwrap()
            .as_str()
            .unwrap()
    );
    let a_status = a_state.current_status.unwrap_materialization();
    assert!(a_status.source_capture.as_ref().unwrap().up_to_date);
    assert!(a_status
        .source_capture
        .as_ref()
        .unwrap()
        .add_bindings
        .is_empty());
    assert_eq!(
        Some("adding binding(s) to match the sourceCapture: [ducks/pond/quacks]\nupdated resource /_meta of 1 bindings"),
        a_status.publications.history[0].detail.as_deref()
    );

    let no_source_state = harness
        .get_controller_state("ducks/materializeNoSource")
        .await;
    let no_source_model = no_source_state
        .live_spec
        .as_ref()
        .unwrap()
        .as_materialization()
        .unwrap();
    assert!(no_source_model.bindings.is_empty());
    assert!(no_source_model.source_capture.is_none());
    let no_source_status = no_source_state.current_status.unwrap_materialization();
    assert_eq!(
        Some("in response to deletion one or more depencencies, removed sourceCapture: \"ducks/notARealCapture\" because the capture was deleted"),
        no_source_status.publications.history[0].detail.as_deref()
    );
    assert!(no_source_status.source_capture.is_none());
}

#[tokio::test]
#[serial_test::serial]
async fn test_source_capture_no_annotations() {
    let harness = TestHarness::init("test_source_capture_no_annotations").await;
    let user_id = harness.setup_tenant("sheep").await;

    let draft = draft_catalog(serde_json::json!({
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
            "ducks/capture": {
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": {
                            "name": "greetings",
                            "prefix": "Hello {}!"
                        },
                        "target": "ducks/pond/quacks"
                    }
                ]
            }
        },
        "materializations": {
            "ducks/materializeA": {
                "sourceCapture": {
                    "capture": "ducks/capture",
                    "targetSchema": "fromSourceName",
                    "deltaUpdates": true,
                },
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test-no-annotation",
                        "config": {}
                    }
                },
                "bindings": [ ]
            }
        }
    }));
    let pub_id = Id::new([0, 0, 0, 0, 0, 0, 0, 9]);
    let built = harness
        .publisher
        .build(
            user_id,
            pub_id,
            None,
            draft,
            Uuid::new_v4(),
            "ops/dp/public/test",
            false,
            0,
        )
        .await
        .expect("build failed");
    assert!(built.has_errors());

    let errors = built.errors().collect::<Vec<_>>();

    insta::assert_debug_snapshot!(errors, @r###"
    [
        Error {
            scope: flow://materialization/ducks/materializeA,
            error: sourceCapture.deltaUpdates set but the connector 'materialize/test' does not support delta updates,
        },
        Error {
            scope: flow://materialization/ducks/materializeA,
            error: sourceCapture.targetSchema set but the connector 'materialize/test' does not support resource schemas,
        },
    ]
    "###);
}
