use super::harness::{draft_catalog, TestHarness};

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
        "quacks",
        a_model.bindings[0].resource.to_value().pointer("/id").unwrap().as_str().unwrap()
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
        Some("adding binding(s) to match the sourceCapture: [ducks/pond/quacks]"),
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
        Some("in response to publication of one or more depencencies, removed sourceCapture: \"ducks/notARealCapture\" because the capture was deleted"),
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
        a_model.bindings[0].resource.to_value().pointer("/schema").unwrap().as_str().unwrap()
    );
    assert_eq!(
        true,
        a_model.bindings[0].resource.to_value().pointer("/delta").unwrap().as_bool().unwrap()
    );
    assert_eq!(
        "quacks",
        a_model.bindings[0].resource.to_value().pointer("/id").unwrap().as_str().unwrap()
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
        Some("adding binding(s) to match the sourceCapture: [ducks/pond/quacks]"),
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
        Some("in response to publication of one or more depencencies, removed sourceCapture: \"ducks/notARealCapture\" because the capture was deleted"),
        no_source_status.publications.history[0].detail.as_deref()
    );
    assert!(no_source_status.source_capture.is_none());
}
