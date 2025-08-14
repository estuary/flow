use crate::integration_tests::harness::{
    draft_catalog, get_collection_generation_id, mock_inferred_schema, TestHarness,
};

#[tokio::test]
#[serial_test::serial]
async fn test_publication_spec_updates() {
    let mut harness = TestHarness::init("test_publication_spec_updates").await;

    // We have two tenants, caterpillars and moths. Caterpillars have a whole end-to-end
    // data flow. Moths are allowed to read caterpillars' data, and have several different
    // materializations of the leaves collection.
    let caterpillar = harness.setup_tenant("caterpillars").await;
    let moth = harness.setup_tenant("moths").await;
    harness
        .add_role_grant("moths/", "caterpillars/", models::Capability::Read)
        .await;

    let draft = draft_catalog(serde_json::json!({
        "captures": {
            "caterpillars/capture": {
                "endpoint": {
                    "connector": { "image": "source/test:test", "config": {} }
                },
                "bindings": [
                    { "resource": { "table": "leaves" }, "target": "caterpillars/leaves" }
                ]
            }
        },
        "collections": {
            "caterpillars/leaves": {
                "writeSchema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    },
                },
                "readSchema": {
                    "allOf": [
                        {"$ref": "flow://write-schema"},
                        {"$ref": "flow://inferred-schema"}
                    ]
                },
                "key": ["/id"]
            },
        },
        "materializations": {
            "caterpillars/materialize": {
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "table": "leaves" },
                        "source": "caterpillars/leaves"
                    },
                ]
            },
        }
    }));

    let result = harness
        .user_publication(caterpillar, "caterpillars initial publication", draft)
        .await;
    assert!(result.status.is_success());

    let draft = draft_catalog(serde_json::json!({
        "materializations": {
            "moths/abort": {
                "onIncompatibleSchemaChange": "abort",
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "table": "leaves" },
                        "source": "caterpillars/leaves"
                    },
                ]
            },
            "moths/disableBinding": {
                "onIncompatibleSchemaChange": "disableBinding",
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "table": "leaves" },
                        "source": "caterpillars/leaves"
                    },
                ]
            },
            "moths/alreadyDisabled": {
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "table": "leaves" },
                        "source": "caterpillars/leaves",
                        "disable": true
                    },
                ]
            },
        }
    }));
    let result = harness
        .user_publication(moth, "moths initial publication", draft)
        .await;
    assert!(
        result.status.is_success(),
        "pub failed: {:?}",
        result.errors
    );

    // Allow controllers to activate everything
    harness.run_pending_controllers(None).await;

    // Update the inferred schema, and allow controllers to respond
    let leaves = harness.get_controller_state("caterpillars/leaves").await;
    let starting_generation_id = get_collection_generation_id(&leaves);
    harness
        .upsert_inferred_schema(mock_inferred_schema(
            "caterpillars/leaves",
            starting_generation_id,
            2,
        ))
        .await;
    harness.run_pending_controllers(None).await;

    // Publish a reset of the collection, with a new key. We'll expect that the
    // caterpillars materialization is updated as part of the same publication,
    // and that the moths materializations are updated asynchronously.
    let reset_draft = draft_catalog(serde_json::json!({
        "collections": {
            "caterpillars/leaves": {
                "reset": true,
                "writeSchema": {
                    "type": "object",
                    "properties": {
                        "newId": { "type": "string" }
                    }
                },
                "readSchema": {
                    "allOf": [
                        {"$ref": "flow://write-schema"},
                        {"$ref": "flow://inferred-schema"}
                    ]
                },
                "key": ["/newId"]
            },
        },
    }));
    let result = harness
        .user_publication(caterpillar, "reset collection", reset_draft)
        .await;
    assert!(
        result.status.is_success(),
        "pub failed: {:?}",
        result.errors
    );

    // The caterpillars materialiization should have been updated by the reset publication
    let pub_specs = harness
        .get_publication_specs("caterpillars/materialize")
        .await;
    assert_eq!(2, pub_specs.len());
    let detail = &pub_specs[1].detail;
    assert!(
        detail.contains("backfilled binding of reset collection caterpillars/leaves"),
        "unexpected detail: {}",
        detail
    );
    let materialization = harness
        .get_controller_state("caterpillars/materialize")
        .await;
    let binding = &materialization
        .live_spec
        .as_ref()
        .unwrap()
        .as_materialization()
        .unwrap()
        .bindings[0];
    assert_eq!(1, binding.backfill);

    // Assert that the moths materializations have not been updated yet.
    for materialization in [
        "moths/abort",
        "moths/disableBinding",
        "moths/alreadyDisabled",
    ] {
        let pub_specs = harness.get_publication_specs(materialization).await;
        assert_eq!(1, pub_specs.len());
    }

    // Run controllers and expect the moths materializations to be updated.
    harness.run_pending_controllers(None).await;
    let pub_specs = harness.get_publication_specs("moths/disableBinding").await;
    assert_eq!(2, pub_specs.len());
    let state = harness.get_controller_state("moths/disableBinding").await;
    let model = state
        .live_spec
        .as_ref()
        .and_then(|ls| ls.as_materialization())
        .unwrap();
    assert!(model.bindings[0].disable);

    let pub_specs = harness.get_publication_specs("moths/abort").await;
    assert_eq!(
        1,
        pub_specs.len(),
        "expect 1 spec in history, since the publication should have failed"
    );
    let state = harness.get_controller_state("moths/abort").await;
    let error = state.error.as_ref().unwrap();
    assert!(
        error.contains("publication failed"),
        "unexpected error: {}",
        error
    );

    let pub_errors = &state
        .current_status
        .unwrap_materialization()
        .publications
        .history[0]
        .errors;
    assert_eq!(1, pub_errors.len());
    assert!(
        pub_errors[0].detail.contains("raising an error because moths/abort specifies `onIncompatibleSchemaChange: abort`: this binding must backfill because its source collection caterpillars/leaves was reset"),
        "unexpected error: {}",
        pub_errors[0].detail
    );

    let state = harness.get_controller_state("moths/alreadyDisabled").await;
    assert!(state.error.is_none());
    let pub_specs = harness.get_publication_specs("moths/alreadyDisabled").await;
    assert_eq!(1, pub_specs.len());

    // Verify that the collection publication history reflects the updating and
    // subsequent resetting of the inferred schema.
    let collection_history = harness.get_publication_specs("caterpillars/leaves").await;
    assert_eq!(3, collection_history.len());
    assert!(
        collection_history[0]
            .detail
            .contains("applied inferred schema placeholder"),
        "unexpected detail: {}",
        collection_history[0].detail
    );
    assert!(
        collection_history[1]
            .detail
            .contains("updating inferred schema"),
        "unexpected detail: {}",
        collection_history[1].detail
    );
    assert!(
        !collection_history[1]
            .spec
            .get()
            .contains("inferredSchemaIsNotAvailable"),
        "expected inferred schema to be updated, got: {}",
        collection_history[1].spec.get()
    );
    assert!(
        collection_history[2]
            .detail
            .contains("applied inferred schema placeholder (inferred schema is stale)"),
        "unexpected detail: {}",
        collection_history[2].detail
    );
    assert!(
        collection_history[2]
            .spec
            .get()
            .contains("inferredSchemaIsNotAvailable"),
        "unexpected inferred schema placeholder, got: {}",
        collection_history[2].spec.get()
    );
}
