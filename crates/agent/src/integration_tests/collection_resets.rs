use crate::integration_tests::harness::{
    TestHarness, draft_catalog, get_collection_generation_id, mock_inferred_schema,
};

#[tokio::test]
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

#[tokio::test]
async fn test_task_resets() {
    let mut harness = TestHarness::init("test_task_resets").await;
    let tenant = harness.setup_tenant("test").await;

    // Create a full data pipeline: capture → source collection → derivation → derived collection → materialization
    let catalog = serde_json::json!({
        "captures": {
            "test/capture": {
                "endpoint": {
                    "connector": { "image": "source/test:test", "config": {} }
                },
                "bindings": [
                    { "resource": { "table": "data" }, "target": "test/source" }
                ]
            }
        },
        "collections": {
            "test/source": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "value": { "type": "number" }
                    }
                },
                "key": ["/id"]
            },
            "test/derived": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "doubled": { "type": "number" }
                    }
                },
                "key": ["/id"],
                "derive": {
                    "using": {
                        "sqlite": { "migrations": [] }
                    },
                    "transforms": [
                        {
                            "name": "double",
                            "source": "test/source",
                            "shuffle": "any",
                            "lambda": "select $id, $value * 2 as doubled;"
                        }
                    ]
                }
            }
        },
        "materializations": {
            "test/materialize": {
                "endpoint": {
                    "connector": { "image": "materialize/test:test", "config": {} }
                },
                "bindings": [
                    { "resource": { "table": "derived" }, "source": "test/derived" }
                ]
            }
        }
    });

    let result = harness
        .user_publication(tenant, "initial pipeline", draft_catalog(catalog.clone()))
        .await;
    assert!(
        result.status.is_success(),
        "pub failed: {:?}",
        result.errors
    );

    // Get initial shard template IDs for all three tasks
    let capture = harness.get_controller_state("test/capture").await;
    let proto_flow::AnyBuiltSpec::Capture(capture_spec) = capture.built_spec.as_ref().unwrap()
    else {
        panic!("expected capture spec");
    };
    let initial_capture_shard = capture_spec.shard_template.as_ref().unwrap().id.clone();

    let derivation = harness.get_controller_state("test/derived").await;
    let proto_flow::AnyBuiltSpec::Collection(collection_spec) =
        derivation.built_spec.as_ref().unwrap()
    else {
        panic!("expected collection spec");
    };
    let initial_derivation_shard = collection_spec
        .derivation
        .as_ref()
        .unwrap()
        .shard_template
        .as_ref()
        .unwrap()
        .id
        .clone();

    let materialization = harness.get_controller_state("test/materialize").await;
    let proto_flow::AnyBuiltSpec::Materialization(mat_spec) =
        materialization.built_spec.as_ref().unwrap()
    else {
        panic!("expected materialization spec");
    };
    let initial_mat_shard = mat_spec.shard_template.as_ref().unwrap().id.clone();

    // Reset all three tasks in a single publication by cloning the catalog and injecting reset flags
    let mut reset_catalog = catalog.clone();
    reset_catalog["captures"]["test/capture"]["reset"] = serde_json::json!(true);
    reset_catalog["collections"]["test/derived"]["reset"] = serde_json::json!(true);
    reset_catalog["materializations"]["test/materialize"]["reset"] = serde_json::json!(true);

    let result = harness
        .user_publication(tenant, "reset all tasks", draft_catalog(reset_catalog))
        .await;
    assert!(
        result.status.is_success(),
        "pub failed: {:?}",
        result.errors
    );

    // Verify all three tasks got new shard template IDs
    let capture = harness.get_controller_state("test/capture").await;
    let proto_flow::AnyBuiltSpec::Capture(capture_spec) = capture.built_spec.as_ref().unwrap()
    else {
        panic!("expected capture spec");
    };
    let new_capture_shard = capture_spec.shard_template.as_ref().unwrap().id.clone();
    assert_ne!(
        initial_capture_shard, new_capture_shard,
        "capture shard ID should have changed after reset"
    );

    let derivation = harness.get_controller_state("test/derived").await;
    let proto_flow::AnyBuiltSpec::Collection(collection_spec) =
        derivation.built_spec.as_ref().unwrap()
    else {
        panic!("expected collection spec");
    };
    let new_derivation_shard = collection_spec
        .derivation
        .as_ref()
        .unwrap()
        .shard_template
        .as_ref()
        .unwrap()
        .id
        .clone();
    assert_ne!(
        initial_derivation_shard, new_derivation_shard,
        "derivation shard ID should have changed after reset"
    );

    let materialization = harness.get_controller_state("test/materialize").await;
    let proto_flow::AnyBuiltSpec::Materialization(mat_spec) =
        materialization.built_spec.as_ref().unwrap()
    else {
        panic!("expected materialization spec");
    };
    let new_mat_shard = mat_spec.shard_template.as_ref().unwrap().id.clone();
    assert_ne!(
        initial_mat_shard, new_mat_shard,
        "materialization shard ID should have changed after reset"
    );

    // Verify publication details mention the resets
    let capture_specs = harness.get_publication_specs("test/capture").await;
    assert_eq!(2, capture_specs.len());
    assert!(
        capture_specs[1]
            .detail
            .contains("reset capture to new generation"),
        "unexpected detail: {}",
        capture_specs[1].detail
    );

    let derivation_specs = harness.get_publication_specs("test/derived").await;
    assert_eq!(2, derivation_specs.len());
    assert!(
        derivation_specs[1]
            .detail
            .contains("reset collection to new generation"),
        "unexpected detail: {}",
        derivation_specs[1].detail
    );

    let mat_specs = harness.get_publication_specs("test/materialize").await;
    assert_eq!(2, mat_specs.len());
    assert!(
        mat_specs[1]
            .detail
            .contains("reset materialization to new generation"),
        "unexpected detail: {}",
        mat_specs[1].detail
    );
}
