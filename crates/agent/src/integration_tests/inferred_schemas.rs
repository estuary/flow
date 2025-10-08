use super::harness::{
    TestHarness, draft_catalog, get_collection_generation_id, mock_inferred_schema,
};
use crate::{controllers::ControllerState, integration_tests::harness::InjectBuildError};
use chrono::{DateTime, Utc};
use serde_json::json;

#[tokio::test]
#[serial_test::serial]
async fn test_inferred_schema_updates() {
    let mut harness = TestHarness::init("test_inferred_schema_updates").await;

    // Setup tenant and create initial publication with inferred schema placeholder
    let user_id = harness.setup_tenant("frogs").await;
    let draft = draft_catalog(json!({
        "collections": {
            "frogs/inferred-collection": {
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
                "key": ["/id"],
            }
        },
        "captures": {
            "frogs/inferred_capture": {
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": {
                            "id": "inferred"
                        },
                        "target": "frogs/inferred-collection"
                    }
                ]
            }
        },
        "materializations": {
            "frogs/materialize": {
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "table": "leaves" },
                        "source": "frogs/inferred-collection"
                    },
                ]
            },
        }

    }));

    // Initial publication
    let result = harness
        .user_publication(user_id, "initial publication", draft)
        .await;
    assert!(result.status.is_success());

    harness.run_pending_controllers(None).await;

    // Get the collection's generation ID
    let collection_state = harness
        .get_controller_state("frogs/inferred-collection")
        .await;

    assert_uses_placholder_inferred_schema(&collection_state);
    // Expect the inferred schema status starts out empty
    let schema_status = collection_state
        .current_status
        .unwrap_collection()
        .inferred_schema
        .as_ref()
        .expect("inferred schema status must be present");
    assert!(
        schema_status.schema_last_updated.is_none(),
        "schema_last_updated should start out as None"
    );
    assert!(schema_status.schema_md5.is_none());

    // First inferred schema update
    let generation_id = get_collection_generation_id(&collection_state);
    let schema_v1 = mock_inferred_schema("frogs/inferred-collection", generation_id, 1);
    let schema_v1_md5 = schema_v1.md5.clone();
    harness.upsert_inferred_schema(schema_v1).await;
    let last_update_time = collection_state.controller_updated_at;

    // Run controllers to publish the inferred schema, and expect it to have been added to the model
    let collection_state = harness
        .run_pending_controller("frogs/inferred-collection")
        .await;

    assert_inferred_schema_present_with(&collection_state, generation_id, 0);
    assert_inferred_schema_status(&collection_state, &schema_v1_md5, last_update_time);

    // Second inferred schema update with simulated publication failure
    let schema_v2 = mock_inferred_schema("frogs/inferred-collection", generation_id, 2);
    let schema_v2_md5 = schema_v2.md5.clone();
    harness.upsert_inferred_schema(schema_v2).await;

    // Fail the next publication
    harness.control_plane().fail_next_build(
        "frogs/inferred-collection",
        InjectBuildError::new(
            tables::synthetic_scope("materialization", "frogs/materialize"),
            anyhow::anyhow!("simulated build failure"),
        ),
    );

    // Run controller - should fail
    let collection_state = harness
        .run_pending_controller("frogs/inferred-collection")
        .await;

    assert!(collection_state.error.is_some());
    // Expect the previous version of the inferred schema to still be present.
    assert_inferred_schema_present_with(&collection_state, generation_id, 0);
    // And the status still shows the outdated md5
    assert_inferred_schema_status(&collection_state, &schema_v1_md5, last_update_time);
    let next_run = harness
        .assert_controller_pending("frogs/inferred-collection")
        .await;
    assert_within_minutes(next_run, 3);

    // Simulate multiple publication failures to test exponential backoff
    for attempt in 2..=4 {
        // Fail the next publication again
        harness.control_plane().fail_next_build(
            "frogs/inferred-collection",
            InjectBuildError::new(
                tables::synthetic_scope("materialization", "frogs/materialize"),
                anyhow::anyhow!("simulated build failure"),
            ),
        );

        let collection_state = harness
            .run_pending_controller("frogs/inferred-collection")
            .await;
        assert!(collection_state.error.is_some());
        assert_eq!(collection_state.failures, attempt as i32);

        // Verify retry backoff increases
        let next_run = harness
            .assert_controller_pending("frogs/inferred-collection")
            .await;
        let expect_max_delay_minutes = match attempt {
            2 => 16,
            3 => 205,
            4 => 270,
            _ => unreachable!(),
        };
        assert_within_minutes(next_run, expect_max_delay_minutes);
    }
    let last_update_time = harness
        .get_controller_state("frogs/inferred-collection")
        .await
        .controller_updated_at;

    // Finally, allow publication to succeed
    let collection_state = harness
        .run_pending_controller("frogs/inferred-collection")
        .await;

    assert!(collection_state.error.is_none());
    assert_eq!(collection_state.failures, 0);

    assert_inferred_schema_present_with(&collection_state, generation_id, 1);
    assert_inferred_schema_status(&collection_state, &schema_v2_md5, last_update_time);
}

fn assert_inferred_schema_status(
    state: &ControllerState,
    expect_md5: &str,
    expect_updated_after: DateTime<Utc>,
) {
    let schema_status = state
        .current_status
        .unwrap_collection()
        .inferred_schema
        .as_ref()
        .expect("inferred schema status must be present");
    assert!(
        schema_status
            .schema_last_updated
            .is_some_and(|updated| updated > expect_updated_after),
        "expected inferred schema status to show update after '{expect_updated_after}', but was {:?}",
        schema_status.schema_last_updated
    );
    assert_eq!(Some(expect_md5), schema_status.schema_md5.as_deref());
}

fn assert_inferred_schema_present_with(
    state: &ControllerState,
    generation_id: models::Id,
    max_property_num: usize,
) {
    let actual = get_effective_inferred_schema(state);

    let actual_gen_id_str = actual
        .pointer("/x-collection-generation-id")
        .and_then(|v| v.as_str())
        .expect("inferred schema missing x-collection-generation-id");
    let actual_generation = models::Id::from_hex(actual_gen_id_str)
        .expect("failed to parse x-collection-generation-id");
    assert_eq!(
        generation_id, actual_generation,
        "expected inferred schema to have x-collection-generation-id: '{generation_id}', but was '{actual_gen_id_str}'"
    );
    assert!(
        actual
            .pointer(&format!("/properties/p{max_property_num}"))
            .is_some(),
        "expected schema to contain property 'p{max_property_num}', in: {actual}"
    );
}

fn assert_uses_placholder_inferred_schema(state: &ControllerState) {
    let actual = get_effective_inferred_schema(state);
    if actual
        .pointer("/properties/_meta/properties/inferredSchemaIsNotAvailable")
        .is_none()
    {
        panic!(
            "collection '{}' schema missing placeholder property, in: {}",
            state.catalog_name, actual
        );
    }
}

fn get_effective_inferred_schema(state: &ControllerState) -> serde_json::Value {
    let read_schema = state
        .live_spec
        .as_ref()
        .and_then(|s| s.as_collection())
        .and_then(|c| c.read_schema.as_ref())
        .expect("live spec is not a collection or read schema is None");
    let mut schema_val: serde_json::Value = serde_json::from_str(read_schema.get()).unwrap();
    let Some(schema_val) = schema_val.pointer_mut("/$defs/flow:~1~1inferred-schema") else {
        panic!(
            "inferred schema definition not found in schema: {}",
            read_schema.get()
        );
    };
    schema_val.take()
}

fn assert_within_minutes(wake_at: DateTime<Utc>, within_minutes: i64) {
    let diff = wake_at - Utc::now();
    assert!(
        diff < chrono::Duration::minutes(within_minutes),
        "expected next run to be within {within_minutes} minutes, but was at {wake_at} ({} minutes)",
        diff.num_minutes()
    );
}
