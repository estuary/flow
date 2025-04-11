use models::{CatalogType, Id};
use proto_flow::AnyBuiltSpec;
use uuid::Uuid;

use crate::{
    integration_tests::harness::{
        draft_catalog, mock_inferred_schema, InjectBuildError, TestHarness,
    },
    publications::{DefaultRetryPolicy, JobStatus, LockFailure, NoopWithCommit, RetryPolicy},
    ControlPlane,
};

#[tokio::test]
#[serial_test::serial]
async fn test_publication_spec_updates() {
    let mut harness = TestHarness::init("test_publication_spec_updates").await;

    let user_id = harness.setup_tenant("caterpillars").await;
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
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
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
        .user_publication(user_id, "initial publication", draft)
        .await;
    assert!(result.status.is_success());

    let materialization_state = harness
        .get_controller_state("caterpillars/materialize")
        .await;
    let AnyBuiltSpec::Materialization(spec) = materialization_state.built_spec.as_ref().unwrap()
    else {
        panic!("expected a materialization");
    };
    assert_eq!(1, spec.bindings.len());

    harness.run_pending_controllers(None).await;

    let reset_draft = draft_catalog(serde_json::json!({
        "collections": {
            "caterpillars/leaves": {
                "reset": true,
                "writeSchema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
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
    }));
    let result = harness
        .user_publication(user_id, "reset collection", reset_draft)
        .await;
    assert!(result.status.is_success());

    // Verify that the collection reset was recorded in the history
    let collection_history = harness.get_publication_specs("caterpillars/leaves").await;
    assert_eq!(2, collection_history.len());
    assert!(collection_history[1].detail.contains("reset"));

    tracing::warn!("sleeping");
    tokio::time::sleep(std::time::Duration::from_secs(6000)).await;

    // This passes, but shouldn't. The materialiization _should_ have been updated
    let pub_specs = harness
        .get_publication_specs("caterpillars/materialize")
        .await;
    assert_eq!(
        1,
        pub_specs.len(),
        "materialization was only touched, so not updated"
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
    assert_eq!(0, binding.backfill);

    harness.run_pending_controllers(None).await;
    let new_materialization_state = harness
        .get_controller_state("caterpillars/materialize")
        .await;
    let new_binding = &new_materialization_state
        .live_spec
        .as_ref()
        .unwrap()
        .as_materialization()
        .unwrap()
        .bindings[0];
    assert_eq!(1, new_binding.backfill);

    let pub_specs = harness
        .get_publication_specs("caterpillars/materialize")
        .await;
    assert_eq!(
        2,
        pub_specs.len(),
        "materialization should have a publication spec"
    );
    assert!(
        pub_specs[2].detail.contains("backfill"),
        "unexpected detail: '{}',\nspec: {}",
        &pub_specs[2].detail,
        pub_specs[2].spec.0.get()
    );
}
