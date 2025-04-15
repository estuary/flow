use super::harness::{draft_catalog, md5_hash, TestHarness};
use crate::publications::JobStatus;
use tables::InferredSchema;

#[tokio::test]
#[serial_test::serial]
async fn test_specs_with_null_bytes() {
    let mut harness = TestHarness::init("specs_with_null_bytes").await;

    let user_id = harness.setup_tenant("possums").await;
    let draft = draft_catalog(serde_json::json!({
        "collections": {
            "possums/bugs": {
                "writeSchema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    },
                    "required": ["id"]
                },
                "readSchema": {
                    "allOf": [
                        {"$ref": "flow://write-schema"},
                        {"$ref": "flow://inferred-schema"}
                    ]
                },
                "key": ["/id"]
            }
        },
        "captures": {
            "possums/capture": {
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "a": "thing" },
                        "target": "possums/bugs"
                    }
                ]
            }
        },
    }));
    let first_pub_result = harness
        .user_publication(user_id, format!("initial publication"), draft)
        .await;
    assert!(
        first_pub_result.status.is_success(),
        "expected success, got {:?}, errors: {:?}",
        first_pub_result.status,
        first_pub_result.errors
    );
    harness.run_pending_controllers(None).await;

    let naughty_draft = draft_catalog(serde_json::json!({
        "collections": {
            "possums/bugs": {
                "writeSchema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "naughty\u{0000}Property": { "type": "string" },
                    },
                    "required": ["id"]
                },
                "readSchema": {
                    "allOf": [
                        {"$ref": "flow://write-schema"},
                        {"$ref": "flow://inferred-schema"}
                    ]
                },
                "key": ["/id"]
            }
        },
    }));

    let result = harness
        .user_publication(user_id, format!("publish spec with nulls"), naughty_draft)
        .await;
    assert_eq!(JobStatus::PublishFailed, result.status);

    insta::assert_debug_snapshot!(result.errors, @r###"
    [
        (
            "",
            "a string in the spec contains a disallowed unicode null escape (\\x00 or \\u0000)",
        ),
    ]
    "###);

    let schema: models::Schema = serde_json::from_value(serde_json::json!({
        "type": "object",
        "properties": {
            "a naughty \u{0000} property": { "type": "integer" }
        },
    }))
    .unwrap();
    let md5 = md5_hash(&schema);
    harness
        .upsert_inferred_schema(InferredSchema {
            collection_name: models::Collection::new("possums/bugs"),
            schema,
            md5,
        })
        .await;

    harness.run_pending_controller("possums/bugs").await;
    let state = harness.get_controller_state("possums/bugs").await;

    // the actual error might have a `(will retry)` suffix
    assert!(state.error.as_ref().expect("error should be set").contains(
        "a string in the spec contains a disallowed unicode null escape (\\x00 or \\u0000)"
    ));

    let history = state
        .current_status
        .unwrap_collection()
        .publications
        .history
        .get(0)
        .expect("missing publication error in history");
    insta::assert_debug_snapshot!(history.errors, @r###"
    [
        Error {
            catalog_name: "",
            scope: None,
            detail: "publish error: a string in the spec contains a disallowed unicode null escape (\\x00 or \\u0000)",
        },
    ]
    "###);
}
