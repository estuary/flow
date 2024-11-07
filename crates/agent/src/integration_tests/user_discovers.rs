use crate::{
    integration_tests::harness::{draft_catalog, set_of, TestHarness, UserDiscoverResult},
    ControlPlane,
};
use proto_flow::capture::response::{discovered::Binding, Discovered};

#[tokio::test]
#[serial_test::serial]
async fn test_user_discovers() {
    let mut harness = TestHarness::init("test_user_discovers").await;

    let user_id = harness.setup_tenant("squirrels").await;

    let initial_resp = Discovered {
        bindings: vec![
            Binding {
                recommended_name: String::from("acorns"),
                document_schema_json: document_schema(1),
                resource_config_json: String::from(r#"{"id": "acorns"}"#),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(), // deprecated field
            },
            Binding {
                recommended_name: String::from("walnuts"),
                document_schema_json: document_schema(1),
                resource_config_json: String::from(r#"{"id": "walnuts"}"#),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(), // deprecated field
            },
            Binding {
                recommended_name: String::from("crab apples"),
                document_schema_json: document_schema(1),
                resource_config_json: String::from(r#"{"id": "crab apples"}"#),
                key: vec!["/id".to_string()],
                disable: true,
                resource_path: Vec::new(), // deprecated field
            },
        ],
    };
    // Start with an empty draft
    let draft_id = harness
        .create_draft(user_id, "initial", Default::default())
        .await;

    let endpoint_config = r#"{"tail": "shake"}"#;
    let result = harness
        .user_discover(
            "source/test",
            ":test",
            "squirrels/capture-1",
            draft_id,
            endpoint_config,
            false,
            Box::new(Ok::<Discovered, String>(initial_resp)),
        )
        .await;
    assert!(
        result.job_status.is_success(),
        "expected success, got: {:?}",
        result.job_status
    );
    assert!(result.errors.is_empty());
    assert_eq!(
        3,
        result.draft.collections.len(),
        "expected 3 collections in draft: {:?}",
        result.draft
    );

    insta::assert_debug_snapshot!("initial-discover", result.draft);

    let pub_result = harness
        .create_user_publication(user_id, draft_id, "initial publication")
        .await;

    assert!(pub_result.status.is_success());
    let published_specs = pub_result
        .live_specs
        .into_iter()
        .map(|ls| (ls.catalog_name, ls.spec_type, ls.spec))
        .collect::<Vec<_>>();
    // Expect to see only the two enabled collections. The `crab apples` should have been pruned.
    insta::assert_debug_snapshot!("initial-publication", published_specs);

    // Now discover again, and have it return some different collections so we
    // can test the merge behavior. Start with some changes already in the
    // draft, so we can assert that the merge handles those properly.
    let draft_id = harness
        .create_draft(
            user_id,
            "second discover",
            draft_catalog(serde_json::json!({
                "captures": {
                    "squirrels/capture-1": {
                        "endpoint": {
                            "connector": {
                                "image": "drafted/different/image:tag",
                                "config": { "drafted": {"config": "should be overwritten by discovers endpoint config" }}
                            }
                        },
                        "bindings": [
                            {
                                // This binding is enabled in the draft, and so
                                // should still be enabled in the merged result,
                                // even though the discover response now
                                // indicates disabled.
                                "resource": {
                                    "id": "walnuts",
                                    "expect": "this config should be retained after merge"
                                },
                                "target": "squirrels/walnuts",
                            },
                            {
                                "resource": {
                                    "id": "drafted",
                                    // This behavior may be unexpected since
                                    // we're passing `update_only` on the
                                    // discover, but it is consistent with the
                                    // previous behavior, which is to always
                                    // remove bindings that are not discovered.
                                    // That's because we assume that bindings
                                    // omitted from Discovered _cannot_ be
                                    // captured.
                                    "expect": "binding removed because it is not in discover response"
                                },
                                "target": "squirrels/drafted-collection"
                            }
                        ]
                    }
                },
                "collections": {
                    "squirrels/acorns": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                                "drafted": { "type": "string" }
                            },
                            "required": ["id", "drafted"]
                        },
                        "projections": {
                            "iiiiiideeeee": "/id"
                        },
                        "key": ["/drafted"]
                    },
                    "squirrels/walnuts": {
                        "writeSchema": {
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                                "drafted": { "type": "string" }
                            },
                            "required": ["id", "drafted"]
                        },
                        "readSchema": {
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                                "drafted": { "type": "string" }
                            },
                            "required": ["id", "drafted"]
                        },
                        // This key should be overwritten by the discover
                        "key": ["/drafted"]
                    },
                    "squirrels/extra": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                            },
                            "required": ["id"]
                        },
                        "key": ["/id"]
                    }
                }
            })),
        )
        .await;

    let next_discover = Discovered {
        bindings: vec![
            Binding {
                recommended_name: String::from("acorns"),
                document_schema_json: document_schema(2),
                resource_config_json: String::from(r#"{"id": "acorns"}"#),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(), // deprecated field
            },
            Binding {
                recommended_name: String::from("walnuts"),
                document_schema_json: document_schema(2),
                resource_config_json: String::from(r#"{"id": "walnuts"}"#),
                key: vec!["/id".to_string()],
                disable: true,
                resource_path: Vec::new(), // deprecated field
            },
            Binding {
                recommended_name: String::from("hickory nuts!"),
                document_schema_json: document_schema(2),
                resource_config_json: String::from(r#"{"id": "hickory-nuts"}"#),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(), // deprecated field
            },
        ],
    };
    let endpoint_config = r##"{ "newConfig": "forDiscover" }"##;
    let result = harness
        .user_discover(
            "source/test",
            ":test",
            "squirrels/capture-1",
            draft_id,
            endpoint_config,
            true,
            Box::new(Ok(next_discover.clone())),
        )
        .await;
    assert!(result.job_status.is_success());
    // Expect that the extra collection is still present in the draft, and that
    // the walnuts binding is the only one enabled. Acorns should be disabled
    // because it was removed in the drafted capture spec, and `update_only` was
    // true.
    insta::assert_debug_snapshot!("second-discover", result.draft);

    let pub_result = harness
        .create_user_publication(user_id, draft_id, "initial publication")
        .await;
    assert!(
        pub_result.status.is_success(),
        "pub failed with errors: {:?}",
        pub_result.errors
    );
    // Ensure that the extra collections got pruned during publish.
    let published_specs = pub_result
        .live_specs
        .into_iter()
        .map(|ls| (ls.catalog_name, ls.spec_type))
        .collect::<Vec<_>>();
    // Expect to see only the two enabled collections. The `crab apples` should have been pruned.
    insta::assert_debug_snapshot!(published_specs, @r###"
    [
        (
            "squirrels/acorns",
            Some(
                "collection",
            ),
        ),
        (
            "squirrels/capture-1",
            Some(
                "capture",
            ),
        ),
        (
            "squirrels/walnuts",
            Some(
                "collection",
            ),
        ),
    ]
    "###);

    // Discover returns an identical response, and so nothing should be updated.
    let names = set_of(&[
        "squirrels/capture-1",
        "squirrels/acorns",
        "squirrels/walnuts",
    ]);
    let tables::LiveCatalog {
        captures: starting_captures,
        collections: starting_collections,
        ..
    } = harness
        .control_plane()
        .get_live_specs(names.clone())
        .await
        .unwrap();

    let draft_id = harness
        .create_draft(user_id, "identical discover", Default::default())
        .await;
    let result = harness
        .user_discover(
            "source/test",
            ":test",
            "squirrels/capture-1",
            draft_id,
            endpoint_config,
            true,
            Box::new(Ok(next_discover)),
        )
        .await;
    assert!(result.job_status.is_success());
    assert_eq!(4, result.draft.spec_count());
    let UserDiscoverResult {
        draft:
            tables::DraftCatalog {
                captures: discovered_captures,
                collections: discovered_collections,
                ..
            },
        ..
    } = result;

    let expected_capture = starting_captures.into_iter().next().unwrap().model;
    let actual_capture = discovered_captures.into_iter().next().unwrap();
    assert_eq!(Some(expected_capture), actual_capture.model);

    for live_collection in starting_collections {
        let Some(discovered) = discovered_collections.get_by_key(&live_collection.collection)
        else {
            panic!(
                "missing discovered collection for {}",
                live_collection.collection
            );
        };
        assert_eq!(Some(live_collection.model), discovered.model);
    }
}

fn document_schema(version: usize) -> String {
    serde_json::to_string(&serde_json::json!({
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "nuttiness": { "type": "number", "maximum": version },
        },
        "required": ["id"]
    }))
    .unwrap()
}
