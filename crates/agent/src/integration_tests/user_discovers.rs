use super::{spec_fixture, wrap_connector_schema};
use crate::{
    ControlPlane,
    integration_tests::harness::{TestHarness, UserDiscoverResult, draft_catalog, set_of},
};
use proto_flow::capture::response::{Discovered, discovered::Binding};

#[tokio::test]
async fn test_user_discovers() {
    let mut harness = TestHarness::init("test_user_discovers").await;

    let user_id = harness.setup_tenant("squirrels").await;

    let initial_resp = Discovered {
        bindings: vec![
            Binding {
                recommended_name: String::from("acorns"),
                document_schema_json: document_schema(1),
                resource_config_json: r#"{"id": "acorns"}"#.into(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
            Binding {
                recommended_name: String::from("walnuts"),
                document_schema_json: document_schema(1),
                resource_config_json: r#"{"id": "walnuts"}"#.into(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
            Binding {
                recommended_name: String::from("crab apples"),
                document_schema_json: document_schema(1),
                resource_config_json: r#"{"id": "crab apples"}"#.into(),
                key: vec!["/id".to_string()],
                disable: true,
                resource_path: Vec::new(),
                is_fallback_key: false,
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
            Ok((spec_fixture(), initial_resp)),
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

    pub_result.errors.iter().for_each(|e| {
        println!("Error: {:?}", e);
    });
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
                        "schema": wrap_connector_schema(serde_json::json!({
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                                "drafted": { "type": "string" }
                            },
                            "required": ["id", "drafted"]
                        })),
                        "projections": {
                            "iiiiiideeeee": "/id"
                        },
                        "key": ["/drafted"]
                    },
                    "squirrels/walnuts": {
                        "writeSchema": wrap_connector_schema(serde_json::json!({
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                                "drafted": { "type": "string" }
                            },
                            "required": ["id", "drafted"]
                        })),
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
                        "schema": wrap_connector_schema(serde_json::json!({
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                            },
                            "required": ["id"]
                        })),
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
                resource_config_json: r#"{"id": "acorns"}"#.into(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
            Binding {
                recommended_name: String::from("walnuts"),
                document_schema_json: document_schema(2),
                resource_config_json: r#"{"id": "walnuts"}"#.into(),
                key: vec!["/id".to_string()],
                disable: true,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
            Binding {
                recommended_name: String::from("hickory nuts!"),
                document_schema_json: document_schema(2),
                resource_config_json: r#"{"id": "hickory-nuts"}"#.into(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(),
                is_fallback_key: false,
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
            Ok((spec_fixture(), next_discover.clone())),
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
            Ok((spec_fixture(), next_discover)),
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

fn document_schema(version: usize) -> bytes::Bytes {
    serde_json::to_string(&serde_json::json!({
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "nuttiness": { "type": "number", "maximum": version },
        },
        "required": ["id"]
    }))
    .unwrap()
    .into()
}

#[tokio::test]
async fn test_discover_authorization_denials() {
    let mut harness = TestHarness::init("test_discover_authorization_denials").await;
    // Snapshot staleness is under explicit test control.
    harness.snapshot_auto_refresh = false;

    let user_id = harness.setup_tenant("squirrels").await;

    // The user holds no grant to chipmunks/, and no chipmunks/ specs exist.
    // Denials are computed from the requested names alone, so the two are
    // indistinguishable by design.
    let draft_id = harness
        .create_draft(user_id, "denied discover", Default::default())
        .await;
    let discover_id = harness
        .queue_user_discover(
            "source/test",
            ":test",
            "chipmunks/capture",
            draft_id,
            r#"{"denied": 1}"#,
            false,
            Ok((
                spec_fixture(),
                Discovered {
                    bindings: Vec::new(),
                },
            )),
        )
        .await;

    // Stamp the Snapshot slightly ahead of the queued row, so that it is
    // authoritative for it despite the temporal-skew allowance of
    // `Snapshot::taken_after`. (Touching the row's `updated_at` instead would
    // re-fire the discovers task-creation trigger.)
    let revoke = harness
        .refresh_snapshot_taken_at(tokens::now() + chrono::Duration::seconds(10))
        .await;

    let authoritative = harness.run_queued_discover(discover_id).await;
    assert!(matches!(
        authoritative.job_status,
        crate::discovers::JobStatus::DiscoverFailed
    ));
    assert!(
        !revoke.is_cancelled(),
        "an authoritative denial must not request a Snapshot refresh"
    );

    // Now a denial under a Snapshot which predates the queued discover.
    let revoke = harness
        .refresh_snapshot_taken_at(tokens::now() - chrono::Duration::minutes(5))
        .await;
    let draft_id = harness
        .create_draft(user_id, "denied stale discover", Default::default())
        .await;
    let discover_id = harness
        .queue_user_discover(
            "source/test",
            ":test",
            "chipmunks/capture",
            draft_id,
            r#"{"denied": 2}"#,
            false,
            Ok((
                spec_fixture(),
                Discovered {
                    bindings: Vec::new(),
                },
            )),
        )
        .await;
    let stale = harness.run_queued_discover(discover_id).await;
    assert!(matches!(
        stale.job_status,
        crate::discovers::JobStatus::DiscoverFailed
    ));
    assert!(
        revoke.is_cancelled(),
        "a denial under a stale Snapshot must request an early refresh"
    );

    // The denials must render byte-identically: the response never reveals
    // whether the Snapshot was stale, nor whether the denied specs exist.
    assert_eq!(authoritative.errors, stale.errors);
    insta::assert_debug_snapshot!(authoritative.errors, @r###"
    [
        (
            "flow://capture/chipmunks/capture",
            "not authorized to read: chipmunks/capture",
        ),
    ]
    "###);
}

#[tokio::test]
async fn test_discover_merge_phase_denial() {
    let mut harness = TestHarness::init("test_discover_merge_phase_denial").await;
    harness.snapshot_auto_refresh = false;

    let user_id = harness.setup_tenant("squirrels").await;

    // The drafted capture is readable by the user, but an existing binding
    // targets a collection outside their grants. The binding is retained by
    // the discover merge, so its target is authorized during the merge phase
    // — where a denial must render exactly like a precheck denial.
    let draft_json = serde_json::json!({
        "captures": {
            "squirrels/capture-1": {
                "endpoint": {
                    "connector": { "image": "source/test:test", "config": {} }
                },
                "bindings": [
                    { "resource": { "id": "acorns" }, "target": "chipmunks/stolen" }
                ]
            }
        }
    });
    let draft_id = harness
        .create_draft(user_id, "merge denial", draft_catalog(draft_json.clone()))
        .await;

    let discovered = Discovered {
        bindings: vec![Binding {
            recommended_name: "acorns".to_string(),
            document_schema_json: document_schema(1),
            resource_config_json: r#"{"id": "acorns"}"#.into(),
            key: vec!["/id".to_string()],
            disable: false,
            resource_path: Vec::new(),
            is_fallback_key: false,
        }],
    };
    let discover_id = harness
        .queue_user_discover(
            "source/test",
            ":test",
            "squirrels/capture-1",
            draft_id,
            r#"{}"#,
            false,
            Ok((spec_fixture(), discovered.clone())),
        )
        .await;

    // Stamp the Snapshot ahead of the queued row: this denial is
    // authoritative, and must not request a refresh.
    let revoke = harness
        .refresh_snapshot_taken_at(tokens::now() + chrono::Duration::seconds(10))
        .await;
    let result = harness.run_queued_discover(discover_id).await;

    assert!(matches!(
        result.job_status,
        crate::discovers::JobStatus::DiscoverFailed
    ));
    assert!(!revoke.is_cancelled());

    // Now the same merge-phase denial under a Snapshot which predates the
    // queued discover: it must request an early refresh, and render
    // byte-identically to the authoritative denial.
    let stale_revoke = harness
        .refresh_snapshot_taken_at(tokens::now() - chrono::Duration::minutes(5))
        .await;
    let draft_id = harness
        .create_draft(user_id, "stale merge denial", draft_catalog(draft_json))
        .await;
    let discover_id = harness
        .queue_user_discover(
            "source/test",
            ":test",
            "squirrels/capture-1",
            draft_id,
            r#"{}"#,
            false,
            Ok((spec_fixture(), discovered)),
        )
        .await;
    let stale = harness.run_queued_discover(discover_id).await;

    assert!(matches!(
        stale.job_status,
        crate::discovers::JobStatus::DiscoverFailed
    ));
    assert!(
        stale_revoke.is_cancelled(),
        "a merge-phase denial under a stale Snapshot must request an early refresh"
    );
    assert_eq!(result.errors, stale.errors);

    // Identical scope and message shape as a precheck denial, naming only the
    // collection the user's own draft binding targets.
    insta::assert_debug_snapshot!(result.errors, @r###"
    [
        (
            "flow://capture/squirrels/capture-1",
            "not authorized to read: chipmunks/stolen",
        ),
    ]
    "###);
}
