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
async fn test_discover_not_authorized_capture() {
    let mut harness = TestHarness::init("test_discover_not_authorized_capture").await;

    let user_id = harness.setup_tenant("squirrels").await;

    // A live capture exists at one of the unauthorized names, and not at the
    // other. The SpecEdit precheck is a pure function of the grant graph and
    // the requested name, so both cases must produce identical outcomes:
    // a discover's failure may never disclose whether a spec exists in
    // another tenant's catalog.
    sqlx::query(
        r#"
        with p1 as (
            insert into live_specs (id, catalog_name, spec_type, controller_task_id, spec, built_spec) values
            ('1111000011110000'::flowid, 'chipmunks/capture-existing', 'capture', '2222000022220000'::flowid, '{
                "bindings": [ ],
                "endpoint": { "connector": { "config": {}, "image": "source/test:test" } }
            }', '{}')
        )
        insert into internal.tasks (task_id, task_type) values ('2222000022220000'::flowid, 2)
        on conflict do nothing;
        "#,
    )
    .execute(&harness.pool)
    .await
    .unwrap();

    for (case, capture_name) in [
        ("missing live spec", "chipmunks/capture-missing"),
        ("existing live spec", "chipmunks/capture-existing"),
    ] {
        let draft_id = harness
            .create_draft(user_id, case, Default::default())
            .await;
        let discover_id = harness
            .queue_user_discover(
                "source/test",
                ":test",
                capture_name,
                draft_id,
                r#"{"filtered": 1}"#,
                false,
                Ok((
                    spec_fixture(),
                    Discovered {
                        bindings: Vec::new(),
                    },
                )),
            )
            .await;
        let result = harness.run_queued_discover(discover_id).await;

        assert!(
            matches!(
                result.job_status,
                models::discovers::JobStatus::NotAuthorized
            ),
            "{case}: expected NotAuthorized, got: {:?}",
            result.job_status
        );
        // A single, grant-based draft error: it speaks only to the user's
        // grants, never to whether a spec exists at the name.
        let expect_errors = vec![(
            format!("flow://capture/{capture_name}"),
            format!(
                "user is not authorized to edit specs under '{capture_name}'; if this access was granted recently, please retry in a moment"
            ),
        )];
        assert_eq!(expect_errors, result.errors, "{case}");
        assert_eq!(0, result.draft.spec_count(), "{case}");

        // A NotAuthorized outcome requests an early background Snapshot
        // refresh, so a grant committed after the Snapshot was taken is
        // visible to a manual retry. Each poll pins a freshly taken Snapshot,
        // so cancellation is attributable to this case alone.
        let snapshot = harness.snapshot_watch.token();
        assert!(
            snapshot.result().unwrap().revoke.is_cancelled(),
            "{case}: expected the NotAuthorized outcome to cancel the Snapshot's revoke token"
        );
    }
}

#[tokio::test]
async fn test_discover_capture_requires_spec_edit() {
    let mut harness = TestHarness::init("test_discover_capture_requires_spec_edit").await;

    // Provision the tenant (and its data-plane read grant), then a separate
    // user whose only authorization is a direct grant to `squirrels/`.
    let _admin_user = harness.setup_tenant("squirrels").await;
    let limited_user = uuid::Uuid::new_v4();
    sqlx::query(r#"insert into auth.users (id, email) values ($1, 'limited@squirrels.test')"#)
        .bind(limited_user)
        .execute(&harness.pool)
        .await
        .unwrap();

    // Only grants conveying SpecEdit may discover: legacy `read` (Viewer) and
    // `write` (Writer) hold CatalogRead but not SpecEdit, while `admin` does.
    for (capability, expect_authorized) in [
        (models::Capability::Read, false),
        (models::Capability::Write, false),
        (models::Capability::Admin, true),
    ] {
        harness
            .add_user_grant(limited_user, "squirrels/", capability)
            .await;
        let draft_id = harness
            .create_draft(limited_user, format!("{capability:?}"), Default::default())
            .await;
        let discover_id = harness
            .queue_user_discover(
                "source/test",
                ":test",
                "squirrels/capture-1",
                draft_id,
                r#"{}"#,
                false,
                Ok((
                    spec_fixture(),
                    Discovered {
                        bindings: Vec::new(),
                    },
                )),
            )
            .await;
        let result = harness.run_queued_discover(discover_id).await;

        if expect_authorized {
            assert!(
                result.job_status.is_success(),
                "{capability:?}: expected success, got: {:?}",
                result.job_status
            );
        } else {
            assert!(
                matches!(
                    result.job_status,
                    models::discovers::JobStatus::NotAuthorized
                ),
                "{capability:?}: expected NotAuthorized, got: {:?}",
                result.job_status
            );
        }
    }
}

#[tokio::test]
async fn test_discover_not_authorized_wins_over_missing_plane() {
    let mut harness =
        TestHarness::init("test_discover_not_authorized_wins_over_missing_plane").await;

    let user_id = harness.setup_tenant("squirrels").await;

    // The user is unauthorized to the capture AND names a nonexistent data
    // plane: the SpecEdit precheck runs first, so NotAuthorized wins.
    let draft_id = harness
        .create_draft(user_id, "ordering", Default::default())
        .await;
    let discover_id = harness
        .queue_user_discover_in_plane(
            "source/test",
            ":test",
            "chipmunks/capture",
            "ops/dp/public/missing",
            draft_id,
            r#"{}"#,
            false,
            Ok((
                spec_fixture(),
                Discovered {
                    bindings: Vec::new(),
                },
            )),
        )
        .await;
    let result = harness.run_queued_discover(discover_id).await;

    assert!(
        matches!(
            result.job_status,
            models::discovers::JobStatus::NotAuthorized
        ),
        "expected NotAuthorized, got: {:?}",
        result.job_status
    );
}

#[tokio::test]
async fn test_discover_no_data_plane() {
    let mut harness = TestHarness::init("test_discover_no_data_plane").await;
    let user_id = harness.setup_tenant("squirrels").await;

    // A plane outside the tenant's `ops/dp/public/` read grant: it exists but
    // is not readable.
    harness
        .add_data_plane(
            "ops/dp/private/other",
            "ops-dp-private-other.dp.test",
            vec!["c2VjcmV0".to_string()],
        )
        .await;
    // A readable plane with no HMAC keys at all is excluded from the
    // authorization Snapshot by construction, and cannot sign anything a
    // discover would need. It is treated the same as a missing plane.
    harness
        .add_data_plane(
            "ops/dp/public/keyless",
            "ops-dp-public-keyless.dp.test",
            Vec::new(),
        )
        .await;

    for (case, data_plane_name) in [
        ("unauthorized plane", "ops/dp/private/other"),
        // The name falls under the tenant's read grant, but no such plane exists.
        ("missing plane", "ops/dp/public/missing"),
        ("keyless plane", "ops/dp/public/keyless"),
    ] {
        let draft_id = harness
            .create_draft(user_id, case, Default::default())
            .await;
        let discover_id = harness
            .queue_user_discover_in_plane(
                "source/test",
                ":test",
                "squirrels/capture-1",
                data_plane_name,
                draft_id,
                r#"{}"#,
                false,
                Ok((
                    spec_fixture(),
                    Discovered {
                        bindings: Vec::new(),
                    },
                )),
            )
            .await;
        let result = harness.run_queued_discover(discover_id).await;

        assert!(
            matches!(result.job_status, models::discovers::JobStatus::NoDataPlane),
            "{case}: expected NoDataPlane, got: {:?}",
            result.job_status
        );

        // A NoDataPlane outcome requests an early background Snapshot refresh
        // (see the rationale in `crate::discovers`). Each poll pins a freshly
        // taken Snapshot, so cancellation is attributable to this case alone.
        let snapshot = harness.snapshot_watch.token();
        assert!(
            snapshot.result().unwrap().revoke.is_cancelled(),
            "{case}: expected the NoDataPlane outcome to cancel the Snapshot's revoke token"
        );
    }
}

#[tokio::test]
async fn test_discover_merge_filters_unauthorized_collection() {
    let mut harness =
        TestHarness::init("test_discover_merge_filters_unauthorized_collection").await;

    let user_id = harness.setup_tenant("squirrels").await;

    // The user holds SpecEdit to the drafted capture, but an existing binding
    // targets a collection outside their grants. The binding is retained by
    // the discover merge; its target is silently filtered from the
    // merge-phase fetch, so the collection is drafted fresh rather than
    // merged with any live spec the user can't read.
    let draft_id = harness
        .create_draft(
            user_id,
            "merge filter",
            draft_catalog(serde_json::json!({
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
            })),
        )
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
            Ok((spec_fixture(), discovered)),
        )
        .await;
    let result = harness.run_queued_discover(discover_id).await;

    assert!(
        result.job_status.is_success(),
        "expected success, got: {:?}",
        result.job_status
    );
    assert!(result.errors.is_empty());
    // The filtered target is drafted as a new collection.
    assert!(
        result
            .draft
            .collections
            .get_by_key(&models::Collection::new("chipmunks/stolen"))
            .is_some(),
        "expected chipmunks/stolen to be drafted fresh: {:?}",
        result.draft
    );
}
