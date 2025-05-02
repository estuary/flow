use crate::integration_tests::harness::{draft_catalog, TestHarness};

#[tokio::test]
#[serial_test::serial]
async fn test_quota_single_task() {
    let mut harness = TestHarness::init("test_quota_single_task").await;

    let user_id = harness.setup_tenant("seaTurtles").await;

    sqlx::query(
        "update tenants set tasks_quota = 2, collections_quota = 3 where tenant = 'seaTurtles/';",
    )
    .execute(&harness.pool)
    .await
    .unwrap();

    let draft = draft_catalog(serde_json::json!({
        "captures": {
            "seaTurtles/CaptureA": minimal_capture(false, &["seaTurtles/CollectionA"]),
            "seaTurtles/CaptureB": minimal_capture(false, &["seaTurtles/CollectionB", "seaTurtles/CollectionC"]),
            "seaTurtles/CaptureDisabled": minimal_capture(true, &[]),
        },
        "collections": {
            "seaTurtles/CollectionA": minimal_collection(),
            "seaTurtles/CollectionB": minimal_collection(),
            "seaTurtles/CollectionC": minimal_collection(),
        }
    }));
    let result = harness
        .user_publication(user_id, "setup quota single task", draft)
        .await;
    assert!(result.status.is_success());

    let draft = draft_catalog(serde_json::json!({
        "captures": {
            "seaTurtles/CaptureC": minimal_capture(false, &["seaTurtles/CollectionA"]),
        },
        "collections": {
            "seaTurtles/UnboundCollection": minimal_collection(),
        }
    }));
    let mut results = harness
        .user_publication(user_id, "quota single task", draft)
        .await;
    results.publication_row_id = models::Id::zero(); // make it stable for the snapshot
    results.pub_id = Some(models::Id::zero());

    insta::assert_debug_snapshot!(results, @r###"
    ScenarioResult {
        publication_row_id: 0000000000000000,
        pub_id: Some(
            0000000000000000,
        ),
        status: PublishFailed,
        errors: [
            (
                "flow://tenant-quotas/seaTurtles/tasks",
                "Request to add 1 task(s) would exceed tenant 'seaTurtles/' quota of 2. 2 are currently in use.",
            ),
        ],
        live_specs: [],
    }
    "###);
}

#[tokio::test]
#[serial_test::serial]
async fn test_quota_derivations() {
    let mut harness = TestHarness::init("test_quota_derivation").await;

    let user_id = harness.setup_tenant("seagulls").await;

    sqlx::query(
        "update tenants set tasks_quota = 2, collections_quota = 3 where tenant = 'seagulls/';",
    )
    .execute(&harness.pool)
    .await
    .unwrap();

    let setup_draft = draft_catalog(serde_json::json!({
        "captures": {
            "seagulls/CaptureA": minimal_capture(false, &["seagulls/CollectionA"]),
            "seagulls/CaptureB": minimal_capture(false, &["seagulls/CollectionB", "seagulls/CollectionC"]),
            "seagulls/CaptureDisabled": minimal_capture(true, &[]),
        },
        "collections": {
            "seagulls/CollectionA": minimal_collection(),
            "seagulls/CollectionB": minimal_collection(),
            "seagulls/CollectionC": minimal_collection(),
        },
    }));
    let setup_result = harness
        .user_publication(user_id, "setup test quota derivation", setup_draft)
        .await;
    assert!(setup_result.status.is_success());

    let draft = draft_catalog(serde_json::json!({
        "collections": {
            "seagulls/DerivationA": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                },
                "key": ["/id"],
                "derive": {
                    "using":{ "typescript": { "module": "foo.ts"}},
                    "transforms": [{ "source": "seagulls/CollectionA","shuffle": "any", "name": "foo"}]
                }
            }
        }
    }));
    let mut result = harness
        .user_publication(user_id, "test quota derivation", draft)
        .await;
    result.publication_row_id = models::Id::zero();
    result.pub_id = Some(models::Id::zero());

    insta::assert_debug_snapshot!(result, @r###"
    ScenarioResult {
        publication_row_id: 0000000000000000,
        pub_id: Some(
            0000000000000000,
        ),
        status: PublishFailed,
        errors: [
            (
                "flow://tenant-quotas/seagulls/tasks",
                "Request to add 1 task(s) would exceed tenant 'seagulls/' quota of 2. 2 are currently in use.",
            ),
            (
                "flow://tenant-quotas/seagulls/collections",
                "Request to add 1 collections(s) would exceed tenant 'seagulls/' quota of 3. 3 are currently in use.",
            ),
        ],
        live_specs: [],
    }
    "###);
}

// Testing that we can disable tasks to reduce usage when at quota
#[tokio::test]
#[serial_test::parallel]
async fn test_disable_when_over_quota() {
    let mut harness = TestHarness::init("test_disable_when_over_quota").await;

    let user_id = harness.setup_tenant("albatrosses").await;

    let setup_draft = draft_catalog(serde_json::json!({
        "collections": {
            "albatrosses/CollectionA": minimal_collection(),
        },
        "captures": {
            "albatrosses/CaptureA": minimal_capture(false, &["albatrosses/CollectionA"]),
            "albatrosses/CaptureB": minimal_capture(false, &["albatrosses/CollectionA"]),
        }
    }));

    let setup_result = harness
        .user_publication(user_id, "setup disable when over quota", setup_draft)
        .await;
    assert!(setup_result.status.is_success());

    // Now drop the quota to be lower than the current number of tasks
    sqlx::query(
        "update tenants set tasks_quota = 1, collections_quota = 1 where tenant = 'albatrosses/';",
    )
    .execute(&harness.pool)
    .await
    .unwrap();

    let draft = draft_catalog(serde_json::json!({
        "captures": {
            "albatrosses/CaptureA": minimal_capture(true, &["albatrosses/CollectionA"]),
        }
    }));
    let mut result = harness
        .user_publication(user_id, "disable captureA", draft)
        .await;
    result.publication_row_id = models::Id::zero();
    result.pub_id = Some(models::Id::zero());

    insta::assert_debug_snapshot!(result, @r###"
    ScenarioResult {
        publication_row_id: 0000000000000000,
        pub_id: Some(
            0000000000000000,
        ),
        status: Success,
        errors: [],
        live_specs: [
            LiveSpec {
                catalog_name: "albatrosses/CaptureA",
                connector_image_name: Some(
                    "source/test",
                ),
                connector_image_tag: Some(
                    ":dev",
                ),
                reads_from: None,
                writes_to: Some(
                    [
                        "albatrosses/CollectionA",
                    ],
                ),
                spec: Some(
                    Object {
                        "bindings": Array [
                            Object {
                                "resource": Object {
                                    "id": String("albatrosses/CollectionA"),
                                },
                                "target": String("albatrosses/CollectionA"),
                            },
                        ],
                        "endpoint": Object {
                            "connector": Object {
                                "config": Object {},
                                "image": String("source/test:dev"),
                            },
                        },
                        "shards": Object {
                            "disable": Bool(true),
                        },
                    },
                ),
                spec_type: Some(
                    "capture",
                ),
            },
        ],
    }
    "###);
}

fn minimal_capture(disable: bool, targets: &[&str]) -> serde_json::Value {
    let bindings = targets
        .iter()
        .map(|collection| {
            serde_json::json!({
                "resource": { "id": collection },
                "target": collection,
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "shards": { "disable": disable },
        "endpoint": {
            "connector": {
                "image": "source/test:dev",
                "config": {}
            }
        },
        "bindings": bindings,
    })
}

fn minimal_collection() -> serde_json::Value {
    serde_json::json!({
        "schema": {
            "type": "object",
            "properties": {
                "id": { "type": "string" }
            }
        },
        "key": ["/id"]
    })
}
