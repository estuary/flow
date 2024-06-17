use crate::integration_tests::harness::{draft_catalog, TestHarness};

#[tokio::test]
#[serial_test::serial]
async fn test_quota_single_task() {
    let mut harness = TestHarness::init("test_quota_single_task").await;

    let user_id = harness.setup_tenant("usageB").await;

    sqlx::query(
        "update tenants set tasks_quota = 2, collections_quota = 2 where tenant = 'usageB/';",
    )
    .execute(&harness.pool)
    .await
    .unwrap();

    let draft = draft_catalog(serde_json::json!({
        "captures": {
            "usageB/CaptureA": minimal_capture(false, &["usageB/CollectionA"]),
            "usageB/CaptureB": minimal_capture(false, &["usageB/CollectionB"]),
            "usageB/CaptureDisabled": minimal_capture(true, &[]),
        },
        "collections": {
            "usageB/CollectionA": minimal_collection(),
            "usageB/CollectionB": minimal_collection(),
        }
    }));
    let result = harness
        .user_publication(user_id, "setup quota single task", draft)
        .await;
    assert!(result.status.is_success());

    let draft = draft_catalog(serde_json::json!({
        "captures": {
            "usageB/CaptureC": minimal_capture(false, &["usageB/CollectionA"]),
        },
        "collections": {
            "usageB/UnboundCollection": minimal_collection(),
        }
    }));
    let mut results = harness
        .user_publication(user_id, "quota single task", draft)
        .await;
    results.publication_id = models::Id::zero(); // make it stable for the snapshot

    insta::assert_debug_snapshot!(results, @r###"
    ScenarioResult {
        publication_id: 0000000000000000,
        status: PublishFailed,
        errors: [
            (
                "flow://tenant-quotas/usageB/tasks",
                "Request to add 1 task(s) would exceed tenant 'usageB/' quota of 2. 2 are currently in use.",
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

    let user_id = harness.setup_tenant("usageB").await;

    sqlx::query(
        "update tenants set tasks_quota = 2, collections_quota = 2 where tenant = 'usageB/';",
    )
    .execute(&harness.pool)
    .await
    .unwrap();

    let setup_draft = draft_catalog(serde_json::json!({
        "captures": {
            "usageB/CaptureA": minimal_capture(false, &["usageB/CollectionA"]),
            "usageB/CaptureB": minimal_capture(false, &["usageB/CollectionB"]),
            "usageB/CaptureDisabled": minimal_capture(true, &[]),
        },
        "collections": {
            "usageB/CollectionA": minimal_collection(),
            "usageB/CollectionB": minimal_collection(),
        },
    }));
    let setup_result = harness
        .user_publication(user_id, "setup test quota derivation", setup_draft)
        .await;
    assert!(setup_result.status.is_success());

    let draft = draft_catalog(serde_json::json!({
        "collections": {
            "usageB/DerivationA": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                },
                "key": ["/id"],
                "derive": {
                    "using":{ "typescript": { "module": "foo.ts"}},
                    "transforms": [{ "source": "usageB/CollectionA","shuffle": "any", "name": "foo"}]
                }
            }
        }
    }));
    let mut result = harness
        .user_publication(user_id, "test quota derivation", draft)
        .await;
    result.publication_id = models::Id::zero();

    insta::assert_debug_snapshot!(result, @r###"
    ScenarioResult {
        publication_id: 0000000000000000,
        status: PublishFailed,
        errors: [
            (
                "flow://tenant-quotas/usageB/tasks",
                "Request to add 1 task(s) would exceed tenant 'usageB/' quota of 2. 2 are currently in use.",
            ),
            (
                "flow://tenant-quotas/usageB/collections",
                "Request to add 1 collections(s) would exceed tenant 'usageB/' quota of 2. 2 are currently in use.",
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

    let user_id = harness.setup_tenant("usageC").await;

    let setup_draft = draft_catalog(serde_json::json!({
        "collections": {
            "usageC/CollectionA": minimal_collection(),
        },
        "captures": {
            "usageC/CaptureA": minimal_capture(false, &["usageC/CollectionA"]),
            "usageC/CaptureB": minimal_capture(false, &["usageC/CollectionA"]),
        }
    }));

    let setup_result = harness
        .user_publication(user_id, "setup disable when over quota", setup_draft)
        .await;
    assert!(setup_result.status.is_success());

    // Now drop the quota to be lower than the current number of tasks
    sqlx::query(
        "update tenants set tasks_quota = 1, collections_quota = 1 where tenant = 'usageC/';",
    )
    .execute(&harness.pool)
    .await
    .unwrap();

    let draft = draft_catalog(serde_json::json!({
        "captures": {
            "usageC/CaptureA": minimal_capture(true, &["usageC/CollectionA"]),
        }
    }));
    let mut result = harness
        .user_publication(user_id, "disable captureA", draft)
        .await;
    result.publication_id = models::Id::zero();

    insta::assert_debug_snapshot!(result, @r###"
    ScenarioResult {
        publication_id: 0000000000000000,
        status: Success {
            linked_materialization_publications: [],
        },
        errors: [],
        live_specs: [
            LiveSpec {
                catalog_name: "usageC/CaptureA",
                connector_image_name: Some(
                    "source/test",
                ),
                connector_image_tag: Some(
                    ":dev",
                ),
                reads_from: Some(
                    [],
                ),
                writes_to: Some(
                    [
                        "usageC/CollectionA",
                    ],
                ),
                spec: Some(
                    Object {
                        "bindings": Array [
                            Object {
                                "resource": Object {},
                                "target": String("usageC/CollectionA"),
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
                "resource": {},
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
