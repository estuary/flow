//! End-to-end coverage of the GraphQL discover surface: `createDiscover` runs
//! against the real schema in-process, the real `DiscoverExecutor` polls the
//! task the row's trigger enqueued, and `discover(id)` reports the outcome.

use crate::integration_tests::harness::{TestHarness, draft_catalog};
use crate::integration_tests::spec_fixture;
use proto_flow::capture::response::{Discovered, discovered::Binding};
use serde_json::json;
use sqlx::types::Uuid;

const CREATE_DISCOVER: &str = r#"
mutation($captureName: Capture!, $connectorTagId: Id!, $draftId: Id, $endpointConfig: JSON!) {
    createDiscover(
        captureName: $captureName
        connectorTagId: $connectorTagId
        dataPlaneName: "ops/dp/public/test"
        draftId: $draftId
        endpointConfig: $endpointConfig
    ) {
        id
        draftId
        status
    }
}
"#;

const CAPTURE: &str = "aliceCo/e2e/capture";

const DISCOVER_STATUS: &str = r#"
query($id: Id!) {
    discover(id: $id) { status }
}
"#;

/// A discover response naming one collection per `names` entry, all keyed on `/id`.
fn discovered(names: &[&str]) -> Discovered {
    Discovered {
        bindings: names
            .iter()
            .map(|name| Binding {
                recommended_name: name.to_string(),
                document_schema_json: json!({
                    "type": "object",
                    "properties": { "id": { "type": "string" } },
                    "required": ["id"],
                })
                .to_string()
                .into(),
                resource_config_json: json!({ "id": name }).to_string().into(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(),
                is_fallback_key: false,
            })
            .collect(),
    }
}

/// The connector tag id a client would obtain before calling `createDiscover`.
async fn source_test_tag_id(harness: &mut TestHarness, user_id: Uuid) -> String {
    let response: serde_json::Value = harness
        .execute_graphql_query(
            user_id,
            r#"query { connector(imageName: "source/test") { spec(imageTag: ":test") { id } } }"#,
            &json!({}),
        )
        .await
        .unwrap();
    response["connector"]["spec"]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("no source/test:test spec in {response}"))
        .to_string()
}

async fn create_discover(
    harness: &mut TestHarness,
    user_id: Uuid,
    capture_name: &str,
    connector_tag_id: &str,
    draft_id: Option<models::Id>,
) -> anyhow::Result<serde_json::Value> {
    let response: serde_json::Value = harness
        .execute_graphql_query(
            user_id,
            CREATE_DISCOVER,
            &json!({
                "captureName": capture_name,
                "connectorTagId": connector_tag_id,
                "draftId": draft_id,
                "endpointConfig": { "tail": "shake" },
            }),
        )
        .await?;
    Ok(response["createDiscover"].clone())
}

async fn discover_status(harness: &mut TestHarness, user_id: Uuid, id: models::Id) -> String {
    let response: serde_json::Value = harness
        .execute_graphql_query(user_id, DISCOVER_STATUS, &json!({ "id": id }))
        .await
        .unwrap();
    response["discover"]["status"].as_str().unwrap().to_string()
}

fn parse_id(value: &serde_json::Value) -> models::Id {
    models::Id::from_hex(value.as_str().unwrap()).unwrap()
}

fn assert_success(result: &crate::integration_tests::harness::UserDiscoverResult) {
    assert!(
        result.job_status.is_success(),
        "expected success, got: {:?} with errors {:?}",
        result.job_status,
        result.errors
    );
}

#[tokio::test]
async fn test_discover_end_to_end_with_new_draft() {
    let mut harness = TestHarness::init("graphql_discover_new_draft").await;
    let alice = harness.setup_tenant("aliceCo").await;
    let tag_id = source_test_tag_id(&mut harness, alice).await;
    harness.discover_handler.connectors.mock_discover(
        CAPTURE,
        Ok((spec_fixture(), discovered(&["acorns", "walnuts"]))),
    );

    let created = create_discover(&mut harness, alice, CAPTURE, &tag_id, None)
        .await
        .unwrap();
    assert_eq!("QUEUED", created["status"]);
    let discover_id = parse_id(&created["id"]);
    let draft_id = parse_id(&created["draftId"]);

    let result = harness.run_queued_discover(discover_id).await;
    assert_success(&result);
    assert_eq!(
        "SUCCESS",
        discover_status(&mut harness, alice, discover_id).await
    );

    // The mutation created the draft for the caller, and the executor filled
    // it with the capture and its collections.
    let owner: Uuid = sqlx::query_scalar("SELECT user_id FROM drafts WHERE id = $1")
        .bind(draft_id)
        .fetch_one(&harness.pool)
        .await
        .unwrap();
    assert_eq!(alice, owner);
    insta::assert_debug_snapshot!("new-draft", result.draft);
}

#[tokio::test]
async fn test_discover_end_to_end_into_existing_draft() {
    let mut harness = TestHarness::init("graphql_discover_existing_draft").await;
    let alice = harness.setup_tenant("aliceCo").await;
    let tag_id = source_test_tag_id(&mut harness, alice).await;

    // The draft already holds the capture with one binding and an unrelated
    // collection. A re-discover must keep both and merge the new binding.
    let draft_id = harness
        .create_draft(
            alice,
            "pre-existing work",
            draft_catalog(json!({
                "captures": {
                    CAPTURE: {
                        "endpoint": { "connector": { "image": "source/test:test", "config": {} } },
                        "bindings": [
                            {
                                "resource": { "id": "acorns", "kept": "drafted resource config" },
                                "target": "aliceCo/e2e/acorns"
                            }
                        ]
                    }
                },
                "collections": {
                    "aliceCo/e2e/unrelated": {
                        "schema": { "type": "object", "properties": { "id": { "type": "string" } } },
                        "key": ["/id"]
                    }
                }
            })),
        )
        .await;
    harness.discover_handler.connectors.mock_discover(
        CAPTURE,
        Ok((spec_fixture(), discovered(&["acorns", "walnuts"]))),
    );

    let created = create_discover(&mut harness, alice, CAPTURE, &tag_id, Some(draft_id))
        .await
        .unwrap();
    assert_eq!(draft_id, parse_id(&created["draftId"]));
    let discover_id = parse_id(&created["id"]);

    let result = harness.run_queued_discover(discover_id).await;
    assert_success(&result);
    assert_eq!(
        "SUCCESS",
        discover_status(&mut harness, alice, discover_id).await
    );
    insta::assert_debug_snapshot!("existing-draft", result.draft);
}

#[tokio::test]
async fn test_discover_end_to_end_connector_failure() {
    let mut harness = TestHarness::init("graphql_discover_connector_failure").await;
    let alice = harness.setup_tenant("aliceCo").await;
    let tag_id = source_test_tag_id(&mut harness, alice).await;
    harness.discover_handler.connectors.mock_discover(
        CAPTURE,
        Err("the connector could not reach the endpoint".to_string()),
    );

    let created = create_discover(&mut harness, alice, CAPTURE, &tag_id, None)
        .await
        .unwrap();
    let discover_id = parse_id(&created["id"]);

    let result = harness.run_queued_discover(discover_id).await;
    assert_eq!(
        "DISCOVER_FAILED",
        discover_status(&mut harness, alice, discover_id).await
    );
    insta::assert_debug_snapshot!("connector-failure-errors", result.errors);
}

#[tokio::test]
async fn test_discover_end_to_end_unauthorized_capture() {
    let mut harness = TestHarness::init("graphql_discover_unauthorized").await;
    let alice = harness.setup_tenant("aliceCo").await;
    let tag_id = source_test_tag_id(&mut harness, alice).await;

    // Alice holds nothing under bobCo/, so the mutation is rejected before a
    // row is written and there is no task for the executor to run.
    let err = create_discover(&mut harness, alice, "bobCo/e2e/capture", &tag_id, None)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("PermissionDenied") && err.contains("SpecEdit"),
        "unexpected error: {err}"
    );
    assert_eq!(
        None,
        harness
            .run_automation_task(automations::task_types::DISCOVERS)
            .await
    );
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM discovers")
        .fetch_one(&harness.pool)
        .await
        .unwrap();
    assert_eq!(0, rows);
}
