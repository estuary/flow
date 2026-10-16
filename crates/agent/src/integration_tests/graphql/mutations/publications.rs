//! Publications submitted with `createPublication`, built and committed by the
//! publications executor, and read back with `publication(id)`.

use crate::integration_tests::harness::{self, TestHarness};
use serde_json::json;

const CREATE: &str = r#"
mutation CreatePublication($draftId: Id!, $dryRun: Boolean!, $detail: String) {
    createPublication(draftId: $draftId, dryRun: $dryRun, detail: $detail) {
        id
    }
}
"#;

const LOOKUP: &str = r#"
query Publication($id: Id!) {
    publication(id: $id) {
        id
        draftId
        dryRun
        pubId
        status { type }
        errors { catalogName scope detail }
    }
}
"#;

/// A collection `aliceCo/data/foo` and a capture `aliceCo/in/cap` into it,
/// with the capture running the connector `image`.
fn capture_draft(image: &str) -> tables::DraftCatalog {
    harness::draft_catalog(json!({
        "collections": {
            "aliceCo/data/foo": {
                "schema": {
                    "type": "object",
                    "properties": { "id": { "type": "string" } },
                    "required": ["id"]
                },
                "key": ["/id"]
            }
        },
        "captures": {
            "aliceCo/in/cap": {
                "endpoint": { "connector": { "image": image, "config": {} } },
                "bindings": [
                    { "resource": { "id": "foo" }, "target": "aliceCo/data/foo" }
                ]
            }
        }
    }))
}

async fn submit(
    harness: &mut TestHarness,
    user_id: uuid::Uuid,
    draft_id: models::Id,
    dry_run: bool,
    detail: Option<&str>,
) -> models::Id {
    let data: serde_json::Value = harness
        .execute_graphql_query(
            user_id,
            CREATE,
            &json!({ "draftId": draft_id, "dryRun": dry_run, "detail": detail }),
        )
        .await
        .expect("createPublication failed");
    serde_json::from_value(data["createPublication"]["id"].clone()).expect("id")
}

async fn lookup(
    harness: &mut TestHarness,
    user_id: uuid::Uuid,
    id: models::Id,
) -> serde_json::Value {
    let data: serde_json::Value = harness
        .execute_graphql_query(user_id, LOOKUP, &json!({ "id": id }))
        .await
        .expect("publication query failed");
    data["publication"].clone()
}

#[tokio::test]
async fn test_graphql_publication_commits_and_reports() {
    let mut harness = TestHarness::init("graphql_publication_commits").await;
    let alice = harness.setup_tenant("aliceCo").await;
    let draft_id = harness
        .create_draft(alice, "initial", capture_draft("source/test:test"))
        .await;

    let id = submit(&mut harness, alice, draft_id, false, Some("via graphql")).await;

    // `pubId` is the `last_pub_id` of the specifications the commit published.
    // The draft is deleted on success, so `errors` is empty.
    let result = harness.run_queued_publication(id).await;
    assert!(result.status.is_success(), "{:?}", result.errors);
    let pub_id = result.pub_id.expect("a committed publication has a pub_id");

    let looked_up = lookup(&mut harness, alice, id).await;
    assert_eq!(looked_up["id"], json!(id));
    assert_eq!(looked_up["status"]["type"], "success");
    assert_eq!(looked_up["pubId"], json!(pub_id));
    assert_eq!(looked_up["errors"], json!([]));

    let mut published = result
        .live_specs
        .iter()
        .map(|spec| spec.catalog_name.as_str())
        .collect::<Vec<_>>();
    published.sort();
    assert_eq!(published, ["aliceCo/data/foo", "aliceCo/in/cap"]);

    let (detail, draft_exists): (Option<String>, bool) = sqlx::query_as(
        r#"
        SELECT p.detail, EXISTS (SELECT 1 FROM drafts d WHERE d.id = p.draft_id)
        FROM publications p WHERE p.id = $1
        "#,
    )
    .bind(id)
    .fetch_one(&harness.pool)
    .await
    .unwrap();
    assert_eq!(detail.as_deref(), Some("via graphql"));
    assert!(!draft_exists, "a successful publication deletes its draft");
}

#[tokio::test]
async fn test_graphql_publication_dry_run_builds_without_committing() {
    let mut harness = TestHarness::init("graphql_publication_dry_run").await;
    let alice = harness.setup_tenant("aliceCo").await;
    let draft_id = harness
        .create_draft(alice, "dry run", capture_draft("source/test:test"))
        .await;

    let id = submit(&mut harness, alice, draft_id, true, None).await;

    // A successful dry run keeps the draft and writes the built specifications
    // into it. It records a `pub_id` that no live specification carries,
    // because nothing is committed.
    let result = harness.run_queued_publication(id).await;
    assert!(result.status.is_success(), "{:?}", result.errors);
    let pub_id = result
        .pub_id
        .expect("a successful dry run records its publication id");
    assert!(result.live_specs.is_empty());

    let looked_up = lookup(&mut harness, alice, id).await;
    assert_eq!(looked_up["dryRun"], true);
    assert_eq!(looked_up["status"]["type"], "success");
    assert_eq!(looked_up["pubId"], json!(pub_id));
    assert_eq!(looked_up["errors"], json!([]));

    harness
        .assert_live_spec_hard_deleted("aliceCo/data/foo")
        .await;
    let built: Vec<(String, bool)> = sqlx::query_as(
        r#"
        SELECT catalog_name::text, built_spec IS NOT NULL
        FROM draft_specs WHERE draft_id = $1 ORDER BY catalog_name
        "#,
    )
    .bind(draft_id)
    .fetch_all(&harness.pool)
    .await
    .unwrap();
    assert_eq!(
        built,
        [
            ("aliceCo/data/foo".to_string(), true),
            ("aliceCo/in/cap".to_string(), true),
        ]
    );
}

#[tokio::test]
async fn test_graphql_publication_build_failure_is_reported() {
    let mut harness = TestHarness::init("graphql_publication_build_failure").await;
    let alice = harness.setup_tenant("aliceCo").await;
    // Alice may publish every name here, so the draft is queued. The connector
    // image is forbidden, which only the executor's build detects.
    let draft_id = harness
        .create_draft(
            alice,
            "forbidden image",
            capture_draft("forbidden_connector:v99"),
        )
        .await;

    let id = submit(&mut harness, alice, draft_id, false, None).await;

    let result = harness.run_queued_publication(id).await;
    assert!(!result.status.is_success());
    assert_eq!(result.pub_id, None);

    let looked_up = lookup(&mut harness, alice, id).await;
    insta::assert_json_snapshot!(looked_up, {
        ".id" => "[id]",
        ".draftId" => "[draft-id]",
    }, @r#"
    {
      "draftId": "[draft-id]",
      "dryRun": false,
      "errors": [
        {
          "catalogName": "aliceCo/in/cap",
          "detail": "Forbidden connector image 'forbidden_connector'",
          "scope": "flow://capture/aliceCo/in/cap"
        }
      ],
      "id": "[id]",
      "pubId": null,
      "status": {
        "type": "buildFailed"
      }
    }
    "#);
}
