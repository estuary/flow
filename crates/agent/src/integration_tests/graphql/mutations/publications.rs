//! Publications driven entirely through the GraphQL API: `createPublication`
//! admits and queues a draft, the real publications executor runs it, and
//! `publication(id)` reads the outcome.

use crate::integration_tests::harness::{self, TestHarness};
use serde_json::json;

const CREATE: &str = r#"
mutation CreatePublication($draftId: Id!, $dryRun: Boolean!, $detail: String) {
    createPublication(draftId: $draftId, dryRun: $dryRun, detail: $detail) {
        id
        draftId
        dryRun
        pubId
        status { type }
        errors { catalogName scope detail }
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

/// A collection and a capture into it under `prefix`, using the test
/// connector image the harness accepts.
fn capture_draft(prefix: &str, image: &str) -> tables::DraftCatalog {
    harness::draft_catalog(json!({
        "collections": {
            format!("{prefix}data/foo"): {
                "schema": {
                    "type": "object",
                    "properties": { "id": { "type": "string" } },
                    "required": ["id"]
                },
                "key": ["/id"]
            }
        },
        "captures": {
            format!("{prefix}in/cap"): {
                "endpoint": { "connector": { "image": image, "config": {} } },
                "bindings": [
                    { "resource": { "id": "foo" }, "target": format!("{prefix}data/foo") }
                ]
            }
        }
    }))
}

/// Submits `draft_id` through the mutation and returns the `createPublication`
/// payload together with its id.
async fn submit(
    harness: &mut TestHarness,
    user_id: uuid::Uuid,
    draft_id: models::Id,
    dry_run: bool,
    detail: Option<&str>,
) -> (models::Id, serde_json::Value) {
    let data: serde_json::Value = harness
        .execute_graphql_query(
            user_id,
            CREATE,
            &json!({ "draftId": draft_id, "dryRun": dry_run, "detail": detail }),
        )
        .await
        .expect("createPublication failed");
    let created = data["createPublication"].clone();
    let id = serde_json::from_value(created["id"].clone()).expect("id");
    (id, created)
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

async fn live_spec_ids(harness: &TestHarness, catalog_name: &str) -> Option<(String, String)> {
    sqlx::query_as(
        "SELECT last_pub_id::text, last_build_id::text FROM live_specs WHERE catalog_name = $1",
    )
    .bind(catalog_name)
    .fetch_optional(&harness.pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn test_graphql_publication_commits_and_reports() {
    let mut harness = TestHarness::init("graphql_publication_commits").await;
    let alice = harness.setup_tenant("aliceCo").await;
    let draft_id = harness
        .create_draft(
            alice,
            "initial",
            capture_draft("aliceCo/", "source/test:test"),
        )
        .await;

    // The mutation admits and queues: nothing is built yet.
    let (id, created) = submit(&mut harness, alice, draft_id, false, Some("via graphql")).await;
    insta::assert_json_snapshot!(created, {
        ".id" => "[id]",
        ".draftId" => "[draft-id]",
    }, @r#"
    {
      "draftId": "[draft-id]",
      "dryRun": false,
      "errors": [],
      "id": "[id]",
      "pubId": null,
      "status": {
        "type": "queued"
      }
    }
    "#);
    assert_eq!(created["draftId"], json!(draft_id));

    // The executor commits it, and the query reports the committed id, which
    // is now the `last_pub_id` of every published specification. The draft
    // is deleted on success, so the publication carries no errors.
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
        .create_draft(
            alice,
            "dry run",
            capture_draft("aliceCo/", "source/test:test"),
        )
        .await;

    let (id, created) = submit(&mut harness, alice, draft_id, true, None).await;
    assert_eq!(created["dryRun"], true);
    assert_eq!(created["status"]["type"], "queued");

    // A successful dry run publishes nothing, keeps the draft, and writes the
    // built specifications back into it. The executor still records the id
    // the build ran under as `pub_id`, which no live specification carries.
    let result = harness.run_queued_publication(id).await;
    assert!(result.status.is_success(), "{:?}", result.errors);
    let build_id = result
        .pub_id
        .expect("a successful dry run records its build id");
    assert!(result.live_specs.is_empty());

    let looked_up = lookup(&mut harness, alice, id).await;
    assert_eq!(looked_up["dryRun"], true);
    assert_eq!(looked_up["status"]["type"], "success");
    assert_eq!(looked_up["pubId"], json!(build_id));
    assert_eq!(looked_up["errors"], json!([]));

    assert_eq!(live_spec_ids(&harness, "aliceCo/data/foo").await, None);
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
    // Alice may publish every name here, so admission queues the draft. The
    // connector image is forbidden, which only the executor's build detects.
    let draft_id = harness
        .create_draft(
            alice,
            "forbidden image",
            capture_draft("aliceCo/", "forbidden_connector:v99"),
        )
        .await;

    let (id, created) = submit(&mut harness, alice, draft_id, false, None).await;
    assert_eq!(created["status"]["type"], "queued");

    let result = harness.run_queued_publication(id).await;
    assert!(!result.status.is_success());
    assert_eq!(result.pub_id, None);

    // The failure and the draft's errors are visible through the query.
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

#[tokio::test]
async fn test_graphql_publication_builds_expanded_live_tasks() {
    let mut harness = TestHarness::init("graphql_publication_expansion").await;
    let alice = harness.setup_tenant("aliceCo").await;

    // A live collection and a live materialization reading it, published
    // through the harness.
    let initial = harness
        .user_publication(
            alice,
            "initial",
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
                "materializations": {
                    "aliceCo/out/mat": {
                        "endpoint": { "connector": { "image": "materialize/test:test", "config": {} } },
                        "bindings": [
                            { "resource": { "id": "foo" }, "source": "aliceCo/data/foo" }
                        ]
                    }
                }
            })),
        )
        .await;
    assert!(initial.status.is_success(), "{:?}", initial.errors);
    let (mat_pub_before, mat_build_before) =
        live_spec_ids(&harness, "aliceCo/out/mat").await.unwrap();

    // Publishing a change to the collection through the API expands to the
    // materialization, which Alice may edit: it is built and touched, so its
    // `last_build_id` advances while its `last_pub_id` does not.
    let draft_id = harness
        .create_draft(
            alice,
            "widen the schema",
            harness::draft_catalog(json!({
                "collections": {
                    "aliceCo/data/foo": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                                "note": { "type": "string" }
                            },
                            "required": ["id"]
                        },
                        "key": ["/id"]
                    }
                }
            })),
        )
        .await;
    let (id, _) = submit(&mut harness, alice, draft_id, false, None).await;
    let result = harness.run_queued_publication(id).await;
    assert!(result.status.is_success(), "{:?}", result.errors);
    let pub_id = result.pub_id.unwrap();

    let published = result
        .live_specs
        .iter()
        .map(|spec| spec.catalog_name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(published, ["aliceCo/data/foo"]);

    let (mat_pub_after, mat_build_after) =
        live_spec_ids(&harness, "aliceCo/out/mat").await.unwrap();
    assert_eq!(mat_pub_after, mat_pub_before);
    assert!(
        mat_build_after > mat_build_before,
        "expanded materialization was not built: {mat_build_before} -> {mat_build_after}"
    );
    let (_, foo_build_after) = live_spec_ids(&harness, "aliceCo/data/foo").await.unwrap();
    assert_eq!(foo_build_after, mat_build_after, "one build covered both");
    assert_eq!(
        lookup(&mut harness, alice, id).await["pubId"],
        json!(pub_id)
    );
}
