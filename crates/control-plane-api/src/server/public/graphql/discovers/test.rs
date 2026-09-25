use crate::test_server;

const ALICE: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);
const BOB: uuid::Uuid = uuid::Uuid::from_bytes([0x22; 16]);
const CREATE: &str = r#"
mutation ($draftId: Id!, $captureName: Name!, $dataPlane: String) {
  createDiscover(draftId: $draftId, captureName: $captureName, dataPlane: $dataPlane) {
    id draftId captureName dataPlaneName status createdAt updatedAt
    errors { catalogName scope detail }
  }
}"#;
const LOOKUP: &str = r#"
query ($id: Id!, $after: String, $first: Int) {
  discover(id: $id) {
    id draftId captureName dataPlaneName status
    errors { catalogName scope detail }
    logs(after: $after, first: $first) {
      edges { cursor node { loggedAt stream line } }
      pageInfo { hasNextPage endCursor }
    }
  }
}"#;

async fn setup(pool: &sqlx::PgPool) -> (test_server::TestServer, models::Id, String, String) {
    // The shared storage fixture covers partition mappings. Discovery also
    // checks their corresponding recovery mappings, as publication does.
    sqlx::query(
        r#"
        INSERT INTO storage_mappings (catalog_prefix, spec)
        SELECT 'recovery/' || catalog_prefix::text, spec
        FROM storage_mappings
        "#,
    )
    .execute(pool)
    .await
    .unwrap();
    let draft_id: models::Id =
        sqlx::query_scalar("INSERT INTO drafts (user_id) VALUES ($1) RETURNING id")
            .bind(ALICE)
            .fetch_one(pool)
            .await
            .unwrap();
    let snapshot = test_server::snapshot(pool.clone(), false).await;
    let server = test_server::TestServer::start(pool.clone(), snapshot).await;
    let alice = server.make_access_token(ALICE, Some("alice@example.com"));
    let bob = server.make_access_token(BOB, Some("bob@example.test"));
    (server, draft_id, alice, bob)
}

async fn stage_capture(pool: &sqlx::PgPool, draft_id: models::Id, name: &str, model: &str) {
    sqlx::query(
        "INSERT INTO draft_specs (draft_id, catalog_name, spec_type, spec) VALUES ($1, $2, 'capture', $3::json)",
    )
    .bind(draft_id)
    .bind(name)
    .bind(model)
    .execute(pool)
    .await
    .unwrap();
}

async fn submit(
    server: &test_server::TestServer,
    token: Option<&str>,
    draft_id: models::Id,
    name: &str,
    plane: Option<&str>,
) -> serde_json::Value {
    server
        .graphql(
            &serde_json::json!({
                "query": CREATE,
                "variables": { "draftId": draft_id, "captureName": name, "dataPlane": plane }
            }),
            token,
        )
        .await
}

async fn submit_without_plane_argument(
    server: &test_server::TestServer,
    token: &str,
    draft_id: models::Id,
    name: &str,
) -> serde_json::Value {
    server
        .graphql(
            &serde_json::json!({
                "query": CREATE,
                "variables": { "draftId": draft_id, "captureName": name }
            }),
            Some(token),
        )
        .await
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn staged_submission_and_private_query(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice, bob) = setup(&pool).await;
    let model = r#"{"endpoint":{"connector":{"image":"source/test:test","config":{"password":"example"}}},"bindings":[],"autoDiscover":{}}"#;
    stage_capture(&pool, draft_id, "aliceCo/new-capture", model).await;

    let before: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT updated_at FROM drafts WHERE id = $1")
            .bind(draft_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let response = submit(&server, Some(&alice), draft_id, "aliceCo/new-capture", None).await;
    assert!(response.get("errors").is_none(), "{response}");
    let discover = &response["data"]["createDiscover"];
    assert_eq!(discover["status"], "queued");
    assert_eq!(discover["dataPlaneName"], "ops/dp/public/aws-us-west-2-c1");
    let id: models::Id = serde_json::from_value(discover["id"].clone()).unwrap();

    let persisted: (bool, String) =
        sqlx::query_as("SELECT update_only, endpoint_config::text FROM discovers WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(persisted.0, "empty autoDiscover disables new bindings");
    assert_eq!(persisted.1.trim(), r#"{"password":"example"}"#);
    let after: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT updated_at FROM drafts WHERE id = $1")
            .bind(draft_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        after, before,
        "a staged capture is not modified on submission"
    );
    let tasks: i64 = sqlx::query_scalar("SELECT count(*) FROM internal.tasks WHERE task_id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(tasks, 1);

    let foreign: serde_json::Value = server
        .graphql(
            &serde_json::json!({ "query": LOOKUP, "variables": { "id": id } }),
            Some(&bob),
        )
        .await;
    assert_eq!(foreign["data"]["discover"], serde_json::Value::Null);
    let foreign_create = submit(&server, Some(&bob), draft_id, "aliceCo/new-capture", None).await;
    let missing_create = submit(
        &server,
        Some(&bob),
        models::Id::new(u64::MAX.to_be_bytes()),
        "aliceCo/new-capture",
        None,
    )
    .await;
    assert_eq!(foreign_create["errors"][0]["message"], "draft not found");
    assert_eq!(
        foreign_create["errors"][0]["message"],
        missing_create["errors"][0]["message"]
    );
    let owner: serde_json::Value = server
        .graphql(
            &serde_json::json!({ "query": LOOKUP, "variables": { "id": id } }),
            Some(&alice),
        )
        .await;
    assert_eq!(owner["data"]["discover"]["id"], discover["id"]);

    let end_cursor = owner["data"]["discover"]["logs"]["pageInfo"]["endCursor"].as_str();
    assert!(end_cursor.is_none(), "a new discover has no log cursor");

    let unauthenticated = submit(&server, None, draft_id, "aliceCo/new-capture", None).await;
    assert!(unauthenticated.get("errors").is_some());
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn copies_live_capture_and_rolls_back_rejections(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let live_model = r#"{"endpoint":{"connector":{"image":"source/test:test","config":{"live":true}}},"bindings":[]}"#;
    sqlx::query(
        "UPDATE live_specs SET spec = $1::json WHERE catalog_name = 'aliceCo/in/capture-foo'",
    )
    .bind(live_model)
    .execute(&pool)
    .await
    .unwrap();
    let (server, draft_id, alice, _) = setup(&pool).await;
    let copied = submit(
        &server,
        Some(&alice),
        draft_id,
        "aliceCo/in/capture-foo",
        None,
    )
    .await;
    assert!(copied.get("errors").is_none(), "{copied}");
    let entry: (String, models::Id, models::Id) = sqlx::query_as(
        r#"
        SELECT ds.spec::text, ds.expect_pub_id, ls.last_pub_id
        FROM draft_specs ds JOIN live_specs ls ON ls.catalog_name = ds.catalog_name
        WHERE ds.draft_id = $1 AND ds.catalog_name = 'aliceCo/in/capture-foo'
        "#,
    )
    .bind(draft_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(entry.0, live_model);
    assert_eq!(entry.1, entry.2);

    let staged_model = r#"{"endpoint":{"connector":{"image":"source/test:test","config":{"draft":true}}},"bindings":[],"autoDiscover":{"addNewBindings":false}}"#;
    sqlx::query("UPDATE draft_specs SET spec = $1::json WHERE draft_id = $2 AND catalog_name = 'aliceCo/in/capture-foo'")
        .bind(staged_model)
        .bind(draft_id)
        .execute(&pool)
        .await
        .unwrap();
    let staged =
        submit_without_plane_argument(&server, &alice, draft_id, "aliceCo/in/capture-foo").await;
    assert!(staged.get("errors").is_none(), "{staged}");
    let staged_config: String =
        sqlx::query_scalar("SELECT endpoint_config::text FROM discovers WHERE id = $1")
            .bind(
                serde_json::from_value::<models::Id>(
                    staged["data"]["createDiscover"]["id"].clone(),
                )
                .unwrap(),
            )
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(staged_config.trim(), r#"{"draft":true}"#);

    let denied = submit(&server, Some(&alice), draft_id, "aliceCo/missing", None).await;
    assert_eq!(denied["errors"][0]["message"], "capture not found");
    let wrong_plane = submit(
        &server,
        Some(&alice),
        draft_id,
        "aliceCo/in/capture-foo",
        Some("ops/dp/public/gcp-us-central1-c2"),
    )
    .await;
    assert_eq!(
        wrong_plane["errors"][0]["message"],
        "data plane differs from the live capture"
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM discovers WHERE draft_id = $1")
        .bind(draft_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 2, "rejected submissions did not enqueue jobs");
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn logs_paginate_and_errors_follow_draft(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice, _) = setup(&pool).await;
    stage_capture(
        &pool,
        draft_id,
        "aliceCo/logged",
        r#"{"endpoint":{"connector":{"image":"source/test:test","config":{}}},"bindings":[]}"#,
    )
    .await;
    let response = submit(&server, Some(&alice), draft_id, "aliceCo/logged", None).await;
    assert!(response.get("errors").is_none(), "{response}");
    let id: models::Id =
        serde_json::from_value(response["data"]["createDiscover"]["id"].clone()).unwrap();
    sqlx::query(
        r#"
        INSERT INTO internal.log_lines (token, stream, log_line, logged_at)
        SELECT logs_token, 'test', line, ts::timestamptz FROM discovers,
        (VALUES ('first', '2026-01-01T00:00:00.000001Z'),
                ('second', '2026-01-01T00:00:00.000002Z'),
                ('third', '2026-01-01T00:00:00.000003Z')) AS lines(line, ts)
        WHERE id = $1
        "#,
    )
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO draft_errors (draft_id, scope, detail) VALUES ($1, 'capture://aliceCo/logged', 'old error')")
        .bind(draft_id)
        .execute(&pool)
        .await
        .unwrap();

    let first: serde_json::Value = server
        .graphql(
            &serde_json::json!({ "query": LOOKUP, "variables": { "id": id, "first": 2 } }),
            Some(&alice),
        )
        .await;
    assert!(first.get("errors").is_none(), "{first}");
    let job = &first["data"]["discover"];
    assert_eq!(job["errors"][0]["detail"], "old error");
    assert_eq!(job["logs"]["edges"].as_array().unwrap().len(), 2);
    assert_eq!(job["logs"]["pageInfo"]["hasNextPage"], true);
    let cursor = job["logs"]["pageInfo"]["endCursor"].as_str().unwrap();
    let second: serde_json::Value = server
        .graphql(
            &serde_json::json!({ "query": LOOKUP, "variables": { "id": id, "after": cursor, "first": 2 } }),
            Some(&alice),
        )
        .await;
    assert_eq!(
        second["data"]["discover"]["logs"]["edges"][0]["node"]["line"],
        "third"
    );
    assert_eq!(
        second["data"]["discover"]["logs"]["pageInfo"]["hasNextPage"],
        false
    );

    let last_cursor = second["data"]["discover"]["logs"]["pageInfo"]["endCursor"]
        .as_str()
        .unwrap();
    let empty: serde_json::Value = server
        .graphql(
            &serde_json::json!({ "query": LOOKUP, "variables": { "id": id, "after": last_cursor } }),
            Some(&alice),
        )
        .await;
    assert!(
        empty["data"]["discover"]["logs"]["edges"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    sqlx::query(
        "UPDATE discovers SET job_status = '{\"type\":\"mergeFailed\"}'::jsonb WHERE id = $1",
    )
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("DELETE FROM draft_errors WHERE draft_id = $1")
        .bind(draft_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO draft_errors (draft_id, scope, detail) VALUES ($1, 'capture://aliceCo/logged', 'replacement error')")
        .bind(draft_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO internal.log_lines (token, stream, log_line, logged_at) SELECT logs_token, 'test', 'late line', now() FROM discovers WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    let later: serde_json::Value = server
        .graphql(
            &serde_json::json!({ "query": LOOKUP, "variables": { "id": id, "after": last_cursor } }),
            Some(&alice),
        )
        .await;
    assert_eq!(later["data"]["discover"]["status"], "mergeFailed");
    assert_eq!(
        later["data"]["discover"]["errors"][0]["detail"],
        "replacement error"
    );
    assert_eq!(
        later["data"]["discover"]["logs"]["edges"][0]["node"]["line"],
        "late line"
    );

    let invalid: serde_json::Value = server
        .graphql(
            &serde_json::json!({ "query": LOOKUP, "variables": { "id": id, "after": "invalid" } }),
            Some(&alice),
        )
        .await;
    assert!(invalid.get("errors").is_some());
    let negative: serde_json::Value = server
        .graphql(
            &serde_json::json!({ "query": LOOKUP, "variables": { "id": id, "first": -1 } }),
            Some(&alice),
        )
        .await;
    assert!(negative.get("errors").is_some());
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn policy_placement_and_invalid_models(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice, _) = setup(&pool).await;
    let cases = [
        ("aliceCo/absent-policy", "", false),
        ("aliceCo/null-policy", ",\"autoDiscover\":null", false),
        ("aliceCo/empty-policy", ",\"autoDiscover\":{}", true),
        (
            "aliceCo/enabled-policy",
            ",\"autoDiscover\":{\"addNewBindings\":true}",
            false,
        ),
    ];
    for (name, policy, expected_update_only) in cases {
        let model = format!(
            "{{\"endpoint\":{{\"connector\":{{\"image\":\"source/test:test\",\"config\":{{}}}}}},\"bindings\":[]{policy}}}"
        );
        stage_capture(&pool, draft_id, name, &model).await;
        let response = submit_without_plane_argument(&server, &alice, draft_id, name).await;
        assert!(response.get("errors").is_none(), "{name}: {response}");
        let id: models::Id =
            serde_json::from_value(response["data"]["createDiscover"]["id"].clone()).unwrap();
        let actual: bool = sqlx::query_scalar("SELECT update_only FROM discovers WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(actual, expected_update_only, "{name}");
    }

    // The nested mapping admits plane two; the parent mapping admits only one.
    stage_capture(
        &pool,
        draft_id,
        "aliceCo/private/explicit",
        r#"{"endpoint":{"connector":{"image":"source/test:test","config":{}}},"bindings":[]}"#,
    )
    .await;
    let allowed = submit(
        &server,
        Some(&alice),
        draft_id,
        "aliceCo/private/explicit",
        Some("ops/dp/public/gcp-us-central1-c2"),
    )
    .await;
    assert!(allowed.get("errors").is_none(), "{allowed}");
    assert_eq!(
        allowed["data"]["createDiscover"]["dataPlaneName"],
        "ops/dp/public/gcp-us-central1-c2"
    );

    let before: (i64, chrono::DateTime<chrono::Utc>) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM discovers WHERE draft_id = $1), updated_at FROM drafts WHERE id = $1",
    )
    .bind(draft_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let disallowed = submit(
        &server,
        Some(&alice),
        draft_id,
        "aliceCo/empty-policy",
        Some("ops/dp/public/gcp-us-central1-c2"),
    )
    .await;
    assert_eq!(
        disallowed["errors"][0]["message"],
        "data plane is not in the storage mapping"
    );
    let invalid_name = submit(&server, Some(&alice), draft_id, "invalid-name", None).await;
    assert!(
        invalid_name["errors"][0]["message"]
            .as_str()
            .unwrap()
            .starts_with("invalid catalog name")
    );

    for (name, spec_type, model) in [
        ("aliceCo/deleted", "capture", None),
        ("aliceCo/other-type", "collection", Some("{}")),
        ("aliceCo/malformed", "capture", Some("{}")),
        (
            "aliceCo/file-config",
            "capture",
            Some(
                r#"{"endpoint":{"connector":{"image":"source/test:test","config":"config.json"}},"bindings":[]}"#,
            ),
        ),
        (
            "aliceCo/failed-tag",
            "capture",
            Some(
                r#"{"endpoint":{"connector":{"image":"source/multi-tag-test:v2","config":{}}},"bindings":[]}"#,
            ),
        ),
    ] {
        sqlx::query("INSERT INTO draft_specs (draft_id, catalog_name, spec_type, spec) VALUES ($1, $2, $3::catalog_spec_type, $4::json)")
            .bind(draft_id)
            .bind(name)
            .bind(spec_type)
            .bind(model)
            .execute(&pool)
            .await
            .unwrap();
        let response = submit(&server, Some(&alice), draft_id, name, None).await;
        assert!(response.get("errors").is_some(), "{name}: {response}");
    }
    let after: (i64, chrono::DateTime<chrono::Utc>) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM discovers WHERE draft_id = $1), updated_at FROM drafts WHERE id = $1",
    )
    .bind(draft_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        after, before,
        "rejections leave jobs and draft time untouched"
    );
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn concurrent_stage_and_submission_do_not_deadlock(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice, _) = setup(&pool).await;
    stage_capture(
        &pool,
        draft_id,
        "aliceCo/concurrent",
        r#"{"endpoint":{"connector":{"image":"source/test:test","config":{}}},"bindings":[]}"#,
    )
    .await;

    // Match stageDraftSpecs' lock order: update the spec first, then touch
    // the draft. Submission waits for this row while holding its draft lock.
    let mut staged = pool.begin().await.unwrap();
    sqlx::query("UPDATE draft_specs SET detail = 'concurrent edit' WHERE draft_id = $1 AND catalog_name = 'aliceCo/concurrent'")
        .bind(draft_id)
        .execute(&mut *staged)
        .await
        .unwrap();

    let server = std::sync::Arc::new(server);
    let pending = {
        let server = server.clone();
        tokio::spawn(async move {
            submit_without_plane_argument(&server, &alice, draft_id, "aliceCo/concurrent").await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%FROM draft_specs%' AND pid <> pg_backend_pid())",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            if blocked {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        sqlx::query("UPDATE drafts SET updated_at = clock_timestamp() WHERE id = $1")
            .bind(draft_id)
            .execute(&mut *staged),
    )
    .await
    .expect("submission's draft lock must not block staging")
    .unwrap();
    staged.commit().await.unwrap();
    let response = tokio::time::timeout(std::time::Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap();
    assert!(response.get("errors").is_none(), "{response}");
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn rejects_plane_without_usable_signing_key(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    sqlx::query(
        "UPDATE data_planes SET hmac_keys = ARRAY['invalid-base64%%%'], encrypted_hmac_keys = '{}'::json WHERE data_plane_name = 'ops/dp/public/aws-us-west-2-c1'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let (server, draft_id, alice, _) = setup(&pool).await;
    stage_capture(
        &pool,
        draft_id,
        "aliceCo/no-signing-key",
        r#"{"endpoint":{"connector":{"image":"source/test:test","config":{}}},"bindings":[]}"#,
    )
    .await;
    let response =
        submit_without_plane_argument(&server, &alice, draft_id, "aliceCo/no-signing-key").await;
    assert_eq!(
        response["errors"][0]["message"],
        "data plane not found or unauthorized"
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM discovers WHERE draft_id = $1")
        .bind(draft_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn concurrent_insert_cannot_be_overwritten_by_live_copy(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    sqlx::query("UPDATE live_specs SET spec = $1::json WHERE catalog_name = 'aliceCo/in/capture-foo'")
        .bind(r#"{"endpoint":{"connector":{"image":"source/test:test","config":{"live":true}}},"bindings":[]}"#)
        .execute(&pool)
        .await
        .unwrap();
    let (server, draft_id, alice, _) = setup(&pool).await;
    let staged_model = r#"{"endpoint":{"connector":{"image":"source/test:test","config":{"staged":true}}},"bindings":[]}"#;
    let mut staged = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO draft_specs (draft_id, catalog_name, spec_type, spec) VALUES ($1, 'aliceCo/in/capture-foo', 'capture', $2::json)")
        .bind(draft_id)
        .bind(staged_model)
        .execute(&mut *staged)
        .await
        .unwrap();

    let server = std::sync::Arc::new(server);
    let pending = {
        let server = server.clone();
        tokio::spawn(async move {
            submit_without_plane_argument(&server, &alice, draft_id, "aliceCo/in/capture-foo").await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%INSERT INTO draft_specs%' AND pid <> pg_backend_pid())",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            if blocked {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    sqlx::query("UPDATE drafts SET updated_at = clock_timestamp() WHERE id = $1")
        .bind(draft_id)
        .execute(&mut *staged)
        .await
        .unwrap();
    staged.commit().await.unwrap();
    let response = tokio::time::timeout(std::time::Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        response["errors"][0]["message"],
        "capture was staged concurrently; retry"
    );
    let preserved: String = sqlx::query_scalar(
        "SELECT spec::text FROM draft_specs WHERE draft_id = $1 AND catalog_name = 'aliceCo/in/capture-foo'",
    )
    .bind(draft_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(preserved, staged_model);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM discovers WHERE draft_id = $1")
        .bind(draft_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}
