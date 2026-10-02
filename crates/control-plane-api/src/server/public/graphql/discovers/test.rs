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
    // Pair the shared partition fixtures with their recovery stores.
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
    // A future taken time makes authorization denials terminal throughout
    // these tests, which use a fixed watch.
    let snapshot = crate::Snapshot::new(
        tokens::now() + chrono::TimeDelta::hours(1),
        crate::snapshot::try_fetch(pool, &mut Default::default())
            .await
            .unwrap(),
    );
    let server = test_server::TestServer::start(
        pool.clone(),
        tokens::fixed(Ok(snapshot)).ready_owned().await,
    )
    .await;
    let alice = server.make_access_token(ALICE, Some("alice@example.com"));
    let bob = server.make_access_token(BOB, Some("bob@example.test"));
    (server, draft_id, alice, bob)
}

async fn insert_discover(pool: &sqlx::PgPool, draft_id: models::Id, name: &str) -> models::Id {
    sqlx::query_scalar(
        r#"
        INSERT INTO discovers (draft_id, capture_name, connector_tag_id, endpoint_config, data_plane_name)
        VALUES ($1, $2, '66:66:66:66:00:00:00:00', '{}', 'ops/dp/public/aws-us-west-2-c1')
        RETURNING id
        "#,
    )
    .bind(draft_id)
    .bind(name)
    .fetch_one(pool)
    .await
    .unwrap()
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

async fn wait_for_query_lock(pool: &sqlx::PgPool, pattern: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE $1 AND pid <> pg_backend_pid())",
            )
            .bind(pattern)
            .fetch_one(pool)
            .await
            .unwrap();
            if blocked {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("request reaches the controlled query lock");
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors")
    )
)]
async fn private_query_and_historical_status(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice, bob) = setup(&pool).await;
    let id = insert_discover(&pool, draft_id, "aliceCo/existing").await;

    let foreign: serde_json::Value = server
        .graphql(
            &serde_json::json!({ "query": LOOKUP, "variables": { "id": id } }),
            Some(&bob),
        )
        .await;
    assert!(foreign.get("errors").is_none(), "{foreign}");
    assert_eq!(foreign["data"]["discover"], serde_json::Value::Null);
    let owner: serde_json::Value = server
        .graphql(
            &serde_json::json!({ "query": LOOKUP, "variables": { "id": id } }),
            Some(&alice),
        )
        .await;
    assert!(owner.get("errors").is_none(), "{owner}");
    assert_eq!(owner["data"]["discover"]["id"], serde_json::json!(id));
    assert_eq!(owner["data"]["discover"]["status"], "QUEUED");
    let end_cursor = owner["data"]["discover"]["logs"]["pageInfo"]["endCursor"].as_str();
    assert!(end_cursor.is_none(), "a new discover has no log cursor");

    sqlx::query(
        r#"UPDATE discovers SET job_status = '{"type":"success","publication_id":"0123456789abcdef","specs_unchanged":true}' WHERE id = $1"#,
    )
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();
    let historical: serde_json::Value = server
        .graphql(
            &serde_json::json!({ "query": LOOKUP, "variables": { "id": id } }),
            Some(&alice),
        )
        .await;
    assert!(historical.get("errors").is_none(), "{historical}");
    assert_eq!(historical["data"]["discover"]["status"], "SUCCESS");

    let unauthenticated: serde_json::Value = server
        .graphql(
            &serde_json::json!({ "query": LOOKUP, "variables": { "id": id } }),
            None,
        )
        .await;
    assert!(unauthenticated.get("errors").is_some());
    insta::assert_json_snapshot!("discover_visibility_and_historical_status",
    [foreign, owner, historical, unauthenticated], {
        "[].data.discover.id" => "[id]",
        "[].data.discover.draftId" => "[draft-id]",
    });
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn staged_submission_and_ownership(pool: sqlx::PgPool) {
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
    insta::assert_json_snapshot!("staged_discover_submission", response, {
        ".data.createDiscover.id" => "[id]",
        ".data.createDiscover.draftId" => "[draft-id]",
        ".data.createDiscover.createdAt" => "[ts]",
        ".data.createDiscover.updatedAt" => "[ts]",
    });
    let discover = &response["data"]["createDiscover"];
    assert_eq!(discover["status"], "QUEUED");
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
    let unauthenticated = submit(&server, None, draft_id, "aliceCo/new-capture", None).await;
    assert!(unauthenticated.get("errors").is_some());
    insta::assert_json_snapshot!(
        "discover_submission_ownership",
        [
            ("foreign draft", foreign_create),
            ("missing draft", missing_create),
            ("unauthenticated", unauthenticated),
        ]
        .map(|(case, response)| serde_json::json!({
            "case": case,
            "message": response["errors"][0]["message"],
        }))
    );
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn copies_live_capture_and_preserves_draft_contents(pool: sqlx::PgPool) {
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
    sqlx::query("INSERT INTO draft_specs (draft_id, catalog_name, spec_type, spec, detail) VALUES ($1, 'aliceCo/unrelated', 'collection', '{}'::json, 'keep this entry')")
        .bind(draft_id).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO draft_errors (draft_id, scope, detail) VALUES ($1, 'collection://aliceCo/unrelated', 'existing error')")
        .bind(draft_id).execute(&pool).await.unwrap();
    let before = draft_state(&pool, draft_id).await;
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
    let after = draft_state(&pool, draft_id).await;
    let before_time: chrono::DateTime<chrono::Utc> =
        serde_json::from_value(before["draft"]["updated_at"].clone()).unwrap();
    let after_time: chrono::DateTime<chrono::Utc> =
        serde_json::from_value(after["draft"]["updated_at"].clone()).unwrap();
    assert!(after_time > before_time);
    assert_eq!(after["specs"][1], before["specs"][0]);
    assert_eq!(after["errors"], before["errors"]);
    assert_eq!(
        copied["data"]["createDiscover"]["errors"][0]["detail"],
        "existing error"
    );
    let compared: serde_json::Value = server.graphql(&serde_json::json!({
        "query": "query($id:Id!) { draft(id:$id) { specs { edges { node { catalogName isUnchanged } } } } }",
        "variables": {"id": draft_id}
    }), Some(&alice)).await;
    assert_eq!(
        compared["data"]["draft"]["specs"]["edges"][0]["node"]["isUnchanged"],
        true
    );

    let staged_model = r#"{"endpoint":{"connector":{"image":"source/test:test","config":{"draft":true}}},"bindings":[],"autoDiscover":{"addNewBindings":false}}"#;
    sqlx::query("UPDATE draft_specs SET spec = $1::json, detail = 'staged metadata' WHERE draft_id = $2 AND catalog_name = 'aliceCo/in/capture-foo'")
        .bind(staged_model)
        .bind(draft_id)
        .execute(&pool)
        .await
        .unwrap();
    let before_staged = draft_state(&pool, draft_id).await;
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

    let after_staged = draft_state(&pool, draft_id).await;
    for field in ["draft", "specs", "errors"] {
        assert_eq!(after_staged[field], before_staged[field], "{field}");
    }
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
    assert_eq!(draft_state(&pool, draft_id).await, after_staged);
    insta::assert_json_snapshot!(
        "live_capture_copy_and_rejections",
        serde_json::json!({
            "copiedPlane": copied["data"]["createDiscover"]["dataPlaneName"],
            "copiedErrors": copied["data"]["createDiscover"]["errors"],
            "stagedPlane": staged["data"]["createDiscover"]["dataPlaneName"],
            "missing": denied["errors"][0]["message"],
            "wrongPlane": wrong_plane["errors"][0]["message"],
        })
    );
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors")
    )
)]
async fn logs_paginate_and_arrive_after_completion(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice, _) = setup(&pool).await;
    let id = insert_discover(&pool, draft_id, "aliceCo/logged").await;
    sqlx::query(
        r#"
        INSERT INTO internal.log_lines (token, stream, log_line, logged_at)
        SELECT logs_token, 'test', line, ts::timestamptz FROM discovers,
        (VALUES ('first', '2026-01-01T00:00:00.000001Z'),
                ('second', '2026-01-01T00:00:00.000002Z'),
                ('third', '2026-01-01T00:00:00.000003Z'),
                ('fourth', '2026-01-01T00:00:00.000004Z'),
                ('fifth', '2026-01-01T00:00:00.000005Z')) AS lines(line, ts)
        WHERE id = $1
        "#,
    )
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(r#"
        WITH foreign_draft AS (
            INSERT INTO drafts (user_id) VALUES ($2) RETURNING id
        ), foreign_job AS (
            INSERT INTO discovers (draft_id, capture_name, connector_tag_id, endpoint_config, data_plane_name)
            SELECT fd.id, 'bobCo/capture', di.connector_tag_id, di.endpoint_config, di.data_plane_name
            FROM foreign_draft fd, discovers di WHERE di.id = $1
            RETURNING logs_token
        )
        INSERT INTO internal.log_lines (token, stream, log_line)
        SELECT logs_token, 'test', 'foreign job' FROM foreign_job
    "#).bind(id).bind(BOB).execute(&pool).await.unwrap();
    let mut pages = Vec::new();
    let mut last_cursor = None;
    let mut timestamps = Vec::<chrono::DateTime<chrono::Utc>>::new();
    for (expected_lines, has_next) in [
        (vec!["first", "second"], true),
        (vec!["third", "fourth"], true),
        (vec!["fifth"], false),
    ] {
        let page: serde_json::Value = server.graphql(
            &serde_json::json!({"query": LOOKUP, "variables": {"id": id, "after": last_cursor, "first": 2}}),
            Some(&alice),
        ).await;
        assert!(page.get("errors").is_none(), "{page}");
        let job = &page["data"]["discover"];
        let edges = job["logs"]["edges"].as_array().unwrap();
        assert_eq!(
            edges
                .iter()
                .map(|edge| edge["node"]["line"].as_str().unwrap())
                .collect::<Vec<_>>(),
            expected_lines
        );
        timestamps.extend(edges.iter().map(|edge| {
            edge["node"]["loggedAt"]
                .as_str()
                .unwrap()
                .parse::<chrono::DateTime<chrono::Utc>>()
                .unwrap()
        }));
        assert_eq!(job["logs"]["pageInfo"]["hasNextPage"], has_next);
        assert_eq!(
            job["logs"]["pageInfo"]["endCursor"],
            edges.last().unwrap()["cursor"]
        );
        last_cursor = Some(
            job["logs"]["pageInfo"]["endCursor"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
        pages.push(job["logs"].clone());
    }
    insta::assert_json_snapshot!("discover_log_pages", pages, {
        "[].edges[].cursor" => "[cursor]",
        "[].edges[].node.loggedAt" => "[ts]",
        "[].pageInfo.endCursor" => "[cursor]",
    });
    assert!(timestamps.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(
        timestamps
            .iter()
            .all(|ts| ts.timestamp_subsec_nanos() % 1_000 == 0)
    );
    assert_eq!(
        timestamps[2] - timestamps[0],
        chrono::Duration::microseconds(2)
    );

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
    insta::assert_json_snapshot!("discover_empty_log_page", empty["data"]["discover"]["logs"]);
    sqlx::query("UPDATE discovers SET job_status = '{\"type\":\"success\"}'::jsonb WHERE id = $1")
        .bind(id)
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
    assert_eq!(later["data"]["discover"]["status"], "SUCCESS");
    assert_eq!(
        later["data"]["discover"]["logs"]["edges"][0]["node"]["line"],
        "late line"
    );

    insta::assert_json_snapshot!("discover_logs_after_completion", serde_json::json!({
        "status": later["data"]["discover"]["status"],
        "logs": later["data"]["discover"]["logs"],
    }), {
        ".logs.edges[].cursor" => "[cursor]",
        ".logs.edges[].node.loggedAt" => "[ts]",
        ".logs.pageInfo.endCursor" => "[cursor]",
    });
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
    sqlx::query("UPDATE storage_mappings SET spec = jsonb_set(spec::jsonb, '{data_planes}', '[\"ops/dp/public/gcp-us-central1-c2\"]')::json WHERE catalog_prefix = 'recovery/aliceCo/'")
        .execute(&pool).await.unwrap();
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
        let stored: String = sqlx::query_scalar(
            "SELECT spec::text FROM draft_specs WHERE draft_id = $1 AND catalog_name = $2",
        )
        .bind(draft_id)
        .bind(name)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(stored, model);
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
        "storage mapping aliceCo/ doesn't permit data plane ops/dp/public/gcp-us-central1-c2"
    );
    let invalid_name = submit(&server, Some(&alice), draft_id, "invalid-name", None).await;
    assert!(
        invalid_name["errors"][0]["message"]
            .as_str()
            .unwrap()
            .starts_with("invalid catalog name")
    );

    let mut rejections = Vec::new();
    for (name, spec_type, model, expected_error) in [
        (
            "aliceCo/deleted",
            "capture",
            None,
            "draft entry is a deletion",
        ),
        (
            "aliceCo/other-type",
            "collection",
            Some("{}"),
            "draft entry is not a capture",
        ),
        (
            "aliceCo/malformed",
            "capture",
            Some("{}"),
            "invalid capture model",
        ),
        (
            "aliceCo/file-config",
            "capture",
            Some(
                r#"{"endpoint":{"connector":{"image":"source/test:test","config":"config.json"}},"bindings":[]}"#,
            ),
            "endpoint configuration must be an inline JSON object",
        ),
        (
            "aliceCo/failed-tag",
            "capture",
            Some(
                r#"{"endpoint":{"connector":{"image":"source/multi-tag-test:v2","config":{}}},"bindings":[]}"#,
            ),
            "capture connector tag is not ready",
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
        let before_rejection = draft_state(&pool, draft_id).await;
        let response = submit(&server, Some(&alice), draft_id, name, None).await;
        assert_eq!(
            response["errors"][0]["message"], expected_error,
            "{name}: {response}"
        );
        assert_eq!(
            draft_state(&pool, draft_id).await,
            before_rejection,
            "{name}"
        );
        rejections.push(serde_json::json!({
            "capture": name,
            "message": response["errors"][0]["message"],
        }));
    }
    insta::assert_json_snapshot!("discover_invalid_submissions", rejections);
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
    wait_for_query_lock(&pool, "%FROM draft_specs%").await;

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
    wait_for_query_lock(&pool, "%INSERT INTO draft_specs%").await;
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

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn submission_preserves_encrypted_endpoint_config(pool: sqlx::PgPool) {
    // The payload keys are deliberately unsorted: SOPS authenticates their order.
    let config = include_str!("testdata/unsorted-config.sops.json");
    let expected = models::RawValue::from_str(config).unwrap();
    let model = format!(
        r#"{{"endpoint":{{"connector":{{"image":"source/test:test","config":{config}}}}},"bindings":[]}}"#
    );
    sqlx::query(
        "UPDATE live_specs SET spec = $1::json WHERE catalog_name = 'aliceCo/in/capture-foo'",
    )
    .bind(&model)
    .execute(&pool)
    .await
    .unwrap();
    let (server, first_draft, alice, _) = setup(&pool).await;
    for copy_live in [false, true] {
        let draft_id = if copy_live {
            sqlx::query_scalar::<_, models::Id>(
                "INSERT INTO drafts (user_id) VALUES ($1) RETURNING id",
            )
            .bind(ALICE)
            .fetch_one(&pool)
            .await
            .unwrap()
        } else {
            first_draft
        };
        if !copy_live {
            let body = format!(
                r#"{{"query":"mutation($id:Id!, $model:JSON!) {{ stageDraftSpecs(draftId:$id, specs:[{{catalogName:\"aliceCo/in/capture-foo\",catalogType:capture,model:$model}}]) }}","variables":{{"id":"{draft_id}","model":{model}}}}}"#
            );
            let request = serde_json::value::RawValue::from_string(body).unwrap();
            let staged: serde_json::Value = server.graphql(&request, Some(&alice)).await;
            assert!(staged.get("errors").is_none(), "{staged}");
        }
        let response = submit(
            &server,
            Some(&alice),
            draft_id,
            "aliceCo/in/capture-foo",
            None,
        )
        .await;
        assert!(response.get("errors").is_none(), "{response}");
        let id: models::Id =
            serde_json::from_value(response["data"]["createDiscover"]["id"].clone()).unwrap();
        let persisted: String =
            sqlx::query_scalar("SELECT endpoint_config::text FROM discovers WHERE id = $1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(persisted.trim(), expected.get());
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), persisted).unwrap();
        let decrypted = std::process::Command::new("sops")
            .args(["--decrypt", "--input-type", "json", "--output-type", "json"])
            .arg(file.path())
            .output()
            .unwrap();
        assert!(
            decrypted.status.success(),
            "{}",
            String::from_utf8_lossy(&decrypted.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&decrypted.stdout).unwrap(),
            serde_json::json!({
                "zebra": "example-secret",
                "alpha": { "zulu": "nested-secret", "bravo": "example-value" }
            })
        );
    }
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn submission_rolls_back_after_copy_and_scheduling(pool: sqlx::PgPool) {
    sqlx::query(
        "UPDATE live_specs SET spec = $1::json WHERE catalog_name = 'aliceCo/in/capture-foo'",
    )
    .bind(r#"{"endpoint":{"connector":{"image":"source/test:test","config":{}}},"bindings":[]}"#)
    .execute(&pool)
    .await
    .unwrap();
    let (server, draft_id, alice, _) = setup(&pool).await;
    sqlx::query("INSERT INTO draft_specs (draft_id, catalog_name, spec_type, spec) VALUES ($1, 'aliceCo/unrelated', 'collection', '{}'::json)")
        .bind(draft_id).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO draft_errors (draft_id, scope, detail) VALUES ($1, 'collection://aliceCo/unrelated', 'retained')")
        .bind(draft_id).execute(&pool).await.unwrap();
    let before = draft_state(&pool, draft_id).await;
    sqlx::raw_sql(r#"
        CREATE FUNCTION fail_discover_submission() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NOT EXISTS (SELECT FROM draft_specs WHERE draft_id = NEW.draft_id AND catalog_name = NEW.capture_name) THEN
                RAISE EXCEPTION 'copy did not happen';
            END IF;
            IF NOT EXISTS (SELECT FROM internal.tasks WHERE task_id = NEW.id) THEN
                RAISE EXCEPTION 'job was not scheduled';
            END IF;
            RAISE EXCEPTION 'failure after copy and scheduling';
        END $$;
        CREATE TRIGGER z_fail_discover_submission AFTER INSERT ON discovers FOR EACH ROW EXECUTE FUNCTION fail_discover_submission();
    "#).execute(&pool).await.unwrap();
    let response = submit(
        &server,
        Some(&alice),
        draft_id,
        "aliceCo/in/capture-foo",
        None,
    )
    .await;
    assert!(
        response["errors"][0]["message"]
            .as_str()
            .unwrap()
            .contains("failure after copy and scheduling"),
        "{response}"
    );
    let after = draft_state(&pool, draft_id).await;
    assert_eq!(after, before);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn submission_enforces_token_capabilities(pool: sqlx::PgPool) {
    let model =
        r#"{"endpoint":{"connector":{"image":"source/test:test","config":{}}},"bindings":[]}"#;
    sqlx::query(
        "UPDATE live_specs SET spec = $1::json WHERE catalog_name = 'aliceCo/in/capture-foo'",
    )
    .bind(model)
    .execute(&pool)
    .await
    .unwrap();
    let (server, draft_id, _, _) = setup(&pool).await;
    for staged in [false, true] {
        if staged {
            stage_capture(&pool, draft_id, "aliceCo/in/capture-foo", model).await;
        }
        let before = draft_state(&pool, draft_id).await;
        for (mask, expected) in [
            ("viewer", "SpecEdit"),
            ("editor", "data plane not found or unauthorized"),
        ] {
            let token =
                server.make_restricted_access_token(ALICE, None, Some(vec![mask.to_owned()]), None);
            let denied = submit(
                &server,
                Some(&token),
                draft_id,
                "aliceCo/in/capture-foo",
                None,
            )
            .await;
            assert!(
                denied["errors"][0]["message"]
                    .as_str()
                    .unwrap()
                    .contains(expected),
                "{denied}"
            );
            assert_eq!(draft_state(&pool, draft_id).await, before);
        }
    }
    let token = server.make_restricted_access_token(
        ALICE,
        None,
        Some(vec!["editor".to_owned(), "viewer".to_owned()]),
        None,
    );
    let accepted = submit(
        &server,
        Some(&token),
        draft_id,
        "aliceCo/in/capture-foo",
        None,
    )
    .await;
    assert!(accepted.get("errors").is_none(), "{accepted}");
}

async fn draft_state(pool: &sqlx::PgPool, draft_id: models::Id) -> serde_json::Value {
    sqlx::query_scalar(r#"
        SELECT jsonb_build_object(
            'draft', (SELECT to_jsonb(d) FROM drafts d WHERE id = $1),
            'specs', (SELECT jsonb_agg(to_jsonb(s) ORDER BY catalog_name) FROM draft_specs s WHERE draft_id = $1),
            'errors', (SELECT jsonb_agg(to_jsonb(e) ORDER BY scope, detail) FROM draft_errors e WHERE draft_id = $1),
            'jobs', (SELECT jsonb_agg(to_jsonb(j) ORDER BY id) FROM discovers j WHERE draft_id = $1),
            'tasks', (SELECT jsonb_agg(to_jsonb(t) ORDER BY task_id) FROM internal.tasks t)
        )
    "#).bind(draft_id).fetch_one(pool).await.unwrap()
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors")
    )
)]
async fn log_page_arguments(pool: sqlx::PgPool) {
    let (server, draft_id, alice, _) = setup(&pool).await;
    let id = insert_discover(&pool, draft_id, "aliceCo/log-limit").await;
    sqlx::query(
        r#"INSERT INTO internal.log_lines (token, stream, log_line, logged_at)
           SELECT di.logs_token, 'test', n::text,
                  '2026-01-01T00:00:00Z'::timestamptz + n * INTERVAL '1 microsecond'
           FROM discovers di CROSS JOIN generate_series(1, 1001) n
           WHERE di.id = $1"#,
    )
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();

    for (first, expected) in [(None, 100), (Some(0), 0), (Some(1000), 1000)] {
        let page: serde_json::Value = server
            .graphql(
                &serde_json::json!({"query": LOOKUP, "variables": {"id": id, "first": first}}),
                Some(&alice),
            )
            .await;
        assert!(page.get("errors").is_none(), "{page}");
        let logs = &page["data"]["discover"]["logs"];
        let edges = logs["edges"].as_array().unwrap();
        assert_eq!(edges.len(), expected, "first: {first:?}");
        assert_eq!(logs["pageInfo"]["hasNextPage"], true);
        if let Some(last) = edges.last() {
            assert_eq!(last["node"]["line"], expected.to_string());
            assert_eq!(logs["pageInfo"]["endCursor"], last["cursor"]);
        } else {
            assert!(logs["pageInfo"]["endCursor"].is_null());
        }
    }
    let mut rejected = Vec::new();
    for first in [-1, 1001, i32::MAX] {
        let page: serde_json::Value = server
            .graphql(
                &serde_json::json!({"query": LOOKUP, "variables": {"id": id, "first": first}}),
                Some(&alice),
            )
            .await;
        assert!(page.get("errors").is_some());
        if first > 1000 {
            assert_eq!(page["errors"][0]["message"], "first cannot exceed 1000");
        }
        rejected.push(serde_json::json!({"first": first, "message": page["errors"][0]["message"]}));
    }
    let invalid: serde_json::Value = server
        .graphql(
            &serde_json::json!({"query": LOOKUP, "variables": {"id": id, "after": "invalid"}}),
            Some(&alice),
        )
        .await;
    assert!(invalid.get("errors").is_some());
    rejected
        .push(serde_json::json!({"after": "invalid", "message": invalid["errors"][0]["message"]}));
    insta::assert_json_snapshot!("discover_log_arguments", rejected);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn plane_gate_requests_refresh_without_waiting(pool: sqlx::PgPool) {
    let (_, draft_id, _, _) = setup(&pool).await;
    sqlx::query(
        "UPDATE live_specs SET spec = $1::json WHERE catalog_name = 'aliceCo/in/capture-foo'",
    )
    .bind(r#"{"endpoint":{"connector":{"image":"source/test:test","config":{}}},"bindings":[]}"#)
    .execute(&pool)
    .await
    .unwrap();
    let before = draft_state(&pool, draft_id).await;
    let mut rejected = Vec::new();
    for case in ["access", "snapshot lookup", "signing readiness"] {
        if case == "signing readiness" {
            sqlx::query("UPDATE data_planes SET hmac_keys = ARRAY['invalid-base64%%%'], encrypted_hmac_keys = '{}'::json WHERE data_plane_name = 'ops/dp/public/aws-us-west-2-c1'")
                .execute(&pool).await.unwrap();
        }
        for old in [true, false] {
            let mut data = crate::snapshot::try_fetch(&pool, &mut Default::default())
                .await
                .unwrap();
            if case == "access" {
                data.role_grants
                    .retain(|grant| grant.object_role.as_str() != "ops/dp/public/");
            } else if case == "snapshot lookup" {
                data.data_planes
                    .retain(|plane| plane.data_plane_name != "ops/dp/public/aws-us-west-2-c1");
            }
            let taken = if old {
                tokens::now() - chrono::TimeDelta::minutes(1)
            } else {
                tokens::now() + chrono::TimeDelta::hours(1)
            };
            let snapshot = crate::Snapshot::new(taken, data);
            let revoke = snapshot.revoke.clone();
            let server = test_server::TestServer::start(
                pool.clone(),
                tokens::fixed(Ok(snapshot)).ready_owned().await,
            )
            .await;
            let alice = server.make_access_token(ALICE, Some("alice@example.com"));
            let response = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                submit(
                    &server,
                    Some(&alice),
                    draft_id,
                    "aliceCo/in/capture-foo",
                    None,
                ),
            )
            .await
            .expect("plane rejection must not await a new snapshot");
            assert_eq!(
                response["errors"][0]["message"], "data plane not found or unauthorized",
                "{case}"
            );
            assert!(
                revoke.is_cancelled(),
                "{case} must request a background refresh"
            );
            assert_eq!(draft_state(&pool, draft_id).await, before, "{case}");
            rejected.push(serde_json::json!({
                "case": case,
                "snapshot": if old { "old" } else { "fresh" },
                "message": response["errors"][0]["message"],
            }));
        }
    }
    insta::assert_json_snapshot!("discover_plane_gate_refresh", rejected);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn authorization_wait_releases_connection_and_draft_lock(pool: sqlx::PgPool) {
    let (_, draft_id, _, _) = setup(&pool).await;
    let data = crate::snapshot::try_fetch(&pool, &mut Default::default())
        .await
        .unwrap();
    let mut stale = data.clone();
    stale.user_grants.retain(|grant| grant.user_id != ALICE);
    let stale = crate::Snapshot::new(tokens::now() - chrono::TimeDelta::minutes(1), stale);
    let revoke = stale.revoke.clone();
    let (pending, replace) = tokens::manual();
    _ = replace(Ok(stale));
    let watch = pending.ready_owned().await;
    let request_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let server = test_server::TestServer::start(request_pool.clone(), watch).await;
    let alice = server.make_access_token(ALICE, Some("alice@example.com"));
    let request = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none())
        .build().unwrap().post(server.base_url().join("/api/graphql").unwrap())
        .query(&[("retryAfter", (tokens::now() + chrono::TimeDelta::minutes(1)).to_rfc3339())])
        .bearer_auth(&alice)
        .json(&serde_json::json!({"query": CREATE, "variables": {"draftId": draft_id, "captureName": "aliceCo/new-capture"}}));
    let waiting = tokio::spawn(request.send());
    tokio::time::timeout(std::time::Duration::from_secs(5), revoke.cancelled())
        .await
        .expect("submission reaches the snapshot wait");
    assert!(!waiting.is_finished());

    // The pool connection and an exclusive draft lock remain available during the wait.
    let mut connection =
        tokio::time::timeout(std::time::Duration::from_secs(1), request_pool.acquire())
            .await
            .expect("authorization wait must release its connection")
            .unwrap();
    let mut txn = sqlx::Connection::begin(&mut *connection).await.unwrap();
    sqlx::query("SELECT id FROM drafts WHERE id = $1 FOR UPDATE NOWAIT")
        .bind(draft_id)
        .fetch_one(&mut *txn)
        .await
        .expect("authorization wait must not lock the draft");
    txn.commit().await.unwrap();
    drop(connection);
    _ = replace(Ok(crate::Snapshot::new(
        tokens::now() + chrono::TimeDelta::hours(1),
        data,
    )));
    let response = tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::TEMPORARY_REDIRECT);
    let jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM discovers WHERE draft_id = $1")
        .bind(draft_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(jobs, 0);
    drop(server);
    request_pool.close().await;
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn submission_rechecks_draft_ownership(pool: sqlx::PgPool) {
    let (server, draft_id, alice, _) = setup(&pool).await;
    stage_capture(
        &pool,
        draft_id,
        "aliceCo/concurrent-delete",
        r#"{"endpoint":{"connector":{"image":"source/test:test","config":{}}},"bindings":[]}"#,
    )
    .await;
    let before = draft_state(&pool, draft_id).await;
    let mut deletion = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM drafts WHERE id = $1 FOR UPDATE")
        .bind(draft_id)
        .fetch_one(&mut *deletion)
        .await
        .unwrap();

    let pending = tokio::spawn(async move {
        submit_without_plane_argument(&server, &alice, draft_id, "aliceCo/concurrent-delete").await
    });
    // Delete after the preflight succeeds, while submission waits for its
    // transactional ownership check, to exercise the ownership recheck.
    wait_for_query_lock(&pool, "%FROM drafts%FOR KEY SHARE%").await;
    sqlx::query("DELETE FROM drafts WHERE id = $1")
        .bind(draft_id)
        .execute(&mut *deletion)
        .await
        .unwrap();
    deletion.commit().await.unwrap();
    let response = tokio::time::timeout(std::time::Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response["errors"][0]["message"], "draft not found");
    insta::assert_json_snapshot!("discover_deleted_draft_during_submission", response);
    let after = draft_state(&pool, draft_id).await;
    assert!(after["jobs"].is_null());
    assert_eq!(after["tasks"], before["tasks"]);
}
