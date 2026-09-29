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
const MODEL: &str =
    r#"{"endpoint":{"connector":{"image":"source/test:test","config":{}}},"bindings":[]}"#;

/// Insert a draft owned by Alice, then start a server over a Snapshot read
/// from `pool`. The Snapshot is fixed, so fixtures it carries (grants, storage
/// mappings, data planes) must be written before this is called.
async fn setup(
    pool: &sqlx::PgPool,
) -> (
    test_server::TestServer,
    models::Id,
    String,
    tokens::CancellationToken,
) {
    let draft_id = insert_draft(pool, ALICE).await;
    let data = crate::snapshot::try_fetch(pool, &mut Default::default())
        .await
        .unwrap();
    // A Snapshot taken after every request makes authorization denials terminal.
    let (server, revoke) = start(pool, data, tokens::now() + chrono::TimeDelta::hours(1)).await;
    let alice = server.make_access_token(ALICE, Some("alice@example.com"));
    (server, draft_id, alice, revoke)
}

/// Serve `data` through a fixed watch, which never refreshes. Returns the
/// Snapshot's revoke token, which a submission cancels to request a refresh.
async fn start(
    pool: &sqlx::PgPool,
    data: crate::snapshot::SnapshotData,
    taken: tokens::DateTime,
) -> (test_server::TestServer, tokens::CancellationToken) {
    let snapshot = crate::Snapshot::new(taken, data);
    let revoke = snapshot.revoke.clone();
    let server = test_server::TestServer::start(
        pool.clone(),
        tokens::fixed(Ok(snapshot)).ready_owned().await,
    )
    .await;
    (server, revoke)
}

async fn insert_draft(pool: &sqlx::PgPool, user_id: uuid::Uuid) -> models::Id {
    sqlx::query_scalar("INSERT INTO drafts (user_id) VALUES ($1) RETURNING id")
        .bind(user_id)
        .fetch_one(pool)
        .await
        .unwrap()
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

/// `MODEL` with `fields` replacing its top-level fields.
fn model_with(fields: serde_json::Value) -> String {
    let serde_json::Value::Object(fields) = fields else {
        panic!("fields must be an object");
    };
    let mut model: serde_json::Value = serde_json::from_str(MODEL).unwrap();
    model.as_object_mut().unwrap().extend(fields);
    model.to_string()
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

async fn lookup(
    server: &test_server::TestServer,
    token: Option<&str>,
    variables: serde_json::Value,
) -> serde_json::Value {
    server
        .graphql(
            &serde_json::json!({ "query": LOOKUP, "variables": variables }),
            token,
        )
        .await
}

/// The `endpoint_config` and `update_only` which a successful submission queued.
async fn queued(pool: &sqlx::PgPool, response: &serde_json::Value) -> (String, bool) {
    let id: models::Id = serde_json::from_value(response["data"]["createDiscover"]["id"].clone())
        .unwrap_or_else(|err| panic!("{err}: {response}"));
    let (config, update_only): (String, bool) =
        sqlx::query_as("SELECT endpoint_config::text, update_only FROM discovers WHERE id = $1")
            .bind(id)
            .fetch_one(pool)
            .await
            .unwrap();
    (config.trim().to_owned(), update_only)
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
async fn lookup_is_private_to_the_draft_owner(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice, _) = setup(&pool).await;
    let bob = server.make_access_token(BOB, Some("bob@example.test"));
    let id = insert_discover(&pool, draft_id, "aliceCo/existing").await;

    let mut responses = Vec::new();
    for token in [Some(alice.as_str()), Some(bob.as_str()), None] {
        responses.push(lookup(&server, token, serde_json::json!({ "id": id })).await);
    }
    insta::assert_json_snapshot!("discover_visibility", responses, {
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
    let (server, draft_id, alice, _) = setup(&pool).await;
    stage_capture(&pool, draft_id, "aliceCo/new-capture", MODEL).await;

    let response = submit(&server, Some(&alice), draft_id, "aliceCo/new-capture", None).await;
    insta::assert_json_snapshot!("staged_discover_submission", response, {
        ".data.createDiscover.id" => "[id]",
        ".data.createDiscover.draftId" => "[draft-id]",
        ".data.createDiscover.createdAt" => "[ts]",
        ".data.createDiscover.updatedAt" => "[ts]",
    });
    let tasks: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM internal.tasks WHERE task_id IN (SELECT id FROM discovers WHERE draft_id = $1)",
    )
    .bind(draft_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(tasks, 1, "submission schedules the executor");

    // A draft owned by someone else is indistinguishable from a missing one.
    let foreign_draft_id = insert_draft(&pool, BOB).await;
    let missing_draft_id = models::Id::new(u64::MAX.to_be_bytes());
    for draft_id in [foreign_draft_id, missing_draft_id] {
        let response = submit(&server, Some(&alice), draft_id, "aliceCo/new-capture", None).await;
        assert_eq!(
            response["errors"][0]["message"], "draft not found",
            "{response}"
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
async fn submission_selects_staged_or_live_capture(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    // Keys are deliberately unsorted: SOPS authenticates an encrypted config by
    // walking it in order, so the queued config must preserve its bytes.
    const LIVE: &str = r#"{"zebra":"live","alpha":{"zulu":1,"bravo":2}}"#;
    const STAGED: &str = r#"{"zebra":"staged","alpha":{"zulu":1,"bravo":2}}"#;
    let model = |config: &str| {
        format!(
            r#"{{"endpoint":{{"connector":{{"image":"source/test:test","config":{config}}}}},"bindings":[]}}"#
        )
    };
    sqlx::query(
        "UPDATE live_specs SET spec = $1::json WHERE catalog_name = 'aliceCo/in/capture-foo'",
    )
    .bind(model(LIVE))
    .execute(&pool)
    .await
    .unwrap();
    let (server, draft_id, alice, _) = setup(&pool).await;
    sqlx::query("INSERT INTO draft_specs (draft_id, catalog_name, spec_type, spec) VALUES ($1, 'aliceCo/unrelated', 'collection', '{}'::json)")
        .bind(draft_id).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO draft_errors (draft_id, scope, detail) VALUES ($1, 'flow://collection/aliceCo/unrelated', 'existing error')")
        .bind(draft_id).execute(&pool).await.unwrap();

    // Without a staged entry, the readable live capture is used, and the draft
    // keeps its entries and errors.
    let before = draft_state(&pool, draft_id).await;
    let live = submit(
        &server,
        Some(&alice),
        draft_id,
        "aliceCo/in/capture-foo",
        None,
    )
    .await;
    let after = draft_state(&pool, draft_id).await;
    for field in ["draft", "specs", "errors"] {
        assert_eq!(after[field], before[field], "{field}");
    }
    // A staged entry takes precedence over the live capture.
    stage_capture(&pool, draft_id, "aliceCo/in/capture-foo", &model(STAGED)).await;
    let staged = submit(
        &server,
        Some(&alice),
        draft_id,
        "aliceCo/in/capture-foo",
        None,
    )
    .await;
    assert_eq!(queued(&pool, &live).await.0, LIVE);
    assert_eq!(queued(&pool, &staged).await.0, STAGED);

    let missing = submit(&server, Some(&alice), draft_id, "aliceCo/missing", None).await;
    let wrong_plane = submit(
        &server,
        Some(&alice),
        draft_id,
        "aliceCo/in/capture-foo",
        Some("ops/dp/public/gcp-us-central1-c2"),
    )
    .await;
    insta::assert_json_snapshot!(
        "live_capture_submission_and_rejections",
        serde_json::json!({
            "livePlane": live["data"]["createDiscover"]["dataPlaneName"],
            "stagedPlane": staged["data"]["createDiscover"]["dataPlaneName"],
            "missing": missing["errors"][0]["message"],
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
    let token: uuid::Uuid = sqlx::query_scalar("SELECT logs_token FROM discovers WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    // Queue the first batch before starting the writer so a page splits it.
    crate::logs::capture_lines(
        tx.clone(),
        "test".into(),
        token,
        &b"first\nsecond\nthird"[..],
    )
    .await
    .unwrap();
    let writer = tokio::spawn(crate::logs::serve_sink(pool.clone(), rx));
    let wait_pool = &pool;
    let wait_for_lines = |expected| async move {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let count: i64 =
                    sqlx::query_scalar("SELECT count(*) FROM internal.log_lines WHERE token = $1")
                        .bind(token)
                        .fetch_one(wait_pool)
                        .await
                        .unwrap();
                if count == expected {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    };
    wait_for_lines(3).await;
    crate::logs::capture_lines(tx.clone(), "test".into(), token, &b"fourth\nfifth"[..])
        .await
        .unwrap();
    wait_for_lines(5).await;
    // Lines of another user's discover are never returned.
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

    // Cursors keep the full precision of `logged_at`, so lines a microsecond
    // apart page without loss or repetition.
    let mut pages = Vec::new();
    let mut after = serde_json::Value::Null;
    for _ in 0..3 {
        let page = lookup(
            &server,
            Some(&alice),
            serde_json::json!({ "id": id, "after": after, "first": 2 }),
        )
        .await;
        assert!(page.get("errors").is_none(), "{page}");
        let logs = page["data"]["discover"]["logs"].clone();
        after = logs["pageInfo"]["endCursor"].clone();
        pages.push(logs);
    }

    // Lines may arrive after discovery completes, and polling resumes from the
    // last cursor.
    sqlx::query(r#"UPDATE discovers SET job_status = '{"type":"success"}' WHERE id = $1"#)
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    crate::logs::capture_lines(tx.clone(), "test".into(), token, &b"late line"[..])
        .await
        .unwrap();
    wait_for_lines(6).await;
    drop(tx);
    writer.await.unwrap().unwrap();
    let later = lookup(
        &server,
        Some(&alice),
        serde_json::json!({ "id": id, "after": after }),
    )
    .await;

    // The writer stamps lines from its own clock, so check their order here and
    // redact them from the snapshot.
    let logged_at = pages
        .iter()
        .chain([&later["data"]["discover"]["logs"]])
        .flat_map(|logs| logs["edges"].as_array().unwrap())
        .map(|edge| {
            edge["node"]["loggedAt"]
                .as_str()
                .unwrap()
                .parse::<chrono::DateTime<chrono::Utc>>()
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(logged_at.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(
        logged_at
            .iter()
            .all(|ts| ts.timestamp_subsec_nanos() % 1_000 == 0)
    );
    assert_eq!(
        logged_at[2] - logged_at[0],
        chrono::Duration::microseconds(2)
    );
    insta::assert_json_snapshot!(
        "discover_log_pages",
        serde_json::json!({
            "pages": pages,
            "afterCompletion": {
                "status": later["data"]["discover"]["status"],
                "logs": later["data"]["discover"]["logs"],
            },
        }),
        {
            ".pages[].edges[].cursor" => "[cursor]",
            ".pages[].edges[].node.loggedAt" => "[ts]",
            ".pages[].pageInfo.endCursor" => "[cursor]",
            ".afterCompletion.logs.edges[].cursor" => "[cursor]",
            ".afterCompletion.logs.edges[].node.loggedAt" => "[ts]",
            ".afterCompletion.logs.pageInfo.endCursor" => "[cursor]",
        }
    );
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn update_only_follows_auto_discover(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice, _) = setup(&pool).await;
    for (name, fields, update_only) in [
        ("aliceCo/absent-policy", serde_json::json!({}), false),
        (
            "aliceCo/null-policy",
            serde_json::json!({ "autoDiscover": null }),
            false,
        ),
        (
            "aliceCo/empty-policy",
            serde_json::json!({ "autoDiscover": {} }),
            true,
        ),
        (
            "aliceCo/enabled-policy",
            serde_json::json!({ "autoDiscover": { "addNewBindings": true } }),
            false,
        ),
    ] {
        stage_capture(&pool, draft_id, name, &model_with(fields)).await;
        let response = submit(&server, Some(&alice), draft_id, name, None).await;
        assert_eq!(queued(&pool, &response).await.1, update_only, "{name}");
    }
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn invalid_submissions_change_nothing(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice, _) = setup(&pool).await;
    let connector = |image: &str, config: serde_json::Value| {
        Some(model_with(serde_json::json!({
            "endpoint": { "connector": { "image": image, "config": config } }
        })))
    };
    let cases = [
        ("aliceCo/deleted", "capture", None),
        ("aliceCo/other-type", "collection", Some("{}".to_owned())),
        ("aliceCo/malformed", "capture", Some("{}".to_owned())),
        (
            "aliceCo/delete-flag",
            "capture",
            Some(model_with(serde_json::json!({ "delete": true }))),
        ),
        (
            "aliceCo/local",
            "capture",
            Some(model_with(serde_json::json!({
                "endpoint": { "local": { "command": ["true"], "config": {} } }
            }))),
        ),
        (
            "aliceCo/untagged",
            "capture",
            connector("source/test", serde_json::json!({})),
        ),
        (
            "aliceCo/file-config",
            "capture",
            connector("source/test:test", serde_json::json!("config.json")),
        ),
        (
            "aliceCo/failed-tag",
            "capture",
            connector("source/multi-tag-test:v2", serde_json::json!({})),
        ),
    ];
    for (name, spec_type, model) in &cases {
        sqlx::query("INSERT INTO draft_specs (draft_id, catalog_name, spec_type, spec) VALUES ($1, $2, $3::catalog_spec_type, $4::json)")
            .bind(draft_id)
            .bind(name)
            .bind(spec_type)
            .bind(model)
            .execute(&pool)
            .await
            .unwrap();
    }

    let before = draft_state(&pool, draft_id).await;
    let mut rejections = Vec::new();
    for (name, _, _) in cases {
        let response = submit(&server, Some(&alice), draft_id, name, None).await;
        rejections.push(serde_json::json!({
            "capture": name,
            "message": response["errors"][0]["message"],
        }));
    }
    insta::assert_json_snapshot!("discover_invalid_submissions", rejections);
    assert_eq!(draft_state(&pool, draft_id).await, before);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn new_captures_use_their_storage_mapping(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    sqlx::query(
        "INSERT INTO storage_mappings (catalog_prefix, spec) VALUES ('aliceCo/planeless/', '{}')",
    )
    .execute(&pool)
    .await
    .unwrap();
    // `carolCo/` has no storage mapping at all.
    sqlx::query("INSERT INTO user_grants (user_id, object_role, capability) VALUES ($1, 'carolCo/', 'admin')")
        .bind(ALICE).execute(&pool).await.unwrap();
    let (server, draft_id, alice, revoke) = setup(&pool).await;

    let mut outcomes = Vec::new();
    for (name, plane) in [
        ("aliceCo/parent-default", None),
        ("aliceCo/private/nested-default", None),
        (
            "aliceCo/private/nested-explicit",
            Some("ops/dp/public/gcp-us-central1-c2"),
        ),
        // The most specific mapping decides alone, though its parent admits this plane.
        (
            "aliceCo/private/parent-plane",
            Some("ops/dp/public/aws-us-west-2-c1"),
        ),
        ("aliceCo/planeless/capture", None),
        ("carolCo/unmapped", None),
    ] {
        stage_capture(&pool, draft_id, name, MODEL).await;
        let response = submit(&server, Some(&alice), draft_id, name, plane).await;
        outcomes.push(serde_json::json!({
            "capture": name,
            "dataPlane": response["data"]["createDiscover"]["dataPlaneName"],
            "error": response["errors"][0]["message"],
        }));
    }
    insta::assert_json_snapshot!("discover_storage_mapping_planes", outcomes);
    assert!(
        revoke.is_cancelled(),
        "mapping rejections request a Snapshot refresh"
    );
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn submission_enforces_token_capabilities(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    sqlx::query(
        "UPDATE live_specs SET spec = $1::json WHERE catalog_name = 'aliceCo/in/capture-foo'",
    )
    .bind(MODEL)
    .execute(&pool)
    .await
    .unwrap();
    let (server, draft_id, _, _) = setup(&pool).await;

    // Discovery needs SpecEdit on the capture and legacy `read` of its data
    // plane, which the editor bundle alone doesn't convey.
    let mut outcomes = Vec::new();
    for mask in [&["viewer"][..], &["editor"], &["editor", "viewer"]] {
        let token = server.make_restricted_access_token(
            ALICE,
            None,
            Some(mask.iter().map(|bundle| bundle.to_string()).collect()),
            None,
        );
        let response = submit(
            &server,
            Some(&token),
            draft_id,
            "aliceCo/in/capture-foo",
            None,
        )
        .await;
        outcomes.push(serde_json::json!({
            "mask": mask,
            "status": response["data"]["createDiscover"]["status"],
            "error": response["errors"][0]["message"],
        }));
    }
    insta::assert_json_snapshot!("discover_token_capabilities", outcomes);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn submission_requires_reading_binding_targets(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice, _) = setup(&pool).await;
    // Scoped to `aliceCo/in/`, a token can edit captures there and read the
    // `aliceCo/data/` collections they write, but not the rest of `aliceCo/`,
    // which Alice's own grant covers.
    let scoped =
        server.make_restricted_access_token(ALICE, None, None, Some("aliceCo/in/".to_owned()));

    let mut outcomes = Vec::new();
    for (name, target) in [
        ("aliceCo/in/readable", "aliceCo/data/foo"),
        ("aliceCo/in/unreadable", "aliceCo/other/foo"),
    ] {
        let bindings = serde_json::json!([{ "resource": { "id": "foo" }, "target": target }]);
        let model = model_with(serde_json::json!({ "bindings": bindings }));
        stage_capture(&pool, draft_id, name, &model).await;
        for (token, credential) in [("scoped", &scoped), ("owner", &alice)] {
            let response = submit(&server, Some(credential), draft_id, name, None).await;
            outcomes.push(serde_json::json!({
                "capture": name,
                "token": token,
                "status": response["data"]["createDiscover"]["status"],
                "error": response["errors"][0]["message"],
            }));
        }
    }
    insta::assert_json_snapshot!("discover_binding_target_authorization", outcomes);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors")
    )
)]
async fn log_page_arguments(pool: sqlx::PgPool) {
    let _guard = test_server::init();
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
        let page = lookup(
            &server,
            Some(&alice),
            serde_json::json!({ "id": id, "first": first }),
        )
        .await;
        let logs = &page["data"]["discover"]["logs"];
        let edges = logs["edges"].as_array().unwrap_or_else(|| panic!("{page}"));
        assert_eq!(edges.len(), expected, "first: {first:?}");
        assert_eq!(logs["pageInfo"]["hasNextPage"], true, "first: {first:?}");
        if let Some(last) = edges.last() {
            assert_eq!(last["node"]["line"], expected.to_string());
        }
    }
    let mut rejected = Vec::new();
    for (argument, value) in [
        ("first", serde_json::json!(-1)),
        ("first", serde_json::json!(1001)),
        ("after", serde_json::json!("invalid")),
    ] {
        let page = lookup(
            &server,
            Some(&alice),
            serde_json::json!({ "id": id, argument: value }),
        )
        .await;
        rejected
            .push(serde_json::json!({ argument: value, "message": page["errors"][0]["message"] }));
    }
    insta::assert_json_snapshot!("discover_log_arguments", rejected);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
    )
)]
async fn submission_redirects_stale_binding_and_data_plane_failures(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    sqlx::query(
        "UPDATE live_specs SET spec = $1::json WHERE catalog_name = 'aliceCo/in/capture-foo'",
    )
    .bind(model_with(serde_json::json!({
        "bindings": [{ "resource": {}, "target": "aliceCo/other/foo" }]
    })))
    .execute(&pool)
    .await
    .unwrap();
    let draft_id = insert_draft(&pool, ALICE).await;
    let data = crate::snapshot::try_fetch(&pool, &mut Default::default())
        .await
        .unwrap();
    let mut no_access = data.clone();
    let mut unlisted = data.clone();
    no_access
        .role_grants
        .retain(|grant| grant.object_role.as_str() != "ops/dp/public/");
    unlisted
        .data_planes
        .retain(|plane| plane.data_plane_name != "ops/dp/public/aws-us-west-2-c1");
    sqlx::query("UPDATE data_planes SET hmac_keys = ARRAY['invalid-base64%%%'], encrypted_hmac_keys = '{}'::json WHERE data_plane_name = 'ops/dp/public/aws-us-west-2-c1'")
        .execute(&pool)
        .await
        .unwrap();
    let unsigned = crate::snapshot::try_fetch(&pool, &mut Default::default())
        .await
        .unwrap();

    let http_client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();
    let before = draft_state(&pool, draft_id).await;

    for (case, data, scope) in [
        ("binding target", data, Some("aliceCo/in/".to_owned())),
        ("data plane permission", no_access, None),
        ("missing data plane", unlisted, None),
        ("invalid signing key", unsigned, None),
    ] {
        let (server, revoke) =
            start(&pool, data, tokens::now() - chrono::TimeDelta::minutes(1)).await;
        let alice =
            server.make_restricted_access_token(ALICE, Some("alice@example.com"), None, scope);
        let client = flow_client_next::rest::Client {
            base_url: server.base_url(),
            http_client: http_client.clone(),
        };
        let response = client
            .post(
                "/api/graphql",
                &serde_json::json!({
                    "query": CREATE,
                    "variables": { "draftId": draft_id, "captureName": "aliceCo/in/capture-foo" }
                }),
                Some(&alice),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            reqwest::StatusCode::TEMPORARY_REDIRECT,
            "{case}"
        );
        assert!(revoke.is_cancelled(), "{case} must request a refresh");
        assert_eq!(draft_state(&pool, draft_id).await, before, "{case}");
    }
}
