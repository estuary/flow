use crate::test_server;

const ALICE: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);
const BOB: uuid::Uuid = uuid::Uuid::from_bytes([0x22; 16]);
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
    sqlx::query(
        "INSERT INTO internal.log_lines (token, stream, log_line, logged_at) SELECT logs_token, 'test', 'late line', '2026-01-01T00:00:01Z' FROM discovers WHERE id = $1",
    )
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();
    let later = lookup(
        &server,
        Some(&alice),
        serde_json::json!({ "id": id, "after": after }),
    )
    .await;
    insta::assert_json_snapshot!(
        "discover_log_pages",
        serde_json::json!({
            "pages": pages,
            "afterCompletion": {
                "status": later["data"]["discover"]["status"],
                "logs": later["data"]["discover"]["logs"],
            },
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
