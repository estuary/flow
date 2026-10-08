use crate::test_server;

const ALICE: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);
const BOB: uuid::Uuid = uuid::Uuid::from_bytes([0x22; 16]);
const LOOKUP: &str = r#"
query ($id: Id!, $after: String, $first: Int) {
  publication(id: $id) {
    id draftId dryRun pubId
    status { type lockFailures { catalogName expected actual } }
    errors { catalogName scope detail }
    logs(after: $after, first: $first) {
      edges { cursor node { loggedAt stream line } }
      pageInfo { hasNextPage endCursor }
    }
  }
}"#;

/// Insert a draft owned by Alice, then start a server over a Snapshot read
/// from `pool`.
async fn setup(pool: &sqlx::PgPool) -> (test_server::TestServer, models::Id, String) {
    let draft_id = insert_draft(pool, ALICE).await;
    let data = crate::snapshot::try_fetch(pool, &mut Default::default())
        .await
        .unwrap();
    let snapshot = crate::Snapshot::new(tokens::now() + chrono::TimeDelta::hours(1), data);
    let server = test_server::TestServer::start(
        pool.clone(),
        tokens::fixed(Ok(snapshot)).ready_owned().await,
    )
    .await;
    let alice = server.make_access_token(ALICE, Some("alice@example.com"));
    (server, draft_id, alice)
}

async fn insert_draft(pool: &sqlx::PgPool, user_id: uuid::Uuid) -> models::Id {
    sqlx::query_scalar("INSERT INTO drafts (user_id) VALUES ($1) RETURNING id")
        .bind(user_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Insert a publication row as the executor leaves it: `job_status` is its
/// JSON, and `pub_id` is set only by a committed publication.
async fn insert_publication(
    pool: &sqlx::PgPool,
    user_id: uuid::Uuid,
    draft_id: models::Id,
    dry_run: bool,
    job_status: &str,
    pub_id: Option<models::Id>,
) -> models::Id {
    sqlx::query_scalar(
        r#"
        INSERT INTO publications (user_id, draft_id, dry_run, job_status, pub_id)
        VALUES ($1, $2, $3, $4::jsonb, $5)
        RETURNING id
        "#,
    )
    .bind(user_id)
    .bind(draft_id)
    .bind(dry_run)
    .bind(job_status)
    .bind(pub_id)
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
    fixtures(path = "../../../../fixtures", scripts("data_planes", "alice"))
)]
async fn private_query_and_historical_status(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice) = setup(&pool).await;
    let bob = server.make_access_token(BOB, Some("bob@example.test"));

    // Statuses as the executor records them in the row: `JobStatus`
    // serializes camelCase but `LockFailure` does not. The lock failure
    // status carries its failures; a success carries its `pub_id`, for a
    // dry run as well.
    let statuses = [
        (false, r#"{"type":"queued"}"#, None),
        (
            false,
            r#"{"type":"success"}"#,
            Some(models::Id::new([7; 8])),
        ),
        (true, r#"{"type":"success"}"#, Some(models::Id::new([8; 8]))),
        (false, r#"{"type":"buildFailed"}"#, None),
        (
            false,
            r#"{"type":"buildIdLockFailure","lockFailures":[{"catalog_name":"aliceCo/data/foo","expected":"0101010101010101","actual":"0202020202020202"}]}"#,
            None,
        ),
        (false, r#"{"type":"emptyDraft"}"#, None),
    ];
    let mut responses = Vec::new();
    for (dry_run, job_status, pub_id) in statuses {
        let id = insert_publication(&pool, ALICE, draft_id, dry_run, job_status, pub_id).await;
        responses.push(lookup(&server, Some(&alice), serde_json::json!({ "id": id })).await);
    }
    insta::assert_json_snapshot!("publication_statuses", responses, {
        "[].data.publication.id" => "[id]",
        "[].data.publication.draftId" => "[draft-id]",
    });

    // Another user and an anonymous caller see nothing, and so does the owner
    // for an id which does not exist.
    let id = insert_publication(&pool, ALICE, draft_id, false, r#"{"type":"queued"}"#, None).await;
    let mut responses = Vec::new();
    for (token, id) in [
        (Some(bob.as_str()), id),
        (None, id),
        (Some(alice.as_str()), models::Id::new([9; 8])),
    ] {
        responses.push(lookup(&server, token, serde_json::json!({ "id": id })).await);
    }
    insta::assert_json_snapshot!("publication_visibility", responses);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(path = "../../../../fixtures", scripts("data_planes", "alice"))
)]
async fn errors_follow_the_draft(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice) = setup(&pool).await;
    let id = insert_publication(
        &pool,
        ALICE,
        draft_id,
        false,
        r#"{"type":"buildFailed"}"#,
        None,
    )
    .await;

    let errors = |pool: &sqlx::PgPool, rows: &'static [(&'static str, &'static str)]| {
        let pool = pool.clone();
        async move {
            sqlx::query("DELETE FROM draft_errors WHERE draft_id = $1")
                .bind(draft_id)
                .execute(&pool)
                .await
                .unwrap();
            let (scopes, details): (Vec<&str>, Vec<&str>) = rows.iter().copied().unzip();
            sqlx::query(
                r#"
                INSERT INTO draft_errors (draft_id, scope, detail)
                SELECT $1, scope, detail FROM unnest($2::text[], $3::text[]) AS rows(scope, detail)
                "#,
            )
            .bind(draft_id)
            .bind(scopes)
            .bind(details)
            .execute(&pool)
            .await
            .unwrap();
        }
    };

    // Errors are the draft's, ordered by scope then detail.
    errors(
        &pool,
        &[
            ("flow://materialization/aliceCo/out/mat", "second"),
            ("flow://capture/aliceCo/in/cap", "first"),
        ],
    )
    .await;
    let first = lookup(&server, Some(&alice), serde_json::json!({ "id": id })).await;

    // A later job on the same draft replaces them.
    errors(&pool, &[("flow://collection/aliceCo/data/foo", "later")]).await;
    let replaced = lookup(&server, Some(&alice), serde_json::json!({ "id": id })).await;

    // Deleting the draft, as a successful publication does, leaves the
    // publication visible with no errors.
    sqlx::query("DELETE FROM drafts WHERE id = $1")
        .bind(draft_id)
        .execute(&pool)
        .await
        .unwrap();
    let deleted = lookup(&server, Some(&alice), serde_json::json!({ "id": id })).await;

    insta::assert_json_snapshot!(
        "publication_errors",
        [
            first["data"]["publication"]["errors"].clone(),
            replaced["data"]["publication"]["errors"].clone(),
            deleted["data"]["publication"].clone(),
        ],
        { "[2].id" => "[id]", "[2].draftId" => "[draft-id]" }
    );
}

// The `drafts` fixture supplies Bob as a user, for the foreign job below.
#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn logs_paginate_and_arrive_after_completion(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice) = setup(&pool).await;
    let id = insert_publication(&pool, ALICE, draft_id, false, r#"{"type":"queued"}"#, None).await;
    let token: uuid::Uuid = sqlx::query_scalar("SELECT logs_token FROM publications WHERE id = $1")
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
    // Lines of another user's publication are never returned.
    sqlx::query(
        r#"
        WITH foreign_draft AS (
            INSERT INTO drafts (user_id) VALUES ($1) RETURNING id
        ), foreign_job AS (
            INSERT INTO publications (user_id, draft_id)
            SELECT $1, fd.id FROM foreign_draft fd
            RETURNING logs_token
        )
        INSERT INTO internal.log_lines (token, stream, log_line)
        SELECT logs_token, 'test', 'foreign job' FROM foreign_job
        "#,
    )
    .bind(BOB)
    .execute(&pool)
    .await
    .unwrap();

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
        let logs = page["data"]["publication"]["logs"].clone();
        after = logs["pageInfo"]["endCursor"].clone();
        pages.push(logs);
    }

    // Lines may arrive after the publication completes, and polling resumes
    // from the last cursor.
    sqlx::query(r#"UPDATE publications SET job_status = '{"type":"success"}' WHERE id = $1"#)
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

    // The writer stamps lines from its own clock, so check their order here
    // and redact them from the snapshot.
    let logged_at = pages
        .iter()
        .chain([&later["data"]["publication"]["logs"]])
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
    insta::assert_json_snapshot!(
        "publication_log_pages",
        serde_json::json!({
            "pages": pages,
            "afterCompletion": {
                "status": later["data"]["publication"]["status"],
                "logs": later["data"]["publication"]["logs"],
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
    fixtures(path = "../../../../fixtures", scripts("data_planes", "alice"))
)]
async fn log_page_arguments(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice) = setup(&pool).await;
    let id = insert_publication(&pool, ALICE, draft_id, false, r#"{"type":"queued"}"#, None).await;
    sqlx::query(
        r#"INSERT INTO internal.log_lines (token, stream, log_line, logged_at)
           SELECT p.logs_token, 'test', n::text,
                  '2026-01-01T00:00:00Z'::timestamptz + n * INTERVAL '1 microsecond'
           FROM publications p CROSS JOIN generate_series(1, 1001) n
           WHERE p.id = $1"#,
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
        let logs = &page["data"]["publication"]["logs"];
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
    insta::assert_json_snapshot!("publication_log_arguments", rejected);
}
