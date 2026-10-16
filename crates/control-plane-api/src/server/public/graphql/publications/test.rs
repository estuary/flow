use crate::test_server;

const ALICE: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);
const BOB: uuid::Uuid = uuid::Uuid::from_bytes([0x22; 16]);
const STATUS: &str = r#"
query ($id: Id!) {
  publication(id: $id) {
    status { type lockFailures { catalogName expected actual } }
    pubId
  }
}"#;
const LOGS: &str = r#"
query ($id: Id!) {
  publication(id: $id) {
    logs { edges { node { line } } }
  }
}"#;

/// Insert a draft owned by Alice, then start a server over a Snapshot read
/// from `pool`. Grants are read once, so write them before calling.
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
/// JSON, and `pub_id` is set only by a successful publication, dry run
/// included.
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
    query: &str,
    id: models::Id,
) -> serde_json::Value {
    server
        .graphql(
            &serde_json::json!({ "query": query, "variables": { "id": id } }),
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

    // `JobStatus` serializes camelCase, but `LockFailure` does not.
    let statuses = [
        (
            false,
            r#"{"type":"success"}"#,
            Some(models::Id::new([7; 8])),
        ),
        (true, r#"{"type":"success"}"#, Some(models::Id::new([8; 8]))),
        (
            false,
            r#"{"type":"buildIdLockFailure","lockFailures":[{"catalog_name":"aliceCo/data/foo","expected":"0101010101010101","actual":"0202020202020202"}]}"#,
            None,
        ),
    ];
    let mut responses = Vec::new();
    for (dry_run, job_status, pub_id) in statuses {
        let id = insert_publication(&pool, ALICE, draft_id, dry_run, job_status, pub_id).await;
        responses.push(lookup(&server, Some(&alice), STATUS, id).await);
    }
    insta::assert_json_snapshot!("publication_statuses", responses);

    let id: models::Id = sqlx::query_scalar(
        r#"
        INSERT INTO publications (user_id, draft_id, dry_run, job_status)
        VALUES ($1, $2, false, '{"type":"queued"}'::jsonb)
        RETURNING id
        "#,
    )
    .bind(ALICE)
    .bind(draft_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let generated: Option<String> =
        sqlx::query_scalar("SELECT pub_id::text FROM publications WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(generated.is_some(), "the column defaults to a generated id");
    let response = lookup(&server, Some(&alice), STATUS, id).await;
    assert_eq!(response["data"]["publication"]["status"]["type"], "queued");
    assert_eq!(
        response["data"]["publication"]["pubId"],
        serde_json::Value::Null
    );

    // Another user and a missing id both read as null. An anonymous caller is
    // refused.
    let id = insert_publication(&pool, ALICE, draft_id, false, r#"{"type":"queued"}"#, None).await;
    let mut responses = Vec::new();
    for (token, id) in [
        (Some(bob.as_str()), id),
        (None, id),
        (Some(alice.as_str()), models::Id::new([9; 8])),
    ] {
        responses.push(lookup(&server, token, STATUS, id).await);
    }
    insta::assert_json_snapshot!("publication_visibility", responses);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(path = "../../../../fixtures", scripts("data_planes", "alice"))
)]
async fn logs_come_from_the_publications_token(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice) = setup(&pool).await;
    let id = insert_publication(&pool, ALICE, draft_id, false, r#"{"type":"queued"}"#, None).await;
    let token: uuid::Uuid = sqlx::query_scalar("SELECT logs_token FROM publications WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();

    // The middle line is under a token that belongs to no publication.
    sqlx::query(
        r#"
        INSERT INTO internal.log_lines (token, stream, log_line, logged_at)
        VALUES ($1, 'test', 'first', '2026-01-01T00:00:00Z'),
               ($2, 'test', 'unrelated', '2026-01-01T00:00:01Z'),
               ($1, 'test', 'second', '2026-01-01T00:00:02Z')
        "#,
    )
    .bind(token)
    .bind(uuid::Uuid::new_v4())
    .execute(&pool)
    .await
    .unwrap();

    let response = lookup(&server, Some(&alice), LOGS, id).await;
    assert_eq!(
        response["data"]["publication"]["logs"],
        serde_json::json!({ "edges": [{ "node": { "line": "first" } }, { "node": { "line": "second" } }] }),
        "{response}"
    );
}
