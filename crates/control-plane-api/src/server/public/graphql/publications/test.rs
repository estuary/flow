use crate::test_server;

const ALICE: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);
const BOB: uuid::Uuid = uuid::Uuid::from_bytes([0x22; 16]);
const CREATE: &str = r#"
mutation ($draftId: Id!, $dryRun: Boolean!, $defaultDataPlane: String, $detail: String) {
  createPublication(draftId: $draftId, dryRun: $dryRun, defaultDataPlane: $defaultDataPlane, detail: $detail) {
    id draftId dryRun pubId
    status { type lockFailures { catalogName expected actual } }
    errors { catalogName scope detail }
  }
}"#;
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
/// from `pool`. The Snapshot is fixed, so fixtures it carries (grants, live
/// specs, storage mappings) must be written before this is called. Returns
/// the Snapshot's revoke token, which a provisional denial cancels.
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
    let snapshot = crate::Snapshot::new(tokens::now() + chrono::TimeDelta::hours(1), data);
    let revoke = snapshot.revoke.clone();
    let server = test_server::TestServer::start(
        pool.clone(),
        tokens::fixed(Ok(snapshot)).ready_owned().await,
    )
    .await;
    let alice = server.make_access_token(ALICE, Some("alice@example.com"));
    (server, draft_id, alice, revoke)
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

/// Stage `model` under `name` in the draft, or a deletion when `model` is
/// None. A deletion stages no type: the loader resolves it from the live
/// spec, and errors when there is none.
async fn stage(
    pool: &sqlx::PgPool,
    draft_id: models::Id,
    name: &str,
    spec_type: &str,
    model: Option<&serde_json::Value>,
) {
    sqlx::query(
        "INSERT INTO draft_specs (draft_id, catalog_name, spec_type, spec) VALUES ($1, $2, $3::catalog_spec_type, $4)",
    )
    .bind(draft_id)
    .bind(name)
    .bind(model.map(|_| spec_type))
    .bind(model)
    .execute(pool)
    .await
    .unwrap();
}

/// Insert a live spec with the controller task its foreign key requires, in
/// data plane one. `flows` connects it to the given live names, which must
/// exist, as the executor's expansion follows `live_spec_flows`.
async fn insert_live_spec(
    pool: &sqlx::PgPool,
    id: &str,
    name: &str,
    spec_type: &str,
    model: &serde_json::Value,
    reads_from: &[&str],
    writes_to: &[&str],
) {
    sqlx::query(
        r#"
        WITH task AS (
            SELECT internal.create_task($1::flowid, 1::smallint, '000000000000'::flowid)
        )
        INSERT INTO live_specs (id, controller_task_id, catalog_name, spec_type, spec, built_spec, data_plane_id)
        SELECT $1::flowid, $1::flowid, $2, $3::catalog_spec_type, $4, '{}', '111111111111' FROM task
        "#,
    )
    .bind(id)
    .bind(name)
    .bind(spec_type)
    .bind(model)
    .execute(pool)
    .await
    .unwrap();
    for (source, target, flow_type) in reads_from
        .iter()
        .map(|source| (*source, name, spec_type))
        .chain(writes_to.iter().map(|target| (name, *target, spec_type)))
    {
        sqlx::query(
            r#"
            INSERT INTO live_spec_flows (source_id, target_id, flow_type)
            SELECT s.id, t.id, $3::flow_type FROM live_specs s, live_specs t
            WHERE s.catalog_name = $1 AND t.catalog_name = $2
            "#,
        )
        .bind(source)
        .bind(target)
        .bind(flow_type)
        .execute(pool)
        .await
        .unwrap();
    }
}

fn collection() -> serde_json::Value {
    serde_json::to_value(models::CollectionDef::example()).unwrap()
}

/// A materialization reading each of `sources`.
fn materialization(sources: &[&str]) -> serde_json::Value {
    let mut model = models::MaterializationDef::example();
    let binding = model.bindings.pop().unwrap();
    model.bindings = sources
        .iter()
        .map(|source| models::MaterializationBinding {
            source: models::Source::Collection(models::Collection::new(*source)),
            ..binding.clone()
        })
        .collect();
    serde_json::to_value(model).unwrap()
}

async fn grant_role(pool: &sqlx::PgPool, subject_role: &str, object_role: &str, capability: &str) {
    sqlx::query("INSERT INTO role_grants (subject_role, object_role, capability) VALUES ($1, $2, $3::grant_capability)")
        .bind(subject_role)
        .bind(object_role)
        .bind(capability)
        .execute(pool)
        .await
        .unwrap();
}

async fn grant_user(pool: &sqlx::PgPool, user_id: uuid::Uuid, object_role: &str, capability: &str) {
    sqlx::query("INSERT INTO user_grants (user_id, object_role, capability) VALUES ($1, $2, $3::grant_capability)")
        .bind(user_id)
        .bind(object_role)
        .bind(capability)
        .execute(pool)
        .await
        .unwrap();
}

/// Replace the draft's specs with `specs` and submit it.
async fn resubmit(
    pool: &sqlx::PgPool,
    server: &test_server::TestServer,
    token: &str,
    draft_id: models::Id,
    specs: &[(&str, &str, Option<&serde_json::Value>)],
) -> serde_json::Value {
    sqlx::query("DELETE FROM draft_specs WHERE draft_id = $1")
        .bind(draft_id)
        .execute(pool)
        .await
        .unwrap();
    for (name, spec_type, model) in specs {
        stage(pool, draft_id, name, spec_type, *model).await;
    }
    submit(
        server,
        Some(token),
        serde_json::json!({ "draftId": draft_id, "dryRun": false }),
    )
    .await
}

async fn submit(
    server: &test_server::TestServer,
    token: Option<&str>,
    variables: serde_json::Value,
) -> serde_json::Value {
    server
        .graphql(
            &serde_json::json!({ "query": CREATE, "variables": variables }),
            token,
        )
        .await
}

/// Everything a submission may write: the draft, its specs and errors, the
/// publications it queued, and the executor tasks they scheduled.
async fn draft_state(pool: &sqlx::PgPool, draft_id: models::Id) -> serde_json::Value {
    sqlx::query_scalar(r#"
        SELECT jsonb_build_object(
            'draft', (SELECT to_jsonb(d) FROM drafts d WHERE id = $1),
            'specs', (SELECT jsonb_agg(to_jsonb(s) ORDER BY catalog_name) FROM draft_specs s WHERE draft_id = $1),
            'errors', (SELECT jsonb_agg(to_jsonb(e) ORDER BY scope, detail) FROM draft_errors e WHERE draft_id = $1),
            'jobs', (SELECT jsonb_agg(to_jsonb(j) ORDER BY id) FROM publications j WHERE draft_id = $1),
            'tasks', (SELECT jsonb_agg(to_jsonb(t) ORDER BY task_id) FROM internal.tasks t WHERE t.task_type = 3)
        )
    "#).bind(draft_id).fetch_one(pool).await.unwrap()
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "storage_mappings")
    )
)]
async fn submission_and_ownership(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice, _) = setup(&pool).await;
    stage(
        &pool,
        draft_id,
        "aliceCo/data/new",
        "collection",
        Some(&collection()),
    )
    .await;

    let before = draft_state(&pool, draft_id).await;
    let response = submit(
        &server,
        Some(&alice),
        serde_json::json!({ "draftId": draft_id, "dryRun": false }),
    )
    .await;
    insta::assert_json_snapshot!("publication_submission", response, {
        ".data.createPublication.id" => "[id]",
        ".data.createPublication.draftId" => "[draft-id]",
    });
    let after = draft_state(&pool, draft_id).await;
    for field in ["draft", "specs", "errors"] {
        assert_eq!(after[field], before[field], "{field}");
    }
    // One row, owned by the caller, with its executor task scheduled.
    let (user_id, tasks): (uuid::Uuid, i64) = sqlx::query_as(
        r#"
        SELECT p.user_id, (SELECT count(*) FROM internal.tasks t WHERE t.task_id = p.id)
        FROM publications p WHERE p.draft_id = $1
        "#,
    )
    .bind(draft_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(user_id, ALICE);
    assert_eq!(tasks, 1, "submission schedules the executor");

    // A draft owned by someone else is indistinguishable from a missing one,
    // and an anonymous caller is refused outright.
    let foreign_draft_id = insert_draft(&pool, BOB).await;
    let missing_draft_id = models::Id::new(u64::MAX.to_be_bytes());
    let mut responses = Vec::new();
    for (token, draft_id) in [
        (Some(alice.as_str()), foreign_draft_id),
        (Some(alice.as_str()), missing_draft_id),
        (None, draft_id),
    ] {
        let response = submit(
            &server,
            token,
            serde_json::json!({ "draftId": draft_id, "dryRun": false }),
        )
        .await;
        responses.push(response["errors"][0]["message"].clone());
    }
    insta::assert_json_snapshot!("publication_submission_ownership", responses);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM publications")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(path = "../../../../fixtures", scripts("data_planes", "alice"))
)]
async fn private_query_and_historical_status(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice, _) = setup(&pool).await;
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
    let (server, draft_id, alice, _) = setup(&pool).await;
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
    let (server, draft_id, alice, _) = setup(&pool).await;
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
    let (server, draft_id, alice, _) = setup(&pool).await;
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

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "storage_mappings")
    )
)]
async fn spec_edit_required_on_every_drafted_name(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice, _) = setup(&pool).await;
    // Alice administers `aliceCo/` and holds nothing under `bobCo/`. The
    // deletion stages no model at all, so authorization has only its name.
    stage(
        &pool,
        draft_id,
        "aliceCo/data/new",
        "collection",
        Some(&collection()),
    )
    .await;
    stage(
        &pool,
        draft_id,
        "bobCo/data/alpha",
        "collection",
        Some(&collection()),
    )
    .await;
    stage(&pool, draft_id, "bobCo/data/zeta-gone", "collection", None).await;

    let before = draft_state(&pool, draft_id).await;
    let response = submit(
        &server,
        Some(&alice),
        serde_json::json!({ "draftId": draft_id, "dryRun": false }),
    )
    .await;
    // Of the two names Alice cannot edit, only the first is reported.
    insta::assert_json_snapshot!("publication_spec_edit_denied", response);
    assert_eq!(draft_state(&pool, draft_id).await, before);

    // Removing the editable name does not change the outcome, and removing
    // the first unauthorized name reports the deletion.
    sqlx::query("DELETE FROM draft_specs WHERE draft_id = $1 AND catalog_name IN ('aliceCo/data/new', 'bobCo/data/alpha')")
        .bind(draft_id)
        .execute(&pool)
        .await
        .unwrap();
    let response = submit(
        &server,
        Some(&alice),
        serde_json::json!({ "draftId": draft_id, "dryRun": false }),
    )
    .await;
    insta::assert_json_snapshot!("publication_spec_edit_denied_deletion", response);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM publications")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "storage_mappings")
    )
)]
async fn deletions_of_live_specs_are_admitted(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    // Two live materializations of Alice's read her collection. `aliceCo/out/`
    // is granted its source by the fixture and `aliceCo/other/` is not, so
    // the second is denied whenever a publication touches it. It is
    // connected to the collection only before the last case.
    insert_live_spec(
        &pool,
        "0000000000a1",
        "aliceCo/out/mat",
        "materialization",
        &materialization(&["aliceCo/data/foo"]),
        &["aliceCo/data/foo"],
        &[],
    )
    .await;
    insert_live_spec(
        &pool,
        "0000000000a2",
        "aliceCo/other/mat",
        "materialization",
        &materialization(&["aliceCo/data/foo"]),
        &[],
        &[],
    )
    .await;
    let (server, draft_id, alice, _) = setup(&pool).await;

    // A deletion stages no model and no type: admission resolves the type
    // from the live specification, and the draft row is left as staged.
    // Deleting a collection expands to its live readers, as the executor
    // does, so their grants decide the outcome.
    let mut outcomes = Vec::new();
    for (case, name, spec_type, connect_other) in [
        ("live task", "aliceCo/out/mat", "materialization", false),
        (
            "live collection with a granted reader",
            "aliceCo/data/foo",
            "collection",
            false,
        ),
        (
            "live collection with an ungranted reader",
            "aliceCo/data/foo",
            "collection",
            true,
        ),
    ] {
        if connect_other {
            sqlx::query(
                r#"
                INSERT INTO live_spec_flows (source_id, target_id, flow_type)
                SELECT s.id, t.id, 'materialization' FROM live_specs s, live_specs t
                WHERE s.catalog_name = 'aliceCo/data/foo' AND t.catalog_name = 'aliceCo/other/mat'
                "#,
            )
            .execute(&pool)
            .await
            .unwrap();
        }
        let response = resubmit(&pool, &server, &alice, draft_id, &[(name, spec_type, None)]).await;
        outcomes.push(serde_json::json!({
            "case": case,
            "status": response["data"]["createPublication"]["status"]["type"],
            "error": response["errors"][0]["message"],
        }));
    }
    insta::assert_json_snapshot!("publication_deletions", outcomes);

    let state = draft_state(&pool, draft_id).await;
    assert_eq!(state["specs"][0]["spec_type"], serde_json::Value::Null);
    assert_eq!(state["specs"][0]["spec"], serde_json::Value::Null);
    assert_eq!(state["jobs"].as_array().unwrap().len(), 2);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "storage_mappings")
    )
)]
async fn load_errors_follow_authorization(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice, _) = setup(&pool).await;
    // Two deletions of live specs which don't exist, and a model which
    // doesn't parse, all under Alice's tenant.
    stage(&pool, draft_id, "aliceCo/data/gone-b", "collection", None).await;
    stage(&pool, draft_id, "aliceCo/data/gone-a", "collection", None).await;
    stage(
        &pool,
        draft_id,
        "aliceCo/data/malformed",
        "collection",
        Some(&serde_json::json!({ "schema": 42 })),
    )
    .await;

    let structural = submit(
        &server,
        Some(&alice),
        serde_json::json!({ "draftId": draft_id, "dryRun": false }),
    )
    .await;

    // A name Alice cannot edit is denied before any load error is inspected,
    // whether or not its live spec exists, and the denial says nothing about
    // the errors of the names she can edit.
    stage(&pool, draft_id, "bobCo/data/gone", "collection", None).await;
    let denied_absent = submit(
        &server,
        Some(&alice),
        serde_json::json!({ "draftId": draft_id, "dryRun": false }),
    )
    .await;
    insert_live_spec(
        &pool,
        "0000000000a1",
        "bobCo/data/gone",
        "collection",
        &collection(),
        &[],
        &[],
    )
    .await;
    let denied_present = submit(
        &server,
        Some(&alice),
        serde_json::json!({ "draftId": draft_id, "dryRun": false }),
    )
    .await;

    insta::assert_json_snapshot!(
        "publication_load_errors",
        serde_json::json!({
            "structural": structural["errors"][0]["message"],
            "deniedAbsent": denied_absent["errors"][0]["message"],
            "deniedPresent": denied_present["errors"][0]["message"],
        })
    );
    let after = draft_state(&pool, draft_id).await;
    for field in ["errors", "jobs", "tasks"] {
        assert_eq!(after[field], serde_json::Value::Null, "{field}");
    }
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "storage_mappings")
    )
)]
async fn referenced_names_need_spec_grants(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    // Alice's tenant may read `aaaCo/data/` and the ops collections. She
    // reads `bobCo/readable/` herself, but nothing under `aliceCo/` is
    // granted to it, so a materialization of hers cannot.
    grant_role(&pool, "aliceCo/", "aaaCo/data/", "read").await;
    grant_role(&pool, "aliceCo/", "ops.us-central1.v1/", "read").await;
    grant_user(&pool, ALICE, "bobCo/readable/", "read").await;
    insert_live_spec(
        &pool,
        "0000000000a1",
        "aaaCo/data/present",
        "collection",
        &collection(),
        &[],
        &[],
    )
    .await;
    let (server, draft_id, alice, _) = setup(&pool).await;

    // Alice's own authority reaches whatever her specs may read, and every
    // staged name is checked for `SpecEdit` before any referenced name, so
    // no token restriction can make her reading of a referenced name the
    // first denial here. That rule is covered by the `authz` tests.
    let mut outcomes = Vec::new();
    for (case, sources) in [
        (
            "own, ops, and granted names present or not",
            &[
                "aliceCo/data/foo",
                "ops.us-central1.v1/logs",
                "aaaCo/data/present",
                "aaaCo/data/absent",
            ][..],
        ),
        (
            "readable by the user but not the spec",
            &["bobCo/readable/c"],
        ),
    ] {
        let response = resubmit(
            &pool,
            &server,
            &alice,
            draft_id,
            &[(
                "aliceCo/out/mat",
                "materialization",
                Some(&materialization(sources)),
            )],
        )
        .await;
        outcomes.push(serde_json::json!({
            "case": case,
            "status": response["data"]["createPublication"]["status"]["type"],
            "error": response["errors"][0]["message"],
        }));
    }
    insta::assert_json_snapshot!("publication_referenced_names", outcomes);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM publications")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "storage_mappings")
    )
)]
async fn expansion_touches_only_editable_tasks(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    // A live materialization of `bobCo/` reads Alice's collection, and Alice
    // administers `bobCo/` too. The materialization holds no grant to its
    // source, so it is denied whenever the publication touches it.
    grant_user(&pool, ALICE, "bobCo/", "admin").await;
    insert_live_spec(
        &pool,
        "0000000000b1",
        "bobCo/out/mat",
        "materialization",
        &materialization(&["aliceCo/data/foo"]),
        &["aliceCo/data/foo"],
        &[],
    )
    .await;
    let (server, draft_id, alice, _) = setup(&pool).await;
    let scoped =
        server.make_restricted_access_token(ALICE, None, None, Some("aliceCo/".to_owned()));
    stage(
        &pool,
        draft_id,
        "aliceCo/data/foo",
        "collection",
        Some(&collection()),
    )
    .await;

    // Scoped to `aliceCo/`, the token cannot edit the materialization, so
    // expansion leaves it out, as the executor's would. Unrestricted, Alice
    // can, so the publication touches it and its own grants are checked.
    let mut outcomes = Vec::new();
    for (case, token) in [("scoped to aliceCo/", &scoped), ("unrestricted", &alice)] {
        let response = submit(
            &server,
            Some(token),
            serde_json::json!({ "draftId": draft_id, "dryRun": false }),
        )
        .await;
        outcomes.push(serde_json::json!({
            "case": case,
            "status": response["data"]["createPublication"]["status"]["type"],
            "error": response["errors"][0]["message"],
        }));
    }
    insta::assert_json_snapshot!("publication_expansion", outcomes);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "storage_mappings")
    )
)]
async fn submission_enforces_token_capabilities(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, _, _) = setup(&pool).await;
    stage(
        &pool,
        draft_id,
        "aliceCo/data/new",
        "collection",
        Some(&collection()),
    )
    .await;

    // Alice's `admin` grant conveys `SpecEdit` only through a mask which
    // includes it, and only within a prefix scope which covers the name.
    let mut outcomes = Vec::new();
    for (case, mask, scope) in [
        ("viewer mask", Some(vec!["viewer"]), None),
        ("editor mask", Some(vec!["editor"]), None),
        ("scoped to aliceCo/in/", None, Some("aliceCo/in/")),
        ("scoped to aliceCo/data/", None, Some("aliceCo/data/")),
    ] {
        let token = server.make_restricted_access_token(
            ALICE,
            None,
            mask.map(|bundles| bundles.into_iter().map(str::to_owned).collect()),
            scope.map(str::to_owned),
        );
        let response = submit(
            &server,
            Some(&token),
            serde_json::json!({ "draftId": draft_id, "dryRun": true }),
        )
        .await;
        outcomes.push(serde_json::json!({
            "case": case,
            "status": response["data"]["createPublication"]["status"]["type"],
            "error": response["errors"][0]["message"],
        }));
    }
    insta::assert_json_snapshot!("publication_token_capabilities", outcomes);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "storage_mappings")
    )
)]
async fn arguments_are_recorded(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice, revoke) = setup(&pool).await;

    // An empty draft is queued: the executor records it as such. The data
    // plane is recorded as given, without consulting the Snapshot: an empty
    // name is omitted, and a plane which doesn't exist or which Alice cannot
    // read is the executor's to reject when it places created specs.
    for (dry_run, detail, plane) in [
        (true, Some("first"), Some("")),
        (false, None, None),
        (
            false,
            Some("unknown plane"),
            Some("ops/dp/public/not-a-plane"),
        ),
        (
            false,
            Some("private plane"),
            Some("ops/dp/private/bobCo2/aws-us-east-1-c1"),
        ),
    ] {
        let response = submit(
            &server,
            Some(&alice),
            serde_json::json!({
                "draftId": draft_id,
                "dryRun": dry_run,
                "detail": detail,
                "defaultDataPlane": plane,
            }),
        )
        .await;
        assert_eq!(
            response["data"]["createPublication"]["status"]["type"], "queued",
            "{response}"
        );
        assert_eq!(response["data"]["createPublication"]["dryRun"], dry_run);
    }
    assert!(!revoke.is_cancelled(), "no refresh is requested");

    let rows: Vec<(bool, Option<String>, Option<String>, String, Option<String>)> = sqlx::query_as(
        r#"
            SELECT dry_run, detail, data_plane_name, job_status->>'type', pub_id::text
            FROM publications WHERE draft_id = $1 ORDER BY id
            "#,
    )
    .bind(draft_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    // `pub_id` is the executor's to set on commit. Its `flowid` domain
    // would default it to a generated id if the insert left it out.
    assert!(rows.iter().all(|row| row.4.is_none()), "{rows:?}");
    insta::assert_debug_snapshot!("publication_arguments", rows);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "storage_mappings")
    )
)]
async fn stale_denials_redirect_until_a_fresh_snapshot(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    // Alice reads `bobCo/readable/` but her materializations cannot.
    grant_user(&pool, ALICE, "bobCo/readable/", "read").await;
    let draft_id = insert_draft(&pool, ALICE).await;
    let data = crate::snapshot::try_fetch(&pool, &mut Default::default())
        .await
        .unwrap();
    // A Snapshot taken before the request makes a denial provisional: the
    // grant may have been created since.
    let (pending, replace) = tokens::manual::<crate::Snapshot>();
    let stale = crate::Snapshot::new(tokens::now() - chrono::TimeDelta::minutes(1), data);
    let revoke = stale.revoke.clone();
    _ = replace(Ok(stale));
    let server = test_server::TestServer::start(pool.clone(), pending.ready_owned().await).await;
    let alice = server.make_access_token(ALICE, Some("alice@example.com"));
    let client = flow_client_next::rest::Client {
        base_url: server.base_url(),
        http_client: reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap(),
    };
    let cases = [
        ("spec edit", "bobCo/data/new", "collection", collection()),
        (
            "spec grant",
            "aliceCo/out/mat",
            "materialization",
            materialization(&["bobCo/readable/c"]),
        ),
    ];

    let mut outcomes = serde_json::Map::new();
    for (case, name, spec_type, model) in &cases {
        sqlx::query("DELETE FROM draft_specs WHERE draft_id = $1")
            .bind(draft_id)
            .execute(&pool)
            .await
            .unwrap();
        stage(&pool, draft_id, name, spec_type, Some(model)).await;
        let response = client
            .post(
                "/api/graphql",
                &serde_json::json!({
                    "query": CREATE,
                    "variables": { "draftId": draft_id, "dryRun": false }
                }),
                Some(&alice),
            )
            .send()
            .await
            .unwrap();
        outcomes.insert(format!("{case} stale"), response.status().as_u16().into());
    }
    assert!(
        revoke.is_cancelled(),
        "a provisional denial requests a refresh"
    );
    let state = draft_state(&pool, draft_id).await;
    assert_eq!(state["jobs"], serde_json::Value::Null);
    assert_eq!(state["tasks"], serde_json::Value::Null);

    // Alice is granted `bobCo/` and a Snapshot taken after that observes it:
    // the staged name is now hers to edit, while the materialization's own
    // grants are unchanged and its denial is terminal.
    grant_user(&pool, ALICE, "bobCo/", "admin").await;
    let data = crate::snapshot::try_fetch(&pool, &mut Default::default())
        .await
        .unwrap();
    let fresh = crate::Snapshot::new(tokens::now() + chrono::TimeDelta::hours(1), data);
    _ = replace(Ok(fresh));
    for (case, name, spec_type, model) in &cases {
        let response = resubmit(
            &pool,
            &server,
            &alice,
            draft_id,
            &[(name, spec_type, Some(model))],
        )
        .await;
        outcomes.insert(
            format!("{case} fresh"),
            serde_json::json!({
                "status": response["data"]["createPublication"]["status"]["type"],
                "error": response["errors"][0]["message"],
            }),
        );
    }
    insta::assert_json_snapshot!("publication_stale_denials", outcomes);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "storage_mappings")
    )
)]
async fn submission_fails_as_one_statement(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice, _) = setup(&pool).await;
    // The executor's task is scheduled by a trigger of the insert. A failure
    // there fails the insert with it.
    sqlx::raw_sql(
        r#"
        CREATE FUNCTION internal.test_fail() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'scheduling failed'; END $$;
        CREATE TRIGGER test_fail AFTER INSERT ON publications
        FOR EACH ROW EXECUTE FUNCTION internal.test_fail();
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    let response = submit(
        &server,
        Some(&alice),
        serde_json::json!({ "draftId": draft_id, "dryRun": false }),
    )
    .await;
    assert!(
        response["errors"][0]["message"]
            .as_str()
            .unwrap_or_else(|| panic!("{response}"))
            .contains("scheduling failed"),
        "{response}"
    );
    let state = draft_state(&pool, draft_id).await;
    assert_eq!(state["jobs"], serde_json::Value::Null);
    assert_eq!(state["tasks"], serde_json::Value::Null);
}
