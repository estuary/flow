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
const CREATE: &str = r#"
mutation ($draftId: Id!, $dryRun: Boolean!, $defaultDataPlane: String, $detail: String) {
  createPublication(draftId: $draftId, dryRun: $dryRun, defaultDataPlane: $defaultDataPlane, detail: $detail) {
    id draftId dryRun pubId
    status { type lockFailures { catalogName expected actual } }
    errors { catalogName scope detail }
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

/// Insert a live spec, with the controller task that `controller_task_id`
/// references, in data plane one, and a `live_spec_flows` row from each live
/// spec named in `reads_from`, which expansion follows. A name without a live
/// spec gets no row.
async fn insert_live_spec(
    pool: &sqlx::PgPool,
    id: &str,
    name: &str,
    spec_type: &str,
    model: &serde_json::Value,
    reads_from: &[&str],
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
    for source in reads_from {
        sqlx::query(
            r#"
            INSERT INTO live_spec_flows (source_id, target_id, flow_type)
            SELECT s.id, t.id, $3::flow_type FROM live_specs s, live_specs t
            WHERE s.catalog_name = $1 AND t.catalog_name = $2
            "#,
        )
        .bind(source)
        .bind(name)
        .bind(spec_type)
        .execute(pool)
        .await
        .unwrap();
    }
}

fn collection() -> serde_json::Value {
    serde_json::to_value(models::CollectionDef::example()).unwrap()
}

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

/// Everything a submission may write: the draft, its specs and errors, and
/// the publications queued for it.
async fn draft_state(pool: &sqlx::PgPool, draft_id: models::Id) -> serde_json::Value {
    sqlx::query_scalar(r#"
        SELECT jsonb_build_object(
            'draft', (SELECT to_jsonb(d) FROM drafts d WHERE id = $1),
            'specs', (SELECT jsonb_agg(to_jsonb(s) ORDER BY catalog_name) FROM draft_specs s WHERE draft_id = $1),
            'errors', (SELECT jsonb_agg(to_jsonb(e) ORDER BY scope, detail) FROM draft_errors e WHERE draft_id = $1),
            'jobs', (SELECT jsonb_agg(to_jsonb(j) ORDER BY id) FROM publications j WHERE draft_id = $1)
        )
    "#).bind(draft_id).fetch_one(pool).await.unwrap()
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

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "storage_mappings")
    )
)]
async fn submission_and_ownership(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice) = setup(&pool).await;
    stage(
        &pool,
        draft_id,
        "aliceCo/data/new",
        "collection",
        Some(&collection()),
    )
    .await;

    // `defaultDataPlane` is stored without consulting the Snapshot. A plane
    // which does not exist fails later, when the executor builds the
    // publication.
    let before = draft_state(&pool, draft_id).await;
    let response = submit(
        &server,
        Some(&alice),
        serde_json::json!({
            "draftId": draft_id,
            "dryRun": false,
            "detail": "first",
            "defaultDataPlane": "ops/dp/public/not-a-plane",
        }),
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
    let row: (uuid::Uuid, i64, Option<String>, Option<String>) = sqlx::query_as(
        r#"
        SELECT p.user_id, (SELECT count(*) FROM internal.tasks t WHERE t.task_id = p.id),
               p.detail, p.data_plane_name
        FROM publications p WHERE p.draft_id = $1
        "#,
    )
    .bind(draft_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        row,
        (
            ALICE,
            1,
            Some("first".to_owned()),
            Some("ops/dp/public/not-a-plane".to_owned()),
        )
    );

    // An empty draft is queued, and the executor records it as `emptyDraft`.
    let empty_draft_id = insert_draft(&pool, ALICE).await;
    let response = submit(
        &server,
        Some(&alice),
        serde_json::json!({ "draftId": empty_draft_id, "dryRun": false }),
    )
    .await;
    assert_eq!(
        response["data"]["createPublication"]["status"]["type"], "queued",
        "{response}"
    );

    // A draft owned by someone else is reported as missing, even when it
    // stages a name Alice may not edit. An anonymous caller is refused
    // outright.
    let foreign_draft_id = insert_draft(&pool, BOB).await;
    stage(
        &pool,
        foreign_draft_id,
        "bobCo/data/staged",
        "collection",
        Some(&collection()),
    )
    .await;
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
    assert_eq!(count, 2);
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
    let (server, draft_id, alice) = setup(&pool).await;
    // Alice administers `aliceCo/` and holds nothing under `bobCo/`. The
    // deletion stages no model at all, so authorization has only its name.
    stage(&pool, draft_id, "bobCo/data/zeta-gone", "collection", None).await;
    stage(
        &pool,
        draft_id,
        "bobCo/data/alpha",
        "collection",
        Some(&collection()),
    )
    .await;
    stage(
        &pool,
        draft_id,
        "aliceCo/data/new",
        "collection",
        Some(&collection()),
    )
    .await;
    let variables = serde_json::json!({ "draftId": draft_id, "dryRun": false });

    // Of the two names Alice cannot edit, the first in name order is
    // reported, though it was staged after the other.
    let first = submit(&server, Some(&alice), variables.clone()).await;

    // A lone deletion is denied the same way whether or not its live spec
    // exists.
    sqlx::query("DELETE FROM draft_specs WHERE draft_id = $1 AND catalog_name IN ('aliceCo/data/new', 'bobCo/data/alpha')")
        .bind(draft_id)
        .execute(&pool)
        .await
        .unwrap();
    let deletion_absent = submit(&server, Some(&alice), variables.clone()).await;
    insert_live_spec(
        &pool,
        "0000000000b1",
        "bobCo/data/zeta-gone",
        "collection",
        &collection(),
        &[],
    )
    .await;
    let deletion_present = submit(&server, Some(&alice), variables).await;

    insta::assert_json_snapshot!(
        "publication_spec_edit_denied",
        serde_json::json!({
            "first": first["errors"][0]["message"],
            "deletionAbsent": deletion_absent["errors"][0]["message"],
            "deletionPresent": deletion_present["errors"][0]["message"],
        })
    );
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "storage_mappings")
    )
)]
async fn deletions_of_live_specs_are_queued(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    // A live materialization of Alice's reads her collection.
    insert_live_spec(
        &pool,
        "0000000000a1",
        "aliceCo/out/mat",
        "materialization",
        &materialization(&["aliceCo/data/foo"]),
        &["aliceCo/data/foo"],
    )
    .await;
    let (server, draft_id, alice) = setup(&pool).await;

    // Submitting leaves a staged deletion's draft row as it is. Deleting the
    // collection expands to its live reader, as the executor does.
    let mut outcomes = Vec::new();
    for (case, name, spec_type) in [
        ("live task", "aliceCo/out/mat", "materialization"),
        (
            "live collection with a live reader",
            "aliceCo/data/foo",
            "collection",
        ),
    ] {
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
async fn expansion_is_authorized_against_the_token(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    // A live materialization of `bobCo/` reads Alice's collection, and Alice
    // administers `bobCo/` too, so expansion adds it to any publication of
    // the collection.
    grant_user(&pool, ALICE, "bobCo/", "admin").await;
    insert_live_spec(
        &pool,
        "0000000000b1",
        "bobCo/out/mat",
        "materialization",
        &materialization(&["aliceCo/data/foo"]),
        &["aliceCo/data/foo"],
    )
    .await;
    let (server, draft_id, alice) = setup(&pool).await;
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

    // Scoped to `aliceCo/`, the token cannot edit the materialization, so the
    // publication is denied. Unrestricted, it can.
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
    let (server, draft_id, alice) = setup(&pool).await;
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

    // A prefix scope bounds reads as well as edits. Scoped to `aliceCo/out/`,
    // the token may edit `aliceCo/out/mat` and read `aliceCo/data/` through
    // the role grant that `aliceCo/out/` holds, but not `aliceCo/other/`,
    // which only Alice's `admin` grant on `aliceCo/` covers. No grant covers
    // the ops collection, which is exempt. The materialization's own grants
    // are the executor's to check, so the unrestricted token is queued.
    let scoped =
        server.make_restricted_access_token(ALICE, None, None, Some("aliceCo/out/".to_owned()));
    for (case, token, sources) in [
        (
            "scoped to aliceCo/out/, reads aliceCo/data/ and ops",
            &scoped,
            &["aliceCo/data/foo", "ops.us-central1.v1/logs"][..],
        ),
        (
            "scoped to aliceCo/out/, reads aliceCo/other/",
            &scoped,
            &["aliceCo/other/foo"],
        ),
        (
            "unrestricted, reads aliceCo/other/",
            &alice,
            &["aliceCo/other/foo"],
        ),
    ] {
        let response = resubmit(
            &pool,
            &server,
            token,
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
    insta::assert_json_snapshot!("publication_token_capabilities", outcomes);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "storage_mappings")
    )
)]
async fn stale_denial_redirects(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let draft_id = insert_draft(&pool, ALICE).await;
    stage(
        &pool,
        draft_id,
        "bobCo/data/new",
        "collection",
        Some(&collection()),
    )
    .await;
    let data = crate::snapshot::try_fetch(&pool, &mut Default::default())
        .await
        .unwrap();
    // A Snapshot taken before the request makes a denial provisional: the
    // grant may have been created since.
    let stale = crate::Snapshot::new(tokens::now() - chrono::TimeDelta::minutes(1), data);
    let revoke = stale.revoke.clone();
    let server =
        test_server::TestServer::start(pool.clone(), tokens::fixed(Ok(stale)).ready_owned().await)
            .await;
    let alice = server.make_access_token(ALICE, Some("alice@example.com"));
    let client = flow_client_next::rest::Client {
        base_url: server.base_url(),
        http_client: reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap(),
    };

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
    assert_eq!(response.status(), reqwest::StatusCode::TEMPORARY_REDIRECT);
    assert!(
        revoke.is_cancelled(),
        "a provisional denial requests a refresh"
    );
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts", "storage_mappings")
    )
)]
async fn submission_is_all_or_nothing(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let (server, draft_id, alice) = setup(&pool).await;
    // The `create_publication_task` trigger creates the executor's task within
    // the publication insert's statement. Failing the task insert instead of
    // the publication insert checks that the publication row rolls back with
    // it. `task_type = 3` is the publications executor's task type.
    sqlx::raw_sql(
        r#"
        CREATE FUNCTION internal.test_fail() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'task creation failed'; END $$;
        CREATE TRIGGER test_fail BEFORE INSERT ON internal.tasks
        FOR EACH ROW WHEN (new.task_type = 3) EXECUTE FUNCTION internal.test_fail();
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
            .contains("task creation failed"),
        "{response}"
    );
    let state = draft_state(&pool, draft_id).await;
    assert_eq!(state["jobs"], serde_json::Value::Null, "no publication row");
}
