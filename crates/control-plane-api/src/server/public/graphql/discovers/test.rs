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

async fn setup(pool: &sqlx::PgPool) -> (test_server::TestServer, models::Id, String, String) {
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
