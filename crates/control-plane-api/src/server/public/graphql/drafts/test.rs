use super::*;
use crate::test_server;

const ALICE: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);
const BOB: uuid::Uuid = uuid::Uuid::from_bytes([0x22; 16]);
const EDITOR: uuid::Uuid = uuid::Uuid::from_bytes([0x33; 16]);

const DRAFT_QUERY: &str = r#"
    query Draft($id: Id!) {
      draft(id: $id) { id createdAt updatedAt detail numSpecs }
    }
"#;

const DRAFTS_QUERY: &str = r#"
    query Drafts($after: String, $first: Int) {
      drafts(after: $after, first: $first) {
        edges { cursor node { id createdAt updatedAt detail numSpecs } }
        pageInfo { hasPreviousPage hasNextPage startCursor endCursor }
      }
    }
"#;

const DRAFT_SPECS_QUERY: &str = r#"
    query DraftSpecs($id: Id!, $after: String, $first: Int) {
      draft(id: $id) {
        numSpecs
        specs(after: $after, first: $first) {
          edges {
            cursor
            node {
              catalogName specType spec expectPubId lastPubId detail
              updatedAt isUnchanged
            }
          }
          pageInfo { hasPreviousPage hasNextPage startCursor endCursor }
        }
      }
    }
"#;

const DRAFT_ERRORS_QUERY: &str = r#"
    query DraftErrors($id: Id!) {
      draft(id: $id) { errors { catalogName scope detail } }
    }
"#;

fn id(value: u64) -> models::Id {
    models::Id::new(value.to_be_bytes())
}

fn ts(value: &str) -> chrono::DateTime<chrono::Utc> {
    value.parse().expect("test timestamp")
}

/// Start a server over `pool` with an ungated authorization snapshot, so that
/// a denial is final rather than provoking the refresh-and-retry path.
async fn start(pool: &sqlx::PgPool) -> test_server::TestServer {
    let snapshot = test_server::snapshot(pool.clone(), false).await;
    test_server::TestServer::start(pool.clone(), snapshot).await
}

/// Mint an access token for one of the users seeded by the `alice` and
/// `drafts` fixtures, pairing each with the email it was seeded under.
fn token(server: &test_server::TestServer, user: uuid::Uuid) -> String {
    let email = match user {
        ALICE => "alice@example.com",
        BOB => "bob@example.test",
        EDITOR => "editor@example.test",
        _ => panic!("no fixture seeds user {user}"),
    };
    server.make_access_token(user, Some(email))
}

async fn insert_draft(
    pool: &sqlx::PgPool,
    id: models::Id,
    user_id: uuid::Uuid,
    detail: &str,
    created_at: chrono::DateTime<chrono::Utc>,
) {
    sqlx::query(
        r#"
        INSERT INTO drafts (id, user_id, detail, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $4)
        "#,
    )
    .bind(id)
    .bind(user_id)
    .bind(detail)
    .bind(created_at)
    .execute(pool)
    .await
    .unwrap();
}

/// Stage a bare draft spec, carrying a catalog name but no model. Enough for
/// the `numSpecs` count and for cascade behavior, neither of which reads the
/// model itself.
async fn insert_spec(
    pool: &sqlx::PgPool,
    id: models::Id,
    draft_id: models::Id,
    catalog_name: &str,
) {
    sqlx::query(
        r#"
        INSERT INTO draft_specs (id, draft_id, catalog_name)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(id)
    .bind(draft_id)
    .bind(catalog_name)
    .execute(pool)
    .await
    .unwrap();
}

/// The client-editable columns of a staged spec. Defaulted so that a fixture
/// names only the columns its scenario turns on, and a reader can see which
/// ones those are without counting argument positions.
#[derive(Default)]
struct StagedSpec {
    spec_type: Option<models::CatalogType>,
    spec: Option<serde_json::Value>,
    expect_pub_id: Option<models::Id>,
    detail: Option<&'static str>,
}

async fn insert_draft_spec(
    pool: &sqlx::PgPool,
    id: models::Id,
    draft_id: models::Id,
    catalog_name: &str,
    updated_at: chrono::DateTime<chrono::Utc>,
    staged: StagedSpec,
) {
    sqlx::query(
        r#"
        INSERT INTO draft_specs (
            id, draft_id, catalog_name, spec_type, spec, expect_pub_id,
            detail, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
        "#,
    )
    .bind(id)
    .bind(draft_id)
    .bind(catalog_name)
    .bind(staged.spec_type)
    .bind(staged.spec.map(sqlx::types::Json))
    .bind(staged.expect_pub_id)
    .bind(staged.detail)
    .bind(updated_at)
    .execute(pool)
    .await
    .unwrap();
}

#[test]
fn validates_page_size() {
    assert_eq!(page_size(None).unwrap(), 100);
    assert_eq!(page_size(Some(1)).unwrap(), 1);
    assert_eq!(page_size(Some(1000)).unwrap(), 1000);

    for invalid in [0, -1, 1001] {
        assert_eq!(
            page_size(Some(invalid)).unwrap_err().message,
            "first must be between 1 and 1000"
        );
    }
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn unauthenticated_queries_are_rejected(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let server = start(&pool).await;

    let response: serde_json::Value = server
        .graphql(&serde_json::json!({ "query": DRAFTS_QUERY }), None)
        .await;

    insta::assert_json_snapshot!("unauthenticated_drafts_query", response);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn drafts_page_in_id_order(pool: sqlx::PgPool) {
    let _guard = test_server::init();

    let first_id = id(0x10);
    let foreign_id = id(0x20);
    let second_id = id(0x30);
    let third_id = id(0x40);

    insert_draft(&pool, first_id, ALICE, "first", ts("2024-01-01T00:00:00Z")).await;
    insert_draft(
        &pool,
        foreign_id,
        BOB,
        "foreign",
        ts("2024-01-01T00:00:00Z"),
    )
    .await;
    insert_draft(
        &pool,
        second_id,
        ALICE,
        "second",
        ts("2024-01-02T00:00:00Z"),
    )
    .await;
    insert_draft(&pool, third_id, ALICE, "third", ts("2024-01-03T00:00:00Z")).await;
    insert_spec(&pool, id(0x101), first_id, "aliceCo/readable").await;
    insert_spec(&pool, id(0x102), first_id, "otherCo/not-readable").await;

    let server = start(&pool).await;
    let alice_token = token(&server, ALICE);

    let first_page: serde_json::Value = server
        .graphql(
            &serde_json::json!({ "query": DRAFTS_QUERY, "variables": { "first": 2 } }),
            Some(&alice_token),
        )
        .await;
    insta::assert_json_snapshot!("drafts_first_page", first_page);

    let after = first_page["data"]["drafts"]["pageInfo"]["endCursor"]
        .as_str()
        .expect("end cursor");
    let second_page: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": DRAFTS_QUERY,
                "variables": { "after": after, "first": 2 }
            }),
            Some(&alice_token),
        )
        .await;
    insta::assert_json_snapshot!("drafts_second_page", second_page);

    // Leaving `first` unset falls back to the default page size, and Bob's
    // draft stays absent from Alice's listing however the page is sized.
    let default_page: serde_json::Value = server
        .graphql(
            &serde_json::json!({ "query": DRAFTS_QUERY, "variables": {} }),
            Some(&alice_token),
        )
        .await;
    insta::assert_json_snapshot!("drafts_default_page", default_page);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn draft_lookup_counts_readable_specs(pool: sqlx::PgPool) {
    let _guard = test_server::init();

    let alice_id = id(0x10);
    let bob_id = id(0x20);
    let editor_id = id(0x50);
    let created_at = ts("2024-01-01T00:00:00Z");

    insert_draft(&pool, alice_id, ALICE, "alice", created_at).await;
    insert_draft(&pool, bob_id, BOB, "bob", created_at).await;
    insert_draft(&pool, editor_id, EDITOR, "editor", created_at).await;
    insert_spec(&pool, id(0x101), alice_id, "aliceCo/readable").await;
    insert_spec(&pool, id(0x102), alice_id, "otherCo/not-readable").await;
    insert_spec(&pool, id(0x103), bob_id, "bobCo/view-only").await;
    insert_spec(&pool, id(0x104), editor_id, "editorCo/editable").await;

    let server = start(&pool).await;

    // `numSpecs` counts only the names the caller may read, so the draft's own
    // size is not a channel for learning about catalog names it contains.
    let alice_draft: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": DRAFT_QUERY,
                "variables": { "id": alice_id.to_string() }
            }),
            Some(&token(&server, ALICE)),
        )
        .await;
    insta::assert_json_snapshot!("lookup_excludes_unreadable_specs", alice_draft);

    // A Viewer holds CatalogRead and no more, which is enough to count the
    // contents of a draft they own.
    let bob_draft: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": DRAFT_QUERY,
                "variables": { "id": bob_id.to_string() }
            }),
            Some(&token(&server, BOB)),
        )
        .await;
    insta::assert_json_snapshot!("lookup_as_viewer", bob_draft);

    // The Editor bundle supplies the same individual CatalogRead bit without
    // the legacy Admin capability.
    let editor_draft: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": DRAFT_QUERY,
                "variables": { "id": editor_id.to_string() }
            }),
            Some(&token(&server, EDITOR)),
        )
        .await;
    insta::assert_json_snapshot!("lookup_as_editor_bundle", editor_draft);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn draft_lookup_hides_foreign_and_unknown_ids(pool: sqlx::PgPool) {
    let _guard = test_server::init();

    let foreign_id = id(0x20);
    insert_draft(
        &pool,
        foreign_id,
        BOB,
        "foreign",
        ts("2024-01-01T00:00:00Z"),
    )
    .await;
    insert_spec(&pool, id(0x101), foreign_id, "bobCo/view-only").await;

    let server = start(&pool).await;
    let alice_token = token(&server, ALICE);

    // A draft's identity is private to its owner, and a foreign ID must be
    // indistinguishable from one that was never issued.
    let mut responses = Vec::new();
    for lookup_id in [foreign_id, id(0xffff)] {
        responses.push(
            server
                .graphql::<_, serde_json::Value>(
                    &serde_json::json!({
                        "query": DRAFT_QUERY,
                        "variables": { "id": lookup_id.to_string() }
                    }),
                    Some(&alice_token),
                )
                .await,
        );
    }
    assert_eq!(responses[0], responses[1]);

    insta::assert_json_snapshot!("lookup_foreign_and_unknown", responses);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn page_size_is_rejected_out_of_range(pool: sqlx::PgPool) {
    let _guard = test_server::init();

    let draft_id = id(0x10);
    insert_draft(&pool, draft_id, ALICE, "paged", ts("2024-01-01T00:00:00Z")).await;

    let server = start(&pool).await;
    let alice_token = token(&server, ALICE);

    // One out-of-range value is enough here: `validates_page_size` covers the
    // bounds directly, so what this adds is that the rejection reaches the
    // client as a GraphQL error against the `drafts` field.
    let out_of_range: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": DRAFTS_QUERY,
                "variables": { "first": 1001 }
            }),
            Some(&alice_token),
        )
        .await;
    insta::assert_json_snapshot!("drafts_invalid_page_size", out_of_range);

    // The nested spec connection validates `first` through the same path.
    let specs_response: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": DRAFT_SPECS_QUERY,
                "variables": { "id": draft_id.to_string(), "first": 0 }
            }),
            Some(&alice_token),
        )
        .await;
    insta::assert_json_snapshot!("draft_specs_invalid_page_size", specs_response);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn draft_specs_page_with_live_comparison(pool: sqlx::PgPool) {
    let _guard = test_server::init();

    let draft_id = id(0x60);
    insert_draft(
        &pool,
        draft_id,
        ALICE,
        "contents",
        ts("2024-02-01T00:00:00Z"),
    )
    .await;

    // The unchanged-spec comparison deliberately ignores the legacy
    // inferred-schema MD5 columns: inferred schema changes are now represented
    // in the stored catalog model itself. Seeding a divergent MD5 proves the
    // comparison does not regress to consulting it.
    sqlx::query(
        r#"
        INSERT INTO inferred_schemas (collection_name, schema, flow_document)
        VALUES ('aliceCo/data/foo', '{"type":"object"}', '{}')
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        UPDATE live_specs
        SET inferred_schema_md5 = 'the previously published inferred schema'
        WHERE catalog_name = 'aliceCo/data/foo'
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    // One spec of each shape the comparison must distinguish: identical to the
    // live spec, a pending catalog deletion, a changed model, and a name with
    // no live counterpart at all.
    insert_draft_spec(
        &pool,
        id(0x601),
        draft_id,
        "aliceCo/data/foo",
        ts("2024-02-02T01:00:00Z"),
        StagedSpec {
            spec_type: Some(models::CatalogType::Collection),
            spec: Some(serde_json::json!({})),
            expect_pub_id: Some(id(0x700)),
            detail: Some("same as live"),
        },
    )
    .await;
    insert_draft_spec(
        &pool,
        id(0x602),
        draft_id,
        "aliceCo/in/capture-foo",
        ts("2024-02-02T02:00:00Z"),
        StagedSpec {
            expect_pub_id: Some(models::Id::zero()),
            ..Default::default()
        },
    )
    .await;
    insert_draft_spec(
        &pool,
        id(0x603),
        draft_id,
        "aliceCo/out/materialize-bar",
        ts("2024-02-02T03:00:00Z"),
        StagedSpec {
            spec_type: Some(models::CatalogType::Materialization),
            spec: Some(serde_json::json!({
            "endpoint": {
            "connector": {
            "image": "example/materialize:test",
            "config": {
            "token": "ENC[AES256_GCM,data:invented,tag:invented,type:str]"
            }
            }
            },
            "bindings": []
            })),
            detail: Some("changed model"),
            ..Default::default()
        },
    )
    .await;
    insert_draft_spec(
        &pool,
        id(0x604),
        draft_id,
        "aliceCo/z-new",
        ts("2024-02-02T04:00:00Z"),
        StagedSpec {
            spec_type: Some(models::CatalogType::Collection),
            spec: Some(serde_json::json!({ "schema": { "type": "object" } })),
            ..Default::default()
        },
    )
    .await;
    insert_draft_spec(
        &pool,
        id(0x605),
        draft_id,
        "otherCo/hidden",
        ts("2024-02-02T04:00:00Z"),
        StagedSpec {
            spec_type: Some(models::CatalogType::Capture),
            spec: Some(serde_json::json!({ "secret": "must not leak" })),
            detail: Some("must not leak"),
            ..Default::default()
        },
    )
    .await;

    let server = start(&pool).await;
    let alice_token = token(&server, ALICE);

    let first_page: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": DRAFT_SPECS_QUERY,
                "variables": { "id": draft_id.to_string(), "first": 2 }
            }),
            Some(&alice_token),
        )
        .await;
    insta::assert_json_snapshot!("draft_specs_first_page", first_page);

    let after = first_page["data"]["draft"]["specs"]["pageInfo"]["endCursor"]
        .as_str()
        .expect("end cursor");
    let second_page: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": DRAFT_SPECS_QUERY,
                "variables": {
                    "id": draft_id.to_string(),
                    "after": after,
                    "first": 2
                }
            }),
            Some(&alice_token),
        )
        .await;
    insta::assert_json_snapshot!("draft_specs_second_page", second_page);

    let remaining_specs: i64 =
        sqlx::query_scalar("SELECT count(*) FROM draft_specs WHERE draft_id = $1")
            .bind(draft_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        remaining_specs, 5,
        "reading isUnchanged must not prune draft specs"
    );
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn draft_errors_filter_unreadable_scopes(pool: sqlx::PgPool) {
    let _guard = test_server::init();

    let draft_id = id(0x60);
    insert_draft(
        &pool,
        draft_id,
        ALICE,
        "diagnostics",
        ts("2024-02-01T00:00:00Z"),
    )
    .await;

    sqlx::query(
        r#"
        INSERT INTO draft_errors (draft_id, scope, detail) VALUES
          ($1, 'flow://collection/aliceCo/data/foo#/schema', 'readable diagnostic'),
          ($1, 'file:///tmp/catalog.yaml', 'global diagnostic'),
          ($1, 'flow://capture/otherCo/hidden', 'hidden diagnostic')
        "#,
    )
    .bind(draft_id)
    .execute(&pool)
    .await
    .unwrap();

    let server = start(&pool).await;

    // A diagnostic whose scope resolves to an unreadable catalog name is
    // dropped, while one that resolves to no catalog name at all stays visible
    // with an empty name rather than being attributed to a spec.
    let response: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": DRAFT_ERRORS_QUERY,
                "variables": { "id": draft_id.to_string() }
            }),
            Some(&token(&server, ALICE)),
        )
        .await;

    insta::assert_json_snapshot!("draft_errors_filtered", response);
}
