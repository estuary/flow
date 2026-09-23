use crate::test_server;

const ALICE: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);
const BOB: uuid::Uuid = uuid::Uuid::from_bytes([0x22; 16]);

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
              catalogName catalogType model expectPubId lastPubId detail
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

const CREATE_DRAFT_MUTATION: &str = r#"
    mutation Create($detail: String) {
      createDraft(detail: $detail) { id createdAt updatedAt detail numSpecs }
    }
"#;

const DELETE_DRAFT_MUTATION: &str = r#"
    mutation DeleteDraft($id: Id!) {
      deleteDraft(id: $id)
    }
"#;

const STAGE_SPECS_MUTATION: &str = r#"
    mutation Stage($draftId: Id!, $specs: [DraftSpecInput!]!) {
      stageDraftSpecs(draftId: $draftId, specs: $specs)
    }
"#;

const UNSTAGE_SPECS_MUTATION: &str = r#"
    mutation UnstageSpecs($draftId: Id!, $catalogNames: [Name!]!) {
      unstageDraftSpecs(draftId: $draftId, catalogNames: $catalogNames)
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

fn error_message(response: &serde_json::Value) -> &str {
    response["errors"][0]["message"]
        .as_str()
        .expect("GraphQL error message")
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn unauthenticated_operations_are_rejected(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let draft_id = id(0x80);
    insert_draft(
        &pool,
        draft_id,
        ALICE,
        "private",
        ts("2024-03-01T00:00:00Z"),
    )
    .await;
    insert_spec(&pool, id(0x801), draft_id, "aliceCo/private").await;
    let server = start(&pool).await;
    let mut errors = std::collections::BTreeMap::new();

    // Separate requests ensure a non-null field's error cannot skip another resolver.
    for (field, query, variables) in [
        ("draft", DRAFT_QUERY, serde_json::json!({ "id": draft_id })),
        ("drafts", DRAFTS_QUERY, serde_json::json!({})),
        ("createDraft", CREATE_DRAFT_MUTATION, serde_json::json!({})),
        (
            "deleteDraft",
            DELETE_DRAFT_MUTATION,
            serde_json::json!({ "id": draft_id }),
        ),
        (
            "stageDraftSpecs",
            STAGE_SPECS_MUTATION,
            serde_json::json!({
                "draftId": draft_id, "specs": [{ "catalogName": "aliceCo/new", "catalogType": "collection", "model": {} }]
            }),
        ),
        (
            "unstageDraftSpecs",
            UNSTAGE_SPECS_MUTATION,
            serde_json::json!({
                "draftId": draft_id, "catalogNames": ["aliceCo/private"]
            }),
        ),
    ] {
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({ "query": query, "variables": variables }),
                None,
            )
            .await;
        assert_eq!(response["errors"].as_array().unwrap().len(), 1, "{field}");
        errors.insert(field, error_message(&response).to_owned());
    }
    insta::assert_json_snapshot!("unauthenticated_operations", errors);
    let remaining: (i64, i64) =
        sqlx::query_as("SELECT (SELECT count(*) FROM drafts), (SELECT count(*) FROM draft_specs)")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(remaining, (1, 1));
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
                "variables": { "after": after }
            }),
            Some(&alice_token),
        )
        .await;
    insta::assert_json_snapshot!("drafts_second_page", second_page);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn draft_lookup_is_private_and_counts_all_staged_names(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let owned_id = id(0x10);
    let foreign_id = id(0x20);
    let created_at = ts("2024-01-01T00:00:00Z");
    insert_draft(&pool, owned_id, BOB, "owned", created_at).await;
    insert_draft(&pool, foreign_id, ALICE, "foreign", created_at).await;
    insert_spec(&pool, id(0x101), owned_id, "bobCo/readable").await;
    insert_spec(&pool, id(0x102), owned_id, "otherCo/outside-grants").await;
    insert_spec(&pool, id(0x103), foreign_id, "bobCo/readable").await;
    let server = start(&pool).await;
    let bob_token = token(&server, BOB);
    let mut responses = Vec::new();
    for lookup_id in [owned_id, foreign_id, id(0xffff)] {
        responses.push(
            server
                .graphql::<_, serde_json::Value>(
                    &serde_json::json!({ "query": DRAFT_QUERY, "variables": { "id": lookup_id } }),
                    Some(&bob_token),
                )
                .await,
        );
    }
    assert_eq!(
        responses[1], responses[2],
        "foreign and missing IDs must be indistinguishable"
    );
    insta::assert_json_snapshot!("draft_lookup", &responses[..2]);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn negative_page_sizes_are_rejected(pool: sqlx::PgPool) {
    let _guard = test_server::init();

    let draft_id = id(0x10);
    insert_draft(&pool, draft_id, ALICE, "paged", ts("2024-01-01T00:00:00Z")).await;

    let server = start(&pool).await;
    let alice_token = token(&server, ALICE);

    let negative: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": DRAFTS_QUERY,
                "variables": { "first": -1 }
            }),
            Some(&alice_token),
        )
        .await;
    insta::assert_json_snapshot!("drafts_invalid_page_size", negative);

    // The nested spec connection validates `first` through the same path.
    let specs_response: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": DRAFT_SPECS_QUERY,
                "variables": { "id": draft_id.to_string(), "first": -1 }
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

    let other_draft_id = id(0x61);
    insert_draft(
        &pool,
        other_draft_id,
        ALICE,
        "another owned draft",
        ts("2024-02-01T00:00:00Z"),
    )
    .await;
    insert_spec(&pool, id(0x610), other_draft_id, "aliceCo/data/foo").await;

    // Different live comparisons and grants, inserted out of catalog-name order.
    sqlx::query(
        r#"
        INSERT INTO draft_specs (draft_id, catalog_name, spec_type, spec, expect_pub_id, detail, updated_at) VALUES
          ($1, 'aliceCo/z-new', 'collection', '{"new":true}', null, null, '2024-02-02T04:00:00Z'),
          ($1, 'aliceCo/in/capture-foo', null, null, '0000000000000000', null, '2024-02-02T02:00:00Z'),
          ($1, 'otherCo/hidden', 'capture', '{"staged":"outside-grant"}', null, 'owned staged change', '2024-02-02T04:00:00Z'),
          ($1, 'aliceCo/data/foo', 'collection', '{}', '0000000000000700', 'same as live', '2024-02-02T01:00:00Z'),
          ($1, 'aliceCo/out/materialize-bar', 'materialization', '{"changed":true}', null, 'changed model', '2024-02-02T03:00:00Z')
        "#,
    )
    .bind(draft_id)
    .execute(&pool)
    .await
    .unwrap();

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
                    "first": 3
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
async fn draft_errors_are_visible_to_owner(pool: sqlx::PgPool) {
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

    let other_draft_id = id(0x61);
    insert_draft(
        &pool,
        other_draft_id,
        ALICE,
        "other diagnostics",
        ts("2024-02-01T00:00:00Z"),
    )
    .await;

    sqlx::query(
        r#"
        INSERT INTO draft_errors (draft_id, scope, detail) VALUES
          ($1, 'flow://collection/aliceCo/data/foo#/schema', 'readable diagnostic'),
          ($1, 'flow://capture/otherCo/hidden', 'outside-grant diagnostic'),
          ($1, 'flow://deletion/otherCo/hidden', 'deletion diagnostic'),
          ($1, 'otherCo/bare', 'bare-name diagnostic'),
          ($1, 'file:///tmp/catalog.yaml', 'global diagnostic'),
          ($1, 'flow://unauthorized/otherCo/build', 'publication diagnostic'),
          ($2, 'flow://collection/aliceCo/data/foo#/schema', 'another draft diagnostic')
        "#,
    )
    .bind(draft_id)
    .bind(other_draft_id)
    .execute(&pool)
    .await
    .unwrap();

    let server = start(&pool).await;

    // Every diagnostic belongs to the owner, including synthetic deletion scopes,
    // bare names, and global scopes that do not resolve to a catalog name.
    let response: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": DRAFT_ERRORS_QUERY,
                "variables": { "id": draft_id.to_string() }
            }),
            Some(&token(&server, ALICE)),
        )
        .await;

    insta::assert_json_snapshot!("draft_errors_visible_to_owner", response);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn create_draft_needs_no_catalog_edit_grant(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let server = start(&pool).await;
    let created: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": CREATE_DRAFT_MUTATION,
                "variables": { "detail": "created through GraphQL" }
            }),
            Some(&token(&server, BOB)),
        )
        .await;
    insta::assert_json_snapshot!("create_draft", created, {
        ".data.createDraft.id" => "[id]",
        ".data.createDraft.createdAt" => "[ts]",
        ".data.createDraft.updatedAt" => "[ts]",
    });
    let created_id = created["data"]["createDraft"]["id"]
        .as_str()
        .unwrap()
        .parse::<models::Id>()
        .unwrap();
    let stored: (uuid::Uuid, String) =
        sqlx::query_as("SELECT user_id, detail FROM drafts WHERE id = $1")
            .bind(created_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stored, (BOB, "created through GraphQL".to_owned()));
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn delete_draft_needs_only_ownership(pool: sqlx::PgPool) {
    let _guard = test_server::init();

    let bob_id = id(0x20);
    let created_at = ts("2024-01-01T00:00:00Z");

    insert_draft(&pool, bob_id, BOB, "bob", created_at).await;
    insert_spec(&pool, id(0x101), bob_id, "bobCo/view-only").await;
    insert_spec(&pool, id(0x103), bob_id, "otherCo/not-readable").await;

    let server = start(&pool).await;
    let alice_token = token(&server, ALICE);

    let delete = |delete_id: models::Id| {
        serde_json::json!({
            "query": DELETE_DRAFT_MUTATION,
            "variables": { "id": delete_id.to_string() }
        })
    };

    let foreign: serde_json::Value = server.graphql(&delete(bob_id), Some(&alice_token)).await;
    let missing: serde_json::Value = server
        .graphql(&delete(id(0xffff)), Some(&alice_token))
        .await;
    assert_eq!(
        foreign, missing,
        "foreign and missing drafts must not be distinguishable"
    );
    insta::assert_json_snapshot!("delete_foreign_draft", foreign);

    // Discarding a draft is authorized by ownership alone, regardless of its
    // staged catalog names.
    let bob_deleted: serde_json::Value = server
        .graphql(&delete(bob_id), Some(&token(&server, BOB)))
        .await;
    insta::assert_json_snapshot!("delete_draft_as_viewer", bob_deleted);

    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM draft_specs WHERE draft_id = $1")
        .bind(bob_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(remaining, 0);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn stage_draft_specs(pool: sqlx::PgPool) {
    let _guard = test_server::init();

    let draft_id = id(0x80);
    let created_at = ts("2024-03-01T00:00:00Z");
    insert_draft(&pool, draft_id, ALICE, "edits", created_at).await;
    sqlx::query(
        r#"
        INSERT INTO draft_specs (draft_id, catalog_name, spec_type, spec, expect_pub_id, detail, created_at, updated_at) VALUES
          ($1, 'aliceCo/data/foo', 'collection', '{"old":true}', '0000000000000810', 'original detail', $2, $2),
          ($1, 'aliceCo/explicit-null', 'test', '{"old":true}', '0000000000000810', 'must be cleared', $2, $2)
        "#,
    )
    .bind(draft_id)
    .bind(created_at)
    .execute(&pool)
    .await
    .unwrap();

    let live_before: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT catalog_name::text, spec_type::text, spec::text FROM live_specs ORDER BY catalog_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    let server = start(&pool).await;
    let alice_token = token(&server, ALICE);

    // A single batch covering every input shape: a staged catalog deletion, a
    // replacement of a staged model, a clearing of both nullable fields, and a
    // name the draft did not previously carry. A repeated name is replaced by
    // its last entry and returned only once, in sorted order.
    let replacement_model = serde_json::json!({ "changed": true });
    let inserted: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": STAGE_SPECS_MUTATION,
                "variables": {
                    "draftId": draft_id.to_string(),
                    "specs": [
                        {
                            "catalogName": "aliceCo/new",
                            "catalogType": "test",
                            "model": { "steps": [{ "old": true }] }
                        },
                        {
                            "catalogName": "aliceCo/in/capture-foo",
                            "catalogType": null,
                            "model": null,
                            "expectPubId": models::Id::zero().to_string(),
                            "detail": "catalog deletion"
                        },
                        {
                            "catalogName": "aliceCo/data/foo",
                            "catalogType": "capture",
                            "model": replacement_model,
                            "expectPubId": models::Id::zero().to_string(),
                            "detail": "replacement detail"
                        },
                        {
                            "catalogName": "aliceCo/explicit-null",
                            "catalogType": "test",
                            "model": { "steps": [] },
                            "expectPubId": null,
                            "detail": null
                        },
                        {
                            "catalogName": "aliceCo/new",
                            "catalogType": "test",
                            "model": { "steps": [] },
                            "expectPubId": null,
                            "detail": null
                        }
                    ]
                }
            }),
            Some(&alice_token),
        )
        .await;
    insta::assert_json_snapshot!("stage_specs", inserted);
    let staged: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": DRAFT_SPECS_QUERY,
                "variables": { "id": draft_id.to_string() }
            }),
            Some(&alice_token),
        )
        .await;
    insta::assert_json_snapshot!("staged_models", staged["data"]["draft"]["specs"]["edges"], {
        "[].node.updatedAt" => "[ts]",
    });

    // Replacing a staged spec updates the row in place rather than recreating
    // it, and touches the draft so that its own timestamp reflects the edit.
    let (spec_created_at, spec_updated_at): (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>) = sqlx::query_as(
        "SELECT created_at, updated_at FROM draft_specs WHERE draft_id = $1 AND catalog_name = 'aliceCo/data/foo'",
    )
    .bind(draft_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(spec_created_at, created_at);
    assert!(spec_updated_at > created_at);
    let draft_updated_at: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT updated_at FROM drafts WHERE id = $1")
            .bind(draft_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(draft_updated_at > created_at);

    // Omitting the nullable client-editable values clears them, exactly as
    // passing null does.
    let omitted_nullable_values: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": STAGE_SPECS_MUTATION,
                "variables": {
                    "draftId": draft_id.to_string(),
                    "specs": [{
                        "catalogName": "aliceCo/data/foo",
                        "catalogType": "capture",
                        "model": replacement_model
                    }]
                }
            }),
            Some(&alice_token),
        )
        .await;
    assert_eq!(
        omitted_nullable_values["data"]["stageDraftSpecs"],
        serde_json::json!(["aliceCo/data/foo"])
    );
    let updated: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": "query($id: Id!) { draft(id: $id) { specs(first: 1) { edges { node { catalogName expectPubId detail } } } } }",
                "variables": { "id": draft_id }
            }),
            Some(&alice_token),
        )
        .await;
    insta::assert_json_snapshot!(
        "stage_specs_omitted_nullables",
        updated["data"]["draft"]["specs"]["edges"]
    );

    // Staging a change is not publishing it.
    let published: i64 =
        sqlx::query_scalar("SELECT count(*) FROM publications WHERE draft_id = $1")
            .bind(draft_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(published, 0);
    let live_after: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT catalog_name::text, spec_type::text, spec::text FROM live_specs ORDER BY catalog_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        live_after, live_before,
        "staging must not modify published models"
    );
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn stage_preserves_spec_key_order(pool: sqlx::PgPool) {
    let _guard = test_server::init();

    let draft_id = id(0x80);
    insert_draft(
        &pool,
        draft_id,
        ALICE,
        "ordered",
        ts("2024-03-01T00:00:00Z"),
    )
    .await;

    let server = start(&pool).await;
    let alice_token = token(&server, ALICE);

    // Sops verifies its MAC by walking a document in order, so a spec that is
    // re-sorted anywhere between the client and the `json` column can no longer
    // be decrypted. These keys are deliberately not in alphabetical order.
    //
    // Both the request and the response are handled as raw bytes: a
    // `serde_json::Value` sorts its keys, which would destroy the very property
    // under test before it reached the wire.
    const ORDERED_SPEC: &str = r#"{"endpoint":{"connector":{"image":"example/source:test","config":{"password":"ENC[AES256_GCM,data:invented,tag:invented,type:str]","user":"invented"}}},"bindings":[]}"#;

    let body = format!(
        r#"{{"query":{query},"variables":{{"draftId":"{draft_id}","specs":[{{"catalogName":"aliceCo/ordered","catalogType":"capture","model":{ORDERED_SPEC}}}]}}}}"#,
        query = serde_json::to_string(STAGE_SPECS_MUTATION).unwrap(),
    );
    let request = serde_json::value::RawValue::from_string(body).unwrap();
    let staged: serde_json::Value = server.graphql(&request, Some(&alice_token)).await;
    assert_eq!(
        staged["data"]["stageDraftSpecs"],
        serde_json::json!(["aliceCo/ordered"])
    );
    let response: Box<serde_json::value::RawValue> = server
        .graphql(
            &serde_json::json!({
                "query": DRAFT_SPECS_QUERY,
                "variables": { "id": draft_id.to_string() }
            }),
            Some(&alice_token),
        )
        .await;

    assert!(
        response.get().contains(ORDERED_SPEC),
        "response reordered the spec: {}",
        response.get()
    );

    let stored: String = sqlx::query_scalar(
        "SELECT spec::text FROM draft_specs WHERE draft_id = $1 AND catalog_name = $2",
    )
    .bind(draft_id)
    .bind("aliceCo/ordered")
    .fetch_one(&pool)
    .await
    .unwrap();
    // sqlx writes a JSONB version byte and then overwrites it with a space once
    // the parameter resolves to `json`, so every spec stored through this path
    // carries one byte of leading whitespace. It sits outside the document, and
    // the ordering this test exists to protect is unaffected.
    assert_eq!(stored.trim_start(), ORDERED_SPEC);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn stage_draft_specs_validates_input(pool: sqlx::PgPool) {
    let _guard = test_server::init();

    let draft_id = id(0x80);
    insert_draft(&pool, draft_id, ALICE, "edits", ts("2024-03-01T00:00:00Z")).await;

    let server = start(&pool).await;
    let alice_token = token(&server, ALICE);

    let mut responses = std::collections::BTreeMap::new();
    for (case, input) in [
        ("omitted_type", serde_json::json!({ "model": {} })),
        (
            "omitted_spec",
            serde_json::json!({ "catalogType": "collection" }),
        ),
        (
            "null_type",
            serde_json::json!({ "catalogType": null, "model": {} }),
        ),
        (
            "null_spec",
            serde_json::json!({ "catalogType": "collection", "model": null }),
        ),
    ] {
        let mut invalid = input;
        invalid["catalogName"] = serde_json::json!("aliceCo/invalid");
        // A valid leading entry must not land when a later input is invalid.
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": STAGE_SPECS_MUTATION,
                    "variables": {
                        "draftId": draft_id.to_string(),
                        "specs": [
                            { "catalogName": "aliceCo/should-not-insert", "catalogType": "collection", "model": {} },
                            invalid
                        ]
                    }
                }),
                Some(&alice_token),
            )
            .await;
        responses.insert(case, error_message(&response).to_owned());
    }
    insta::assert_json_snapshot!("stage_specs_invalid_inputs", responses);

    let written: i64 = sqlx::query_scalar("SELECT count(*) FROM draft_specs WHERE draft_id = $1")
        .bind(draft_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(written, 0, "a rejected batch must write no rows");
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn owner_can_stage_and_unstage_outside_grants(pool: sqlx::PgPool) {
    let _guard = test_server::init();

    let draft_id = id(0x80);
    let created_at = ts("2024-03-01T00:00:00Z");
    insert_draft(&pool, draft_id, BOB, "edits", created_at).await;

    let server = start(&pool).await;
    let bob_token = token(&server, BOB);

    let staged: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": STAGE_SPECS_MUTATION,
                "variables": {
                    "draftId": draft_id.to_string(),
                    "specs": [
                        {
                            "catalogName": "aliceCo/data/foo",
                            "catalogType": "collection",
                            "model": {}
                        },
                        {
                            "catalogName": "otherCo/new",
                            "catalogType": "collection",
                            "model": { "schema": { "type": "object" } }
                        }
                    ]
                }
            }),
            Some(&bob_token),
        )
        .await;
    insta::assert_json_snapshot!("stage_specs_outside_grants", staged);
    // The first model matches a live spec, but Bob cannot read its metadata.
    let unchanged: i64 =
        sqlx::query_scalar("SELECT count(*) FROM unchanged_draft_specs WHERE draft_id = $1")
            .bind(draft_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(unchanged, 1);
    let queried: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": DRAFT_SPECS_QUERY,
                "variables": { "id": draft_id.to_string() }
            }),
            Some(&bob_token),
        )
        .await;
    assert_eq!(queried["data"]["draft"]["numSpecs"], 2);
    let nodes = queried["data"]["draft"]["specs"]["edges"]
        .as_array()
        .unwrap();
    assert_eq!(nodes[0]["node"]["model"], serde_json::json!({}));
    assert_eq!(nodes[0]["node"]["lastPubId"], serde_json::Value::Null);
    assert_eq!(nodes[0]["node"]["isUnchanged"], false);
    let names: Vec<_> = nodes
        .iter()
        .map(|edge| &edge["node"]["catalogName"])
        .collect();
    assert_eq!(serde_json::json!(names), staged["data"]["stageDraftSpecs"]);

    let unstaged: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": UNSTAGE_SPECS_MUTATION,
                "variables": {
                    "draftId": draft_id.to_string(),
                    "catalogNames": ["otherCo/new", "aliceCo/data/foo"]
                }
            }),
            Some(&bob_token),
        )
        .await;
    insta::assert_json_snapshot!("unstage_specs_outside_grants", unstaged);

    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM draft_specs WHERE draft_id = $1")
        .bind(draft_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(remaining, 0);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn unstage_draft_specs(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let draft_id = id(0x80);
    let other_draft_id = id(0x81);
    let created_at = ts("2024-03-01T00:00:00Z");
    insert_draft(&pool, draft_id, ALICE, "edits", created_at).await;
    insert_draft(&pool, other_draft_id, BOB, "other draft", created_at).await;
    sqlx::query(
        r#"
        INSERT INTO draft_specs (draft_id, catalog_name, spec_type, spec) VALUES
          ($1, 'aliceCo/delete-me', null, null),
          ($1, 'aliceCo/new', 'test', '{}'),
          ($1, 'aliceCo/keep', 'test', '{}'),
          ($2, 'aliceCo/new', 'test', '{}')
        "#,
    )
    .bind(draft_id)
    .bind(other_draft_id)
    .execute(&pool)
    .await
    .unwrap();
    let server = start(&pool).await;
    let alice_token = token(&server, ALICE);
    let deleted: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": UNSTAGE_SPECS_MUTATION,
                "variables": {
                    "draftId": draft_id,
                    "catalogNames": ["aliceCo/new", "aliceCo/not-present", "aliceCo/delete-me"]
                }
            }),
            Some(&alice_token),
        )
        .await;
    insta::assert_json_snapshot!("unstage_specs", deleted);
    let remaining: Vec<(models::Id, String)> = sqlx::query_as(
        "SELECT draft_id, catalog_name::text FROM draft_specs ORDER BY draft_id, catalog_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        remaining,
        vec![
            (draft_id, "aliceCo/keep".to_owned()),
            (other_draft_id, "aliceCo/new".to_owned())
        ]
    );
    let edited_at: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT updated_at FROM drafts WHERE id = $1")
            .bind(draft_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(edited_at > created_at);

    for (field, query, variables) in [
        (
            "unstageDraftSpecs",
            UNSTAGE_SPECS_MUTATION,
            serde_json::json!({ "draftId": draft_id, "catalogNames": [] }),
        ),
        (
            "unstageDraftSpecs",
            UNSTAGE_SPECS_MUTATION,
            serde_json::json!({ "draftId": draft_id, "catalogNames": ["aliceCo/not-present"] }),
        ),
        (
            "stageDraftSpecs",
            STAGE_SPECS_MUTATION,
            serde_json::json!({ "draftId": draft_id, "specs": [] }),
        ),
    ] {
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({ "query": query, "variables": variables }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(response["data"][field], serde_json::json!([]));
        let after_noop: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT updated_at FROM drafts WHERE id = $1")
                .bind(draft_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            after_noop, edited_at,
            "{field} with {variables} must not touch the draft"
        );
    }
    let after_noops: Vec<(models::Id, String)> = sqlx::query_as(
        "SELECT draft_id, catalog_name::text FROM draft_specs ORDER BY draft_id, catalog_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(after_noops, remaining);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn spec_edits_require_an_owned_draft(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let foreign_id = id(0x81);
    let created_at = ts("2024-03-01T00:00:00Z");
    insert_draft(&pool, foreign_id, BOB, "foreign", created_at).await;
    insert_spec(&pool, id(0x811), foreign_id, "aliceCo/private").await;
    let server = start(&pool).await;
    let alice_token = token(&server, ALICE);

    for (query, payload) in [
        (
            STAGE_SPECS_MUTATION,
            serde_json::json!({ "specs": [{
                "catalogName": "aliceCo/private", "catalogType": "collection", "model": { "changed": true }
            }] }),
        ),
        (
            UNSTAGE_SPECS_MUTATION,
            serde_json::json!({ "catalogNames": ["aliceCo/private"] }),
        ),
    ] {
        let mut responses = Vec::new();
        for draft_id in [foreign_id, id(0xffff)] {
            let mut variables = payload.clone();
            variables["draftId"] = serde_json::json!(draft_id);
            let response: serde_json::Value = server
                .graphql(
                    &serde_json::json!({ "query": query, "variables": variables }),
                    Some(&alice_token),
                )
                .await;
            assert_eq!(error_message(&response), "draft not found");
            responses.push(response);
        }
        assert_eq!(
            responses[0], responses[1],
            "foreign and missing IDs must be indistinguishable"
        );
    }
    let stored: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT catalog_name::text, spec::text FROM draft_specs WHERE draft_id = $1",
    )
    .bind(foreign_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(stored, vec![("aliceCo/private".to_owned(), None)]);
    let updated_at: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT updated_at FROM drafts WHERE id = $1")
            .bind(foreign_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(updated_at, created_at);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../../../fixtures",
        scripts("data_planes", "alice", "drafts")
    )
)]
async fn stage_rolls_back_after_database_error(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let draft_id = id(0x80);
    let created_at = ts("2024-03-01T00:00:00Z");
    insert_draft(&pool, draft_id, ALICE, "atomic edit", created_at).await;
    insert_spec(&pool, id(0x801), draft_id, "aliceCo/existing").await;
    let server = start(&pool).await;
    // Name accepts strings; the catalog_name domain rejects the trailing slash
    // only when the second upsert reaches Postgres, after the first has run.
    let response: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": STAGE_SPECS_MUTATION,
                "variables": { "draftId": draft_id, "specs": [
                    { "catalogName": "aliceCo/existing", "catalogType": "collection", "model": { "changed": true } },
                    { "catalogName": "aliceCo/invalid/", "catalogType": "collection", "model": {} }
                ] }
            }),
            Some(&token(&server, ALICE)),
        )
        .await;
    assert!(
        error_message(&response).contains("Must be a valid catalog name"),
        "{response}"
    );
    let stored: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT catalog_name::text, spec::text FROM draft_specs WHERE draft_id = $1",
    )
    .bind(draft_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(stored, vec![("aliceCo/existing".to_owned(), None)]);
    let updated_at: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT updated_at FROM drafts WHERE id = $1")
            .bind(draft_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(updated_at, created_at);
}
