use async_graphql::{Context, MaybeUndefined, types::connection};

const DEFAULT_PAGE_SIZE: i32 = 100;
const MAX_PAGE_SIZE: i32 = 1000;

/// An authenticated user's private workspace for staging catalog changes.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct Draft {
    pub id: models::Id,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub detail: Option<String>,
    /// Number of staged specifications the caller is currently allowed to read.
    pub num_specs: i32,
}

/// The stable identity and creation time of a deleted draft.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct DeletedDraft {
    pub id: models::Id,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

pub type DraftConnection = connection::Connection<
    models::Id,
    Draft,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

#[derive(Debug)]
struct DraftMetadataRow {
    id: models::Id,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
    detail: Option<String>,
    num_specs: i64,
}

impl DraftMetadataRow {
    fn into_graphql(self) -> Draft {
        Draft {
            id: self.id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            detail: self.detail,
            num_specs: i32::try_from(self.num_specs).expect("draft spec count fits GraphQL Int"),
        }
    }
}

fn page_size(first: MaybeUndefined<i32>) -> async_graphql::Result<i32> {
    match first {
        MaybeUndefined::Undefined => Ok(DEFAULT_PAGE_SIZE),
        MaybeUndefined::Null => Err(async_graphql::Error::new("first must not be null")),
        MaybeUndefined::Value(first) if (1..=MAX_PAGE_SIZE).contains(&first) => Ok(first),
        MaybeUndefined::Value(_) => Err(async_graphql::Error::new(format!(
            "first must be between 1 and {MAX_PAGE_SIZE}"
        ))),
    }
}

#[derive(Debug, Default)]
pub struct DraftsQuery;

#[async_graphql::Object]
impl DraftsQuery {
    /// Look up a draft owned by the authenticated user.
    async fn draft(
        &self,
        ctx: &Context<'_>,
        id: models::Id,
    ) -> async_graphql::Result<Option<Draft>> {
        let env = ctx.data::<crate::Envelope>()?;
        let subject = env.claims()?.subject();
        let user_id = subject.user_id;
        let snapshot = env.snapshot();
        let readable_prefixes = super::authorized_prefixes::authorized_prefixes(
            &snapshot.role_grants,
            &snapshot.user_grants,
            &subject,
            models::authz::Capability::CatalogRead,
            None,
        );

        let row = sqlx::query_as!(
            DraftMetadataRow,
            r#"
            SELECT
                d.id AS "id!: models::Id",
                d.created_at,
                d.updated_at,
                d.detail,
                (
                    SELECT count(*)
                    FROM draft_specs ds
                    WHERE ds.draft_id = d.id
                      AND ds.catalog_name::text ^@ ANY($3)
                ) AS "num_specs!"
            FROM drafts d
            WHERE d.id = $1 AND d.user_id = $2
            "#,
            id as models::Id,
            user_id,
            &readable_prefixes,
        )
        .fetch_optional(&env.pg_pool)
        .await?;

        Ok(row.map(DraftMetadataRow::into_graphql))
    }

    /// List drafts owned by the authenticated user in stable ID order.
    async fn drafts(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        first: MaybeUndefined<i32>,
    ) -> async_graphql::Result<DraftConnection> {
        let env = ctx.data::<crate::Envelope>()?;
        let subject = env.claims()?.subject();
        let user_id = subject.user_id;
        let limit = page_size(first)?;
        let snapshot = env.snapshot();
        let readable_prefixes = super::authorized_prefixes::authorized_prefixes(
            &snapshot.role_grants,
            &snapshot.user_grants,
            &subject,
            models::authz::Capability::CatalogRead,
            None,
        );

        connection::query_with::<models::Id, _, _, _, async_graphql::Error>(
            after,
            None,
            Some(limit),
            None,
            |after, _, first, _| async move {
                let limit = first.expect("validated first is always present");
                let rows = sqlx::query_as!(
                    DraftMetadataRow,
                    r#"
                    SELECT
                        d.id AS "id!: models::Id",
                        d.created_at,
                        d.updated_at,
                        d.detail,
                        (
                            SELECT count(*)
                            FROM draft_specs ds
                            WHERE ds.draft_id = d.id
                              AND ds.catalog_name::text ^@ ANY($4)
                        ) AS "num_specs!"
                    FROM drafts d
                    WHERE d.user_id = $1
                      AND ($2::flowid IS NULL OR d.id > $2)
                    ORDER BY d.id ASC
                    LIMIT $3
                    "#,
                    user_id,
                    after as Option<models::Id>,
                    i64::try_from(limit + 1).expect("page limit fits i64"),
                    &readable_prefixes,
                )
                .fetch_all(&env.pg_pool)
                .await?;

                let has_next = rows.len() > limit;
                let mut result = DraftConnection::new(after.is_some(), has_next);
                result.edges.extend(rows.into_iter().take(limit).map(|row| {
                    let cursor = row.id;
                    connection::Edge::new(cursor, row.into_graphql())
                }));
                Ok(result)
            },
        )
        .await
    }
}

#[derive(Debug, Default)]
pub struct DraftsMutation;

#[async_graphql::Object]
impl DraftsMutation {
    /// Create an empty draft owned by the authenticated user.
    async fn create_draft(
        &self,
        ctx: &Context<'_>,
        detail: Option<String>,
    ) -> async_graphql::Result<Draft> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;

        let row = sqlx::query!(
            r#"
            INSERT INTO drafts (detail, user_id)
            VALUES ($1, $2)
            RETURNING
                id AS "id!: models::Id",
                created_at,
                updated_at
            "#,
            detail.as_deref(),
            user_id,
        )
        .fetch_one(&env.pg_pool)
        .await?;

        Ok(Draft {
            id: row.id,
            created_at: row.created_at,
            updated_at: row.updated_at,
            detail,
            num_specs: 0,
        })
    }

    /// Delete a draft after authorizing every catalog name it contains.
    async fn delete_draft(
        &self,
        ctx: &Context<'_>,
        id: models::Id,
    ) -> async_graphql::Result<DeletedDraft> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;
        let mut txn = env.pg_pool.begin().await?;

        // Lock the parent before its children: the FK makes concurrent inserts
        // wait on this row, while the child locks below block catalog-name
        // edits. Together they keep the authorized name set stable through the
        // cascading delete.
        let row = sqlx::query!(
            r#"
            SELECT id AS "id!: models::Id", created_at
            FROM drafts
            WHERE id = $1 AND user_id = $2
            FOR UPDATE
            "#,
            id as models::Id,
            user_id,
        )
        .fetch_optional(&mut *txn)
        .await?;
        let Some(row) = row else {
            return Err(async_graphql::Error::new("draft not found"));
        };
        let id = row.id;
        let created_at = row.created_at;

        let catalog_names = sqlx::query_scalar!(
            r#"
            SELECT catalog_name::text AS "catalog_name!"
            FROM draft_specs
            WHERE draft_id = $1
            ORDER BY catalog_name
            FOR UPDATE
            "#,
            id as models::Id,
        )
        .fetch_all(&mut *txn)
        .await?;
        let policy_result = crate::server::evaluate_names_authorization(
            env.snapshot(),
            env.claims()?,
            models::authz::Capability::SpecEdit,
            catalog_names.iter(),
        );
        env.authorization_outcome(policy_result).await?;

        let deleted = sqlx::query!(
            "DELETE FROM drafts WHERE id = $1 AND user_id = $2",
            id as models::Id,
            user_id,
        )
        .execute(&mut *txn)
        .await?;
        assert_eq!(deleted.rows_affected(), 1, "locked draft still exists");
        txn.commit().await?;

        Ok(DeletedDraft { id, created_at })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_server;

    const ALICE: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);
    const BOB: uuid::Uuid = uuid::Uuid::from_bytes([0x22; 16]);
    const EDITOR: uuid::Uuid = uuid::Uuid::from_bytes([0x33; 16]);

    fn id(value: u64) -> models::Id {
        models::Id::new(value.to_be_bytes())
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

    #[test]
    fn validates_page_size() {
        assert_eq!(page_size(MaybeUndefined::Undefined).unwrap(), 100);
        assert_eq!(page_size(MaybeUndefined::Value(1)).unwrap(), 1);
        assert_eq!(page_size(MaybeUndefined::Value(1000)).unwrap(), 1000);

        assert_eq!(
            page_size(MaybeUndefined::Null).unwrap_err().message,
            "first must not be null"
        );
        for invalid in [0, -1, 1001] {
            assert_eq!(
                page_size(MaybeUndefined::Value(invalid))
                    .unwrap_err()
                    .message,
                "first must be between 1 and 1000"
            );
        }
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_draft_lifecycle_and_authorization(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        sqlx::query("INSERT INTO auth.users (id, email) VALUES ($1, $2)")
            .bind(BOB)
            .bind("bob@example.test")
            .execute(&pool)
            .await
            .unwrap();
        // A Viewer may own drafts and inspect readable specs, but has no
        // authority to mutate those catalog names.
        sqlx::query(
            "INSERT INTO user_grants (user_id, object_role, capability) VALUES ($1, $2, 'read')",
        )
        .bind(BOB)
        .bind("bobCo/")
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO auth.users (id, email) VALUES ($1, $2)")
            .bind(EDITOR)
            .bind("editor@example.test")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO user_grants (user_id, object_role, capability, bundles)
            VALUES ($1, $2, 'none', ARRAY['editor'::capability_bundle])
            "#,
        )
        .bind(EDITOR)
        .bind("editorCo/")
        .execute(&pool)
        .await
        .unwrap();

        let first_id = id(0x10);
        let foreign_id = id(0x20);
        let second_id = id(0x30);
        let third_id = id(0x40);
        let editor_id = id(0x50);
        let first_created_at = "2024-01-01T00:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        let second_created_at = "2024-01-02T00:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        let third_created_at = "2024-01-03T00:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();

        insert_draft(&pool, first_id, ALICE, "first", first_created_at).await;
        insert_draft(&pool, foreign_id, BOB, "foreign", first_created_at).await;
        insert_draft(&pool, second_id, ALICE, "second", second_created_at).await;
        insert_draft(&pool, third_id, ALICE, "third", third_created_at).await;
        insert_draft(&pool, editor_id, EDITOR, "editor", third_created_at).await;
        insert_spec(&pool, id(0x101), first_id, "aliceCo/readable").await;
        insert_spec(&pool, id(0x102), first_id, "otherCo/not-readable").await;
        insert_spec(&pool, id(0x103), third_id, "aliceCo/deletable").await;
        insert_spec(&pool, id(0x104), foreign_id, "bobCo/view-only").await;
        insert_spec(&pool, id(0x105), editor_id, "editorCo/editable").await;

        let auth_snapshot = test_server::snapshot(pool.clone(), false).await;
        let server = test_server::TestServer::start(pool.clone(), auth_snapshot).await;
        let alice_token = server.make_access_token(ALICE, Some("alice@example.test"));
        let bob_token = server.make_access_token(BOB, Some("bob@example.test"));
        let editor_token = server.make_access_token(EDITOR, Some("editor@example.test"));

        let unauthenticated: serde_json::Value = server
            .graphql(
                &serde_json::json!({ "query": "query { drafts { edges { cursor } } }" }),
                None,
            )
            .await;
        assert!(error_message(&unauthenticated).contains("missing a required Authorization"));

        let first_page: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                        query Drafts($id: Id!) {
                          draft(id: $id) {
                            id createdAt updatedAt detail numSpecs
                          }
                          drafts(first: 2) {
                            edges { cursor node { id detail numSpecs } }
                            pageInfo {
                              hasPreviousPage hasNextPage startCursor endCursor
                            }
                          }
                        }
                    "#,
                    "variables": { "id": first_id.to_string() }
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(first_page["data"]["draft"]["id"], first_id.to_string());
        assert_eq!(first_page["data"]["draft"]["detail"], "first");
        assert_eq!(first_page["data"]["draft"]["numSpecs"], 1);
        assert_eq!(
            first_page["data"]["drafts"]["edges"][0]["node"]["id"],
            first_id.to_string()
        );
        assert_eq!(
            first_page["data"]["drafts"]["edges"][1]["node"]["id"],
            second_id.to_string()
        );
        assert_eq!(
            first_page["data"]["drafts"]["pageInfo"]["hasPreviousPage"],
            false
        );
        assert_eq!(
            first_page["data"]["drafts"]["pageInfo"]["hasNextPage"],
            true
        );

        let after = first_page["data"]["drafts"]["pageInfo"]["endCursor"]
            .as_str()
            .unwrap();
        assert_eq!(after, second_id.to_string());
        let second_page: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                        query Drafts($after: String) {
                          drafts(after: $after, first: 2) {
                            edges { cursor node { id detail numSpecs } }
                            pageInfo { hasPreviousPage hasNextPage }
                          }
                        }
                    "#,
                    "variables": { "after": after }
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            second_page["data"]["drafts"]["edges"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            second_page["data"]["drafts"]["edges"][0]["node"]["id"],
            third_id.to_string()
        );
        assert_eq!(
            second_page["data"]["drafts"]["pageInfo"],
            serde_json::json!({ "hasPreviousPage": true, "hasNextPage": false })
        );

        // A caller's draft identity is private even when another user has
        // catalog permissions, and unknown IDs are indistinguishable.
        for hidden_id in [foreign_id, id(0xffff)] {
            let response: serde_json::Value = server
                .graphql(
                    &serde_json::json!({
                        "query": "query Draft($id: Id!) { draft(id: $id) { id } }",
                        "variables": { "id": hidden_id.to_string() }
                    }),
                    Some(&alice_token),
                )
                .await;
            assert_eq!(response["data"]["draft"], serde_json::Value::Null);
        }

        // Omitting first uses the default and still excludes Bob's draft.
        let default_page: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": "query { drafts { edges { node { id } } } }"
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            default_page["data"]["drafts"]["edges"]
                .as_array()
                .unwrap()
                .len(),
            3
        );

        for invalid_first in [serde_json::Value::Null, 0.into(), (-1).into(), 1001.into()] {
            let response: serde_json::Value = server
                .graphql(
                    &serde_json::json!({
                        "query": "query Drafts($first: Int) { drafts(first: $first) { edges { cursor } } }",
                        "variables": { "first": invalid_first }
                    }),
                    Some(&alice_token),
                )
                .await;
            assert!(error_message(&response).starts_with("first must"));
        }

        let created: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                        mutation {
                          createDraft(detail: "created through GraphQL") {
                            id createdAt updatedAt detail numSpecs
                          }
                        }
                    "#
                }),
                Some(&alice_token),
            )
            .await;
        let created_id = created["data"]["createDraft"]["id"]
            .as_str()
            .expect("created draft ID")
            .parse::<models::Id>()
            .unwrap();
        assert_eq!(
            created["data"]["createDraft"]["detail"],
            "created through GraphQL"
        );
        assert_eq!(created["data"]["createDraft"]["numSpecs"], 0);
        let created_owner: uuid::Uuid =
            sqlx::query_scalar("SELECT user_id FROM drafts WHERE id = $1")
                .bind(created_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(created_owner, ALICE);

        let bob_draft_count_before: i64 =
            sqlx::query_scalar("SELECT count(*) FROM drafts WHERE user_id = $1")
                .bind(BOB)
                .fetch_one(&pool)
                .await
                .unwrap();
        let viewer_created: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": "mutation { createDraft { id numSpecs } }"
                }),
                Some(&bob_token),
            )
            .await;
        let viewer_created_id = viewer_created["data"]["createDraft"]["id"]
            .as_str()
            .expect("Viewer-created draft ID")
            .parse::<models::Id>()
            .unwrap();
        assert_eq!(viewer_created["data"]["createDraft"]["numSpecs"], 0);
        let bob_draft_count_after: i64 =
            sqlx::query_scalar("SELECT count(*) FROM drafts WHERE user_id = $1")
                .bind(BOB)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(bob_draft_count_after, bob_draft_count_before + 1);

        // With no affected catalog names, ownership is sufficient to delete
        // the empty draft that a Viewer was allowed to create.
        let viewer_deleted_empty: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": "mutation Delete($id: Id!) { deleteDraft(id: $id) { id } }",
                    "variables": { "id": viewer_created_id.to_string() }
                }),
                Some(&bob_token),
            )
            .await;
        assert_eq!(
            viewer_deleted_empty["data"]["deleteDraft"]["id"],
            viewer_created_id.to_string()
        );

        let viewer_draft: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": "query Draft($id: Id!) { draft(id: $id) { id numSpecs } }",
                    "variables": { "id": foreign_id.to_string() }
                }),
                Some(&bob_token),
            )
            .await;
        assert_eq!(viewer_draft["data"]["draft"]["numSpecs"], 1);

        let viewer_denied_delete: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": "mutation Delete($id: Id!) { deleteDraft(id: $id) { id } }",
                    "variables": { "id": foreign_id.to_string() }
                }),
                Some(&bob_token),
            )
            .await;
        assert!(error_message(&viewer_denied_delete).contains("bobCo/view-only"));
        assert!(error_message(&viewer_denied_delete).contains("SpecEdit"));
        let viewer_draft_specs: i64 =
            sqlx::query_scalar("SELECT count(*) FROM draft_specs WHERE draft_id = $1")
                .bind(foreign_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(viewer_draft_specs, 1);

        // The Editor bundle, without legacy Admin, supplies the individual
        // CatalogRead and SpecEdit bits used by these resolvers.
        let editor_draft: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": "query Draft($id: Id!) { draft(id: $id) { id numSpecs } }",
                    "variables": { "id": editor_id.to_string() }
                }),
                Some(&editor_token),
            )
            .await;
        assert_eq!(editor_draft["data"]["draft"]["numSpecs"], 1);
        let editor_deleted: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": "mutation Delete($id: Id!) { deleteDraft(id: $id) { id } }",
                    "variables": { "id": editor_id.to_string() }
                }),
                Some(&editor_token),
            )
            .await;
        assert_eq!(
            editor_deleted["data"]["deleteDraft"]["id"],
            editor_id.to_string()
        );

        let denied_delete: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": "mutation Delete($id: Id!) { deleteDraft(id: $id) { id } }",
                    "variables": { "id": first_id.to_string() }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(error_message(&denied_delete).contains("otherCo/not-readable"));
        let remaining_specs: i64 =
            sqlx::query_scalar("SELECT count(*) FROM draft_specs WHERE draft_id = $1")
                .bind(first_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(remaining_specs, 2, "a denied batch deletion is atomic");

        let delete_missing = |delete_id: models::Id| {
            serde_json::json!({
                "query": "mutation Delete($id: Id!) { deleteDraft(id: $id) { id } }",
                "variables": { "id": delete_id.to_string() }
            })
        };
        let foreign_delete: serde_json::Value = server
            .graphql(&delete_missing(foreign_id), Some(&alice_token))
            .await;
        let missing_delete: serde_json::Value = server
            .graphql(&delete_missing(id(0xffff)), Some(&alice_token))
            .await;
        assert_eq!(error_message(&foreign_delete), "draft not found");
        assert_eq!(
            error_message(&foreign_delete),
            error_message(&missing_delete),
            "foreign and missing drafts must not be distinguishable"
        );

        let deleted: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                        mutation Delete($id: Id!) {
                          deleteDraft(id: $id) { id createdAt }
                        }
                    "#,
                    "variables": { "id": third_id.to_string() }
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(deleted["data"]["deleteDraft"]["id"], third_id.to_string());
        let returned_created_at = deleted["data"]["deleteDraft"]["createdAt"]
            .as_str()
            .unwrap()
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        assert_eq!(returned_created_at, third_created_at);
        let deleted_rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM drafts d LEFT JOIN draft_specs ds ON ds.draft_id = d.id WHERE d.id = $1",
        )
        .bind(third_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(deleted_rows, 0);
    }
}
