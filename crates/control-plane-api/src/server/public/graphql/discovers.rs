//! GraphQL surface for user-initiated connector discovers.
//!
//! A discover is visible to exactly one person: the owner of the draft it
//! merges into. `discovers` carries no `user_id` of its own, and this API reads
//! Postgres on a privileged connection where row-level security does not
//! apply, so the join to `drafts` on the caller's id *is* the access check.
//! Omitting it would expose every discover, including its `logsToken` bearer
//! credential, to any caller holding an id.

use async_graphql::Context;
use models::Id;

/// A user-initiated connector discovery operation.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct Discover {
    pub id: Id,
    /// The draft that discovered specs are merged into.
    pub draft_id: Id,
    /// Catalog name of the capture being discovered.
    pub capture_name: models::Capture,
    pub connector_tag_id: Id,
    pub data_plane_name: models::Name,
    pub update_only: bool,
    pub detail: Option<String>,
    /// Bearer token for reading this operation's logs.
    pub logs_token: uuid::Uuid,
    /// Current outcome of the operation. `QUEUED` until the server has
    /// processed it.
    pub status: models::discovers::DiscoverStatusType,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Default)]
pub struct DiscoversQuery;

#[async_graphql::Object]
impl DiscoversQuery {
    /// Fetch a single discover by id. Returns null if no such discover exists
    /// or the caller does not own its draft; the two cases are
    /// indistinguishable.
    async fn discover(&self, ctx: &Context<'_>, id: Id) -> async_graphql::Result<Option<Discover>> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        let Some(row) = sqlx::query!(
            r#"
            SELECT
                d.id AS "id: Id",
                d.draft_id AS "draft_id: Id",
                d.capture_name AS "capture_name: models::Capture",
                d.connector_tag_id AS "connector_tag_id: Id",
                d.data_plane_name AS "data_plane_name: models::Name",
                d.update_only,
                d.detail,
                d.logs_token,
                d.job_status->>'type' AS "status!: String",
                d.created_at,
                d.updated_at
            FROM discovers d
            JOIN drafts ON drafts.id = d.draft_id
            WHERE d.id = $1::flowid AND drafts.user_id = $2
            "#,
            id as Id,
            claims.sub,
        )
        .fetch_optional(&env.pg_pool)
        .await?
        else {
            return Ok(None);
        };

        // The stored tag is the camelCase serde name of DiscoverStatusType.
        let status = serde_json::from_value(serde_json::Value::String(row.status))?;

        Ok(Some(Discover {
            id: row.id,
            draft_id: row.draft_id,
            capture_name: row.capture_name,
            connector_tag_id: row.connector_tag_id,
            data_plane_name: row.data_plane_name,
            update_only: row.update_only,
            detail: row.detail,
            logs_token: row.logs_token,
            status,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }))
    }
}

#[cfg(test)]
mod test {
    use crate::test_server;

    const DISCOVER_QUERY: &str = r#"
    query($id: Id!) {
        discover(id: $id) {
            id
            draftId
            captureName
            connectorTagId
            dataPlaneName
            updateOnly
            detail
            logsToken
            status
            createdAt
            updatedAt
        }
    }"#;

    const ALICE: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);

    // Alice owns draft ..01 (one queued discover, and one discover per stored
    // status) and draft ..02 (one discover, deleted by the cascade case). Bob
    // owns draft ..03 with one discover. Bob needs only an auth.users row, not
    // the bob_co fixture's tenant, so he is created here as invite_links does.
    async fn seed(pool: &sqlx::PgPool) {
        sqlx::raw_sql(
            r#"
            INSERT INTO auth.users (id, email) VALUES
                ('22222222-2222-2222-2222-222222222222', 'bob@example.test');

            INSERT INTO drafts (id, user_id, detail) VALUES
                ('0100000000000001', '11111111-1111-1111-1111-111111111111', 'alice draft'),
                ('0100000000000002', '11111111-1111-1111-1111-111111111111', 'alice doomed draft'),
                ('0100000000000003', '22222222-2222-2222-2222-222222222222', 'bob draft');

            INSERT INTO discovers (
                id, draft_id, capture_name, connector_tag_id, data_plane_name,
                endpoint_config, update_only, detail, job_status
            )
            SELECT
                s.id::flowid, s.draft_id::flowid, s.capture_name, '66:66:66:66:00:00:00:00',
                'ops/dp/public/aws-us-west-2-c1', '{}', s.update_only, s.detail,
                ('{"type": "' || s.status || '"}')::jsonb_obj
            FROM (VALUES
                ('0200000000000001', '0100000000000001', 'aliceCo/in/capture-foo', true,  'first discover', 'queued'),
                ('0200000000000002', '0100000000000002', 'aliceCo/in/doomed',      false, null,             'queued'),
                ('0200000000000003', '0100000000000003', 'bobCo/capture',          false, 'bob discover',   'queued'),

                ('0300000000000001', '0100000000000001', 'aliceCo/in/s', false, null, 'queued'),
                ('0300000000000002', '0100000000000001', 'aliceCo/in/s', false, null, 'success'),
                ('0300000000000003', '0100000000000001', 'aliceCo/in/s', false, null, 'discoverFailed'),
                ('0300000000000004', '0100000000000001', 'aliceCo/in/s', false, null, 'mergeFailed'),
                ('0300000000000005', '0100000000000001', 'aliceCo/in/s', false, null, 'pullFailed'),
                ('0300000000000006', '0100000000000001', 'aliceCo/in/s', false, null, 'tagFailed'),
                ('0300000000000007', '0100000000000001', 'aliceCo/in/s', false, null, 'wrongProtocol'),
                ('0300000000000008', '0100000000000001', 'aliceCo/in/s', false, null, 'imageForbidden'),
                ('0300000000000009', '0100000000000001', 'aliceCo/in/s', false, null, 'noDataPlane'),
                ('030000000000000a', '0100000000000001', 'aliceCo/in/s', false, null, 'notAuthorized'),
                ('030000000000000b', '0100000000000001', 'aliceCo/in/s', false, null, 'deprecatedBackground')
            ) AS s(id, draft_id, capture_name, update_only, detail, status);
            "#,
        )
        .execute(pool)
        .await
        .unwrap();
    }

    async fn query(
        server: &test_server::TestServer,
        id: &str,
        token: Option<&str>,
    ) -> serde_json::Value {
        server
            .graphql(
                &serde_json::json!({
                    "query": DISCOVER_QUERY,
                    "variables": { "id": id },
                }),
                token,
            )
            .await
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../fixtures",
            scripts("data_planes", "alice", "connectors")
        )
    )]
    async fn test_discover_query(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        seed(&pool).await;
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;
        let alice_token = server.make_access_token(ALICE, Some("alice@example.com"));

        // Alice reads her own queued discover.
        let own = query(&server, "0200000000000001", Some(&alice_token)).await;
        insta::assert_json_snapshot!("own_discover", own, {
            ".data.discover.logsToken" => "[uuid]",
            ".data.discover.createdAt" => "[datetime]",
            ".data.discover.updatedAt" => "[datetime]",
        });

        // Every stored camelCase tag reads back as its GraphQL name.
        let mut statuses = serde_json::Map::new();
        for id in [
            "0300000000000001",
            "0300000000000002",
            "0300000000000003",
            "0300000000000004",
            "0300000000000005",
            "0300000000000006",
            "0300000000000007",
            "0300000000000008",
            "0300000000000009",
            "030000000000000a",
            "030000000000000b",
        ] {
            let response = query(&server, id, Some(&alice_token)).await;
            statuses.insert(
                id.to_string(),
                response["data"]["discover"]["status"].clone(),
            );
        }
        insta::assert_json_snapshot!("status_mapping", statuses);

        // Another user's discover and an unknown id are the same null, with no
        // error to tell them apart.
        let not_owned = query(&server, "0200000000000003", Some(&alice_token)).await;
        let unknown = query(&server, "0f00000000000000", Some(&alice_token)).await;
        assert_eq!(not_owned, unknown);
        insta::assert_json_snapshot!("not_owned_or_unknown", not_owned);

        // Deleting a draft cascades to its discovers.
        let before = query(&server, "0200000000000002", Some(&alice_token)).await;
        assert_eq!(before["data"]["discover"]["id"], "0200000000000002");
        sqlx::query("DELETE FROM drafts WHERE id = '0100000000000002'")
            .execute(&pool)
            .await
            .unwrap();
        let after = query(&server, "0200000000000002", Some(&alice_token)).await;
        assert_eq!(after, unknown);

        // Unauthenticated callers get an error, not a null.
        let unauthenticated = query(&server, "0200000000000001", None).await;
        insta::assert_json_snapshot!("unauthenticated", unauthenticated);
    }
}
