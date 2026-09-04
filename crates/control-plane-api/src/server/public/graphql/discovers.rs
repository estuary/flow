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

#[derive(Debug, Default)]
pub struct DiscoversMutation;

#[async_graphql::Object]
impl DiscoversMutation {
    /// Queue a connector discovery for `captureName`, merging the discovered
    /// bindings and collections into a draft.
    ///
    /// When `draftId` is given, that draft is used and must be owned by the
    /// caller. When it is omitted, a new draft owned by the caller is created
    /// and its id is returned as `Discover.draftId`.
    ///
    /// The caller must hold `SpecEdit` on `captureName` and read on
    /// `dataPlaneName`. The connector tag must exist, have a successfully
    /// processed spec, and be a capture connector. Poll `discover(id)` for the
    /// outcome; the merged specs are in the draft.
    ///
    /// Note that a legacy `write` grant does not convey `SpecEdit`: users who
    /// can read and append to a prefix's collections still cannot discover
    /// under it. Legacy `admin` on a prefix covering the capture satisfies
    /// both requirements in a normally provisioned tenant. The `Editor` bundle
    /// conveys `SpecEdit` but on its own does not reach the public data
    /// planes through the tenant's role grant.
    async fn create_discover(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Catalog name of the capture to discover. It need not exist yet.")]
        capture_name: models::Capture,
        #[graphql(
            desc = "Id of the connector tag to run, as returned by `connector.spec(imageTag).id`."
        )]
        connector_tag_id: Id,
        #[graphql(
            desc = "Data plane in which to run the connector. The caller must hold read on it."
        )]
        data_plane_name: models::Name,
        #[graphql(
            desc = "Endpoint configuration passed to the connector, stored verbatim. Object \
                    field order is preserved so that sops-encrypted configurations remain \
                    decryptable. Encryption is the caller's responsibility."
        )]
        endpoint_config: async_graphql::Json<async_graphql::Value>,
        #[graphql(
            desc = "The draft that discovered specs are merged into. Must be owned by the \
                    caller. When omitted, a new draft is created for this discover."
        )]
        draft_id: Option<Id>,
        #[graphql(
            default = false,
            desc = "When true, existing bindings are refreshed and newly discovered bindings \
                    are added in a disabled state."
        )]
        update_only: bool,
        #[graphql(
            desc = "Optional description, recorded on the discover and on a draft this \
                    mutation creates."
        )]
        detail: Option<String>,
    ) -> async_graphql::Result<Discover> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        if let Err(err) = validator::Validate::validate(&capture_name) {
            return Err(async_graphql::Error::new(format!(
                "invalid capture name: {err}"
            )));
        }

        // The executor re-checks both of these against its own pinned
        // Snapshot, but a denial there surfaces only as a terminal job status
        // after the job has been queued. Gating here makes it a synchronous
        // error instead.
        super::verify_authorization(env, &capture_name, models::authz::Capability::SpecEdit)
            .await?;

        // Resolving the plane consults what exists in the Snapshot, so an
        // unauthorized plane and a missing one are deliberately
        // indistinguishable: a terminal denial becomes the same "not found"
        // as an absent plane, while a provisional denial against a stale
        // Snapshot still takes the standard refresh-and-retry path.
        let snapshot = env.snapshot();
        let policy_result = crate::server::evaluate_names_authorization(
            snapshot,
            claims,
            models::Capability::Read,
            [data_plane_name.as_str()],
        );
        let may_read_plane = match env.authorization_outcome(policy_result).await {
            Ok(_) => true,
            Err(retry @ crate::ApiError::AuthZRetry(_)) => return Err(retry.into()),
            Err(_) => false,
        };
        if !may_read_plane
            || snapshot
                .data_plane_by_catalog_name(&data_plane_name)
                .is_none()
        {
            return Err(async_graphql::Error::new(format!(
                "data plane {data_plane_name} was not found"
            )));
        }

        // `protocol` is null until the tag's spec job succeeds, and a null
        // comparison would fail the non-null decode rather than read as false.
        let Some(tag) = sqlx::query!(
            r#"
            SELECT
                job_status->>'type' = 'success' AS "processed!",
                coalesce(protocol = 'capture', false) AS "is_capture!"
            FROM connector_tags
            WHERE id = $1::flowid
            "#,
            connector_tag_id as Id,
        )
        .fetch_optional(&env.pg_pool)
        .await?
        else {
            return Err(async_graphql::Error::new("connector tag not found"));
        };
        if !tag.processed {
            return Err(async_graphql::Error::new(format!(
                "connector tag {connector_tag_id} has not been successfully processed"
            )));
        }
        if !tag.is_capture {
            return Err(async_graphql::Error::new(format!(
                "connector tag {connector_tag_id} is not a capture connector"
            )));
        }

        let mut txn = env.pg_pool.begin().await?;

        // A given draft must belong to the caller; missing and not-owned are
        // the same "not found" so ids cannot probe other users' drafts.
        let draft_id = match draft_id {
            Some(draft_id) => sqlx::query_scalar!(
                r#"SELECT id AS "id: Id" FROM drafts WHERE id = $1::flowid AND user_id = $2"#,
                draft_id as Id,
                claims.sub,
            )
            .fetch_optional(&mut *txn)
            .await?
            .ok_or_else(|| async_graphql::Error::new("draft not found"))?,
            None => sqlx::query_scalar!(
                r#"INSERT INTO drafts (user_id, detail) VALUES ($1, $2) RETURNING id AS "id: Id""#,
                claims.sub,
                detail.as_deref(),
            )
            .fetch_one(&mut *txn)
            .await?,
        };

        // `async_graphql::Value` keeps object fields in input order, and JSON
        // (not JSONB) storage keeps them on the way in, so the stored
        // configuration has the caller's field order (whitespace is not kept).
        let endpoint_config =
            models::RawValue::from_string(serde_json::to_string(&endpoint_config.0)?)?;

        // The `create_discover_task` trigger enqueues the executor's task.
        let row = sqlx::query!(
            r#"
            INSERT INTO discovers (
                capture_name, connector_tag_id, draft_id, endpoint_config,
                update_only, data_plane_name, detail
            )
            VALUES ($1::text::catalog_name, $2::flowid, $3::flowid, $4, $5, $6, $7)
            RETURNING id AS "id: Id", logs_token, created_at, updated_at
            "#,
            capture_name.as_str(),
            connector_tag_id as Id,
            draft_id as Id,
            crate::TextJson(endpoint_config) as crate::TextJson<models::RawValue>,
            update_only,
            data_plane_name.as_str(),
            detail.as_deref(),
        )
        .fetch_one(&mut *txn)
        .await?;

        txn.commit().await?;

        tracing::info!(
            discover_id = %row.id,
            %draft_id,
            %capture_name,
            %connector_tag_id,
            %data_plane_name,
            user_id = %claims.sub,
            "queued discover"
        );

        Ok(Discover {
            id: row.id,
            draft_id,
            capture_name,
            connector_tag_id,
            data_plane_name,
            update_only,
            detail,
            logs_token: row.logs_token,
            status: models::discovers::DiscoverStatusType::Queued,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

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
    const BOB: uuid::Uuid = uuid::Uuid::from_bytes([0x22; 16]);
    const SERVICE_ACCOUNT: uuid::Uuid = uuid::Uuid::from_bytes([0x33; 16]);
    const CAROL: uuid::Uuid = uuid::Uuid::from_bytes([0x44; 16]);
    const SOURCE_TAG: &str = "6666666600000000";
    // Deliberately unsorted so that a stored copy proves field order survived.
    const ENDPOINT_CONFIG: &str = r#"{"b": 1, "a": {"z": true, "y": null}, "Z": "z", "A": "a"}"#;
    const PUBLIC_PLANE: &str = "ops/dp/public/aws-us-west-2-c1";

    // Alice owns draft ..01 (one queued discover, and one discover per stored
    // status) and draft ..02 (one discover, deleted by the cascade case). Bob
    // owns draft ..03 with one discover. Bob needs only an auth.users row, not
    // the bob_co fixture's tenant, so he is created here as invite_links does.
    //
    // Grants are seeded before the server starts because its authorization
    // Snapshot is taken once at start-up: bob holds legacy `write` on aliceCo/,
    // carol holds the `editor` bundle with no legacy capability, and a service
    // account holds `admin`.
    async fn seed(pool: &sqlx::PgPool) {
        sqlx::raw_sql(
            r#"
            INSERT INTO auth.users (id, email) VALUES
                ('22222222-2222-2222-2222-222222222222', 'bob@example.test'),
                ('33333333-3333-3333-3333-333333333333', 'aliceCo/robot@service_accounts.estuary.dev'),
                ('44444444-4444-4444-4444-444444444444', 'carol@example.test');

            INSERT INTO internal.service_accounts (user_id, catalog_name, created_by) VALUES
                ('33333333-3333-3333-3333-333333333333', 'aliceCo/robot', '11111111-1111-1111-1111-111111111111');

            INSERT INTO user_grants (user_id, object_role, capability, bundles) VALUES
                ('22222222-2222-2222-2222-222222222222', 'aliceCo/', 'write', '{}'),
                ('33333333-3333-3333-3333-333333333333', 'aliceCo/', 'admin', '{}'),
                ('44444444-4444-4444-4444-444444444444', 'aliceCo/', 'none', '{editor}');

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

    /// A `createDiscover` request. Optional arguments are omitted from the
    /// document when `None`, which is the only way to exercise a default: a
    /// variable bound to null is rejected for a non-null argument. The body
    /// is rendered to text so that `endpoint_config` reaches the server
    /// byte-for-byte; a `serde_json::Value` would sort its keys.
    struct CreateRequest {
        capture_name: &'static str,
        connector_tag_id: &'static str,
        data_plane_name: &'static str,
        endpoint_config: &'static str,
        draft_id: Option<&'static str>,
        update_only: Option<bool>,
        detail: Option<&'static str>,
    }

    impl CreateRequest {
        fn new(capture_name: &'static str) -> Self {
            Self {
                capture_name,
                connector_tag_id: SOURCE_TAG,
                data_plane_name: PUBLIC_PLANE,
                endpoint_config: ENDPOINT_CONFIG,
                draft_id: None,
                update_only: None,
                detail: None,
            }
        }

        fn body(&self) -> Box<serde_json::value::RawValue> {
            let quote = |s: &str| serde_json::to_string(s).unwrap();
            let mut args = format!(
                "captureName: {}, connectorTagId: {}, dataPlaneName: {}, endpointConfig: $endpointConfig",
                quote(self.capture_name),
                quote(self.connector_tag_id),
                quote(self.data_plane_name),
            );
            if let Some(draft_id) = self.draft_id {
                args += &format!(", draftId: {}", quote(draft_id));
            }
            if let Some(update_only) = self.update_only {
                args += &format!(", updateOnly: {update_only}");
            }
            if let Some(detail) = self.detail {
                args += &format!(", detail: {}", quote(detail));
            }
            let query = format!(
                "mutation($endpointConfig: JSON!) {{ createDiscover({args}) {{ \
                 id draftId captureName connectorTagId dataPlaneName updateOnly detail \
                 logsToken status createdAt updatedAt }} }}"
            );
            serde_json::value::RawValue::from_string(format!(
                r#"{{"query": {}, "variables": {{"endpointConfig": {}}}}}"#,
                quote(&query),
                self.endpoint_config,
            ))
            .unwrap()
        }
    }

    async fn create(
        server: &test_server::TestServer,
        request: CreateRequest,
        token: Option<&str>,
    ) -> serde_json::Value {
        server.graphql(&request.body(), token).await
    }

    /// The id of a successful `createDiscover`, or a panic showing the errors.
    fn created_id(response: &serde_json::Value) -> String {
        response["data"]["createDiscover"]["id"]
            .as_str()
            .unwrap_or_else(|| panic!("createDiscover failed: {response}"))
            .to_string()
    }

    /// What the mutation wrote, read back from the tables it touched: the
    /// discovers row, its draft's owner and detail, and the automation task
    /// the insert trigger enqueued. The configuration is read as text so the
    /// snapshot shows field order; sqlx's JSON encoding prefixes one space
    /// where JSONB would carry a version byte, which `ltrim` drops.
    async fn stored(pool: &sqlx::PgPool, id: &str) -> serde_json::Value {
        sqlx::query_scalar(
            r#"
            SELECT json_build_object(
                'capture_name', d.capture_name,
                'connector_tag_id', d.connector_tag_id::text,
                'data_plane_name', d.data_plane_name,
                'endpoint_config', ltrim(d.endpoint_config::text),
                'update_only', d.update_only,
                'detail', d.detail,
                'job_status', d.job_status,
                'draft_user_id', drafts.user_id,
                'draft_detail', drafts.detail,
                'task_type', (SELECT task_type FROM internal.tasks WHERE task_id = d.id)
            )
            FROM discovers d JOIN drafts ON drafts.id = d.draft_id
            WHERE d.id = $1::flowid
            "#,
        )
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn count_for_capture(pool: &sqlx::PgPool, capture_name: &str) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM discovers WHERE capture_name = $1")
            .bind(capture_name)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../fixtures",
            scripts("data_planes", "alice", "connectors")
        )
    )]
    async fn test_create_discover(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        seed(&pool).await;
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;
        let alice_token = server.make_access_token(ALICE, Some("alice@example.com"));

        // Into an existing owned draft, with every optional argument given.
        // `aliceCo/in/capture-foo` has a live spec.
        let request = CreateRequest {
            draft_id: Some("0100000000000001"),
            update_only: Some(true),
            detail: Some("into existing draft"),
            ..CreateRequest::new("aliceCo/in/capture-foo")
        };
        let response = create(&server, request, Some(&alice_token)).await;
        insta::assert_json_snapshot!("create_into_existing_draft", response, {
            ".data.createDiscover.id" => "[id]",
            ".data.createDiscover.logsToken" => "[uuid]",
            ".data.createDiscover.createdAt" => "[datetime]",
            ".data.createDiscover.updatedAt" => "[datetime]",
        });
        let first_id = created_id(&response);
        insta::assert_json_snapshot!(
            "create_into_existing_draft_stored",
            stored(&pool, &first_id).await
        );

        // Again into the same draft: no uniqueness rule applies.
        let request = CreateRequest {
            draft_id: Some("0100000000000001"),
            ..CreateRequest::new("aliceCo/in/capture-foo")
        };
        let response = create(&server, request, Some(&alice_token)).await;
        let second_id = created_id(&response);
        assert_ne!(first_id, second_id);
        // Two created here plus the seeded one.
        assert_eq!(3, count_for_capture(&pool, "aliceCo/in/capture-foo").await);
        assert_eq!(
            4,
            stored(&pool, &second_id).await["task_type"],
            "each discover enqueues its own task"
        );

        // Draft omitted: a new draft owned by the caller is created, and
        // `detail` lands on both rows. The capture has no live spec.
        let request = CreateRequest {
            detail: Some("auto draft"),
            ..CreateRequest::new("aliceCo/brand-new/capture")
        };
        let response = create(&server, request, Some(&alice_token)).await;
        let created = &response["data"]["createDiscover"];
        let row = stored(&pool, &created_id(&response)).await;
        let draft_id: String = sqlx::query_scalar(
            "SELECT replace(id::text, ':', '') FROM drafts WHERE user_id = $1 AND detail = 'auto draft'",
        )
        .bind(ALICE)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(created["draftId"], draft_id);
        insta::assert_json_snapshot!("create_auto_draft_stored", row);

        // Draft and detail both omitted: both detail columns are null.
        let response = create(
            &server,
            CreateRequest::new("aliceCo/brand-new/capture"),
            Some(&alice_token),
        )
        .await;
        let row = stored(&pool, &created_id(&response)).await;
        assert_eq!(serde_json::Value::Null, row["detail"]);
        assert_eq!(serde_json::Value::Null, row["draft_detail"]);
        assert_eq!(false, row["update_only"]);

        // A service account is an ordinary caller: it owns the created draft.
        let sa_token = server.make_access_token(SERVICE_ACCOUNT, None);
        let response = create(
            &server,
            CreateRequest::new("aliceCo/robot/capture"),
            Some(&sa_token),
        )
        .await;
        let row = stored(&pool, &created_id(&response)).await;
        assert_eq!(SERVICE_ACCOUNT.to_string(), row["draft_user_id"]);

        // The `editor` bundle alone conveys SpecEdit, so carol passes the
        // capture gate, but it does not reach the public plane: the tenant's
        // role grant to `ops/dp/public/` is legacy `read`, whose Viewer bits
        // include ViewDataPlanePrivateNetworking, and a Delegate edge passes
        // through only the bits its parent holds. Editor lacks that bit, so
        // the plane check fails exactly as the executor's would.
        let carol_token = server.make_access_token(CAROL, Some("carol@example.test"));
        let response = create(
            &server,
            CreateRequest::new("aliceCo/carol/capture"),
            Some(&carol_token),
        )
        .await;
        insta::assert_json_snapshot!("create_editor_bundle_cannot_reach_plane", response);
        assert_eq!(0, count_for_capture(&pool, "aliceCo/carol/capture").await);

        // No SpecEdit on the capture: alice holds nothing under bobCo/.
        let denied = create(&server, CreateRequest::new("bobCo/x"), Some(&alice_token)).await;
        insta::assert_json_snapshot!("create_denied_no_spec_edit", denied);
        assert_eq!(0, count_for_capture(&pool, "bobCo/x").await);

        // Legacy `write` reads and appends but does not convey SpecEdit.
        let bob_token = server.make_access_token(BOB, Some("bob@example.test"));
        let denied = create(&server, CreateRequest::new("aliceCo/x"), Some(&bob_token)).await;
        insta::assert_json_snapshot!("create_denied_legacy_write", denied);
        assert_eq!(0, count_for_capture(&pool, "aliceCo/x").await);
    }

    // Without a read grant on the plane, an existing plane and a missing one
    // yield the same error. The grant is removed before the server starts
    // because the Snapshot is taken once.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../fixtures",
            scripts("data_planes", "alice", "connectors")
        )
    )]
    async fn test_create_discover_without_plane_grant(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        sqlx::query("DELETE FROM role_grants WHERE subject_role = 'aliceCo/' AND object_role = 'ops/dp/public/'")
            .execute(&pool)
            .await
            .unwrap();
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;
        let alice_token = server.make_access_token(ALICE, Some("alice@example.com"));

        let ungranted = create(
            &server,
            CreateRequest::new("aliceCo/in/capture-foo"),
            Some(&alice_token),
        )
        .await;
        let request = CreateRequest {
            data_plane_name: "ops/dp/public/nope",
            ..CreateRequest::new("aliceCo/in/capture-foo")
        };
        let missing = create(&server, request, Some(&alice_token)).await;

        insta::assert_json_snapshot!(
            "create_plane_not_found",
            serde_json::json!({
                "ungranted": ungranted,
                "missing": missing,
            })
        );
        assert_eq!(0, count_for_capture(&pool, "aliceCo/in/capture-foo").await);
    }

    // A denial against a Snapshot older than the request is provisional: the
    // server answers 307 so the client retries once a fresh Snapshot exists,
    // and nothing is written. The gated test Snapshot serves an empty
    // Snapshot first, and `reqwest` must not follow the redirect for the test
    // to observe it.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../fixtures",
            scripts("data_planes", "alice", "connectors")
        )
    )]
    async fn test_create_discover_stale_snapshot(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;
        let alice_token = server.make_access_token(ALICE, Some("alice@example.com"));

        let client = flow_client_next::rest::Client {
            base_url: server.base_url(),
            http_client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
        };
        let response = client
            .post(
                "/api/graphql",
                &CreateRequest::new("aliceCo/in/capture-foo").body(),
                Some(&alice_token),
            )
            .send()
            .await
            .unwrap();

        assert_eq!(reqwest::StatusCode::TEMPORARY_REDIRECT, response.status());
        let location = response.headers()["location"].to_str().unwrap();
        assert!(
            location.starts_with("/api/graphql?started=") && location.contains("&retryAfter="),
            "unexpected Location: {location}"
        );
        assert_eq!(0, count_for_capture(&pool, "aliceCo/in/capture-foo").await);
    }

    // Every synchronous rejection of `createDiscover`, pinned as one contract:
    // the messages are what clients will match on, and none of them writes a
    // row. Missing and not-owned drafts must be byte-identical so a draft id
    // cannot probe for other users' drafts.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../fixtures",
            scripts("data_planes", "alice", "connectors")
        )
    )]
    async fn test_create_discover_validation_errors(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        seed(&pool).await;
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;
        let alice_token = server.make_access_token(ALICE, Some("alice@example.com"));
        let seeded: (i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM discovers), (SELECT count(*) FROM drafts)",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let invalid_capture_name = create(
            &server,
            CreateRequest::new("aliceCo/not a valid name"),
            Some(&alice_token),
        )
        .await;
        let unknown_tag = create(
            &server,
            CreateRequest {
                connector_tag_id: "0f0f0f0f0f0f0f0f",
                ..CreateRequest::new("aliceCo/x")
            },
            Some(&alice_token),
        )
        .await;
        // `source/multi-tag-test:v2` is a tag whose spec job failed, so its
        // `protocol` is still null.
        let unprocessed_tag = create(
            &server,
            CreateRequest {
                connector_tag_id: "6666666600000005",
                ..CreateRequest::new("aliceCo/x")
            },
            Some(&alice_token),
        )
        .await;
        // `materialize/test:test` is successfully processed but the wrong protocol.
        let materialization_tag = create(
            &server,
            CreateRequest {
                connector_tag_id: "6666666600000001",
                ..CreateRequest::new("aliceCo/x")
            },
            Some(&alice_token),
        )
        .await;
        let unknown_plane = create(
            &server,
            CreateRequest {
                data_plane_name: "ops/dp/public/nope",
                ..CreateRequest::new("aliceCo/x")
            },
            Some(&alice_token),
        )
        .await;
        // Draft ..03 belongs to bob.
        let draft_not_owned = create(
            &server,
            CreateRequest {
                draft_id: Some("0100000000000003"),
                ..CreateRequest::new("aliceCo/x")
            },
            Some(&alice_token),
        )
        .await;
        let draft_missing = create(
            &server,
            CreateRequest {
                draft_id: Some("0100000000000f0f"),
                ..CreateRequest::new("aliceCo/x")
            },
            Some(&alice_token),
        )
        .await;
        assert_eq!(draft_not_owned, draft_missing);
        let unauthenticated = create(&server, CreateRequest::new("aliceCo/x"), None).await;

        insta::assert_json_snapshot!(
            "create_validation_errors",
            serde_json::json!({
                "invalid_capture_name": invalid_capture_name,
                "unknown_tag": unknown_tag,
                "unprocessed_tag": unprocessed_tag,
                "materialization_tag": materialization_tag,
                "unknown_plane": unknown_plane,
                "draft_not_owned_or_missing": draft_not_owned,
                "unauthenticated": unauthenticated,
            })
        );

        let after: (i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM discovers), (SELECT count(*) FROM drafts)",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            seeded, after,
            "a rejected discover must insert neither a discover nor a draft"
        );
    }
}
