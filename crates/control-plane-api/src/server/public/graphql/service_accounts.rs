use super::TimestampCursor;
use async_graphql::{Context, types::connection};

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct ServiceAccount {
    // Exposed as `id`: a service account's identifier happens to be its
    // backing auth.users id, but that's an implementation detail we don't
    // surface in the public schema.
    #[graphql(name = "id")]
    pub user_id: uuid::Uuid,
    pub display_name: String,
    pub catalog_name: models::Name,
    pub created_by: uuid::Uuid,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
    pub api_keys: Vec<ApiKeyInfo>,
}

/// A user_grant to seed a service account with at creation time.
#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct ServiceAccountGrantInput {
    pub prefix: models::Prefix,
    pub capability: models::Capability,
}

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct ApiKeyInfo {
    #[graphql(name = "id")]
    pub key_id: models::Id,
    pub label: String,
    pub created_by: uuid::Uuid,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct CreateApiKeyResult {
    #[graphql(name = "id")]
    pub key_id: models::Id,
    pub secret: String,
}

pub type PaginatedServiceAccounts = connection::Connection<
    TimestampCursor,
    ServiceAccount,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

#[derive(Debug, Default)]
pub struct ServiceAccountsQuery;

const DEFAULT_PAGE_SIZE: usize = 25;
const MAX_PREFIXES: usize = 20;

#[async_graphql::Object]
impl ServiceAccountsQuery {
    async fn service_accounts(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        first: Option<i32>,
    ) -> async_graphql::Result<PaginatedServiceAccounts> {
        let env = ctx.data::<crate::Envelope>()?;

        let snapshot = env.snapshot();
        // Service accounts are visible to callers who can manage them: those
        // holding ManageServiceAccounts on a prefix covering the account's
        // catalog_name.
        let user_accessible_prefixes = super::authorized_prefixes::authorized_prefixes(
            &snapshot.role_grants,
            &snapshot.user_grants,
            env.claims()?.sub,
            models::authz::Capability::ManageServiceAccounts,
            None,
        );

        if user_accessible_prefixes.is_empty() {
            return Ok(PaginatedServiceAccounts::new(false, false));
        }
        if user_accessible_prefixes.len() > MAX_PREFIXES {
            return Err(async_graphql::Error::new("Too many accessible prefixes"));
        }

        connection::query_with::<TimestampCursor, _, _, _, async_graphql::Error>(
            after,
            None,
            first,
            None,
            |after, _, first, _| async move {
                let after_created_at = after.map(|c| c.0);
                let limit = first.unwrap_or(DEFAULT_PAGE_SIZE);

                let sa_rows = sqlx::query!(
                    r#"
                    SELECT
                        sa.user_id,
                        sa.display_name,
                        sa.catalog_name AS "catalog_name!: String",
                        sa.created_by,
                        sa.created_at AS "created_at!: chrono::DateTime<chrono::Utc>",
                        sa.updated_at AS "updated_at!: chrono::DateTime<chrono::Utc>",
                        sa.last_used_at AS "last_used_at: chrono::DateTime<chrono::Utc>"
                    FROM internal.service_accounts sa
                    WHERE sa.catalog_name::text ^@ ANY($1)
                      AND ($2::timestamptz IS NULL OR sa.created_at < $2)
                    ORDER BY sa.created_at DESC
                    LIMIT $3 + 1
                    "#,
                    &user_accessible_prefixes,
                    after_created_at,
                    limit as i64,
                )
                .fetch_all(&env.pg_pool)
                .await?;

                let has_next = sa_rows.len() > limit;

                let user_ids: Vec<uuid::Uuid> =
                    sa_rows.iter().take(limit).map(|r| r.user_id).collect();

                // Keys are batch-loaded for the whole page in one query (no
                // N+1). The tradeoff is that this runs even when the caller
                // didn't select `apiKeys`. That's fine for a low-frequency admin
                // listing against an indexed column. Revisit with a `DataLoader`
                // (a `#[ComplexObject]` api_keys resolver backed by a batching
                // loader keyed on service_account_id) if either changes: callers
                // commonly list service accounts WITHOUT `apiKeys` (making this
                // fetch mostly wasted), or more lazily-resolved per-account child
                // collections get added — at which point one batching mechanism
                // beats several conditional eager fetches.
                let key_rows = if user_ids.is_empty() {
                    vec![]
                } else {
                    sqlx::query!(
                        r#"
                        SELECT
                            ak.id AS "id!: models::Id",
                            ak.service_account_id,
                            ak.label,
                            ak.created_by,
                            ak.created_at AS "created_at!: chrono::DateTime<chrono::Utc>",
                            ak.expires_at AS "expires_at!: chrono::DateTime<chrono::Utc>",
                            ak.last_used_at AS "last_used_at: chrono::DateTime<chrono::Utc>"
                        FROM internal.api_keys ak
                        WHERE ak.service_account_id = ANY($1)
                          AND ak.revoked_at IS NULL
                        ORDER BY ak.created_at DESC
                        "#,
                        &user_ids,
                    )
                    .fetch_all(&env.pg_pool)
                    .await?
                };

                let mut keys_by_sa: std::collections::HashMap<uuid::Uuid, Vec<ApiKeyInfo>> =
                    std::collections::HashMap::new();
                for kr in key_rows {
                    keys_by_sa
                        .entry(kr.service_account_id)
                        .or_default()
                        .push(ApiKeyInfo {
                            key_id: kr.id,
                            label: kr.label,
                            created_by: kr.created_by,
                            created_at: kr.created_at,
                            expires_at: kr.expires_at,
                            last_used_at: kr.last_used_at,
                        });
                }

                let edges: Vec<_> = sa_rows
                    .into_iter()
                    .take(limit)
                    .map(|r| {
                        let api_keys = keys_by_sa.remove(&r.user_id).unwrap_or_default();
                        connection::Edge::new(
                            TimestampCursor(r.created_at),
                            ServiceAccount {
                                user_id: r.user_id,
                                display_name: r.display_name,
                                catalog_name: models::Name::new(&r.catalog_name),
                                created_by: r.created_by,
                                created_at: r.created_at,
                                updated_at: r.updated_at,
                                last_used_at: r.last_used_at,
                                api_keys,
                            },
                        )
                    })
                    .collect();

                let mut conn = connection::Connection::new(after_created_at.is_some(), has_next);
                conn.edges = edges;
                Ok(conn)
            },
        )
        .await
    }
}

#[derive(Debug, Default)]
pub struct ServiceAccountsMutation;

#[async_graphql::Object]
impl ServiceAccountsMutation {
    /// Create a service account homed at the specified catalog name, seeded
    /// with the given user_grants.
    ///
    /// `catalogName` is a management anchor: admins of a prefix covering it
    /// may manage the account. It determines who may manage the account, not
    /// what the account may access. Access is determined solely by the
    /// account's user_grants, which may span multiple prefixes.
    ///
    /// The caller must have admin capability on the catalog name AND on each
    /// granted prefix. Creates an auth.users row, an
    /// internal.service_accounts row, and a user_grants row per requested
    /// grant.
    async fn create_service_account(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
        display_name: String,
        grants: Vec<ServiceAccountGrantInput>,
    ) -> async_graphql::Result<ServiceAccount> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        if let Err(err) = validator::Validate::validate(&catalog_name) {
            return Err(async_graphql::Error::new(format!(
                "invalid catalog name: {err}"
            )));
        }
        for grant in &grants {
            if let Err(err) = validator::Validate::validate(&grant.prefix) {
                return Err(async_graphql::Error::new(format!(
                    "invalid grant prefix {}: {err}",
                    grant.prefix.as_str(),
                )));
            }
            // `none` confers no access until bundles are wired, so reject it
            // rather than mint a no-op grant.
            if grant.capability == models::Capability::None {
                return Err(async_graphql::Error::new(
                    "grant capability must be one of: read, write, admin",
                ));
            }
        }

        super::verify_authorization(env, catalog_name.as_str(), models::Capability::Admin).await?;
        // Each grant requires the same authorization as granting a human user:
        // admin capability on the granted prefix. This prevents a caller from
        // provisioning an account with access they don't themselves administer.
        for grant in &grants {
            super::verify_authorization(env, grant.prefix.as_str(), models::Capability::Admin)
                .await?;
        }

        let mut txn = env.pg_pool.begin().await?;

        let sa_user_id = uuid::Uuid::new_v4();

        sqlx::query!(
            r#"
            INSERT INTO auth.users (id, email, raw_user_meta_data)
            VALUES ($1, $2, $3)
            "#,
            sa_user_id,
            format!("sa+{}@service.estuary.dev", sa_user_id),
            serde_json::json!({
                "full_name": display_name,
            }),
        )
        .execute(&mut *txn)
        .await?;

        let now = sqlx::query_scalar!(
            r#"
            INSERT INTO internal.service_accounts (user_id, catalog_name, display_name, created_by)
            VALUES ($1, $2::text::catalog_name, $3, $4)
            RETURNING created_at AS "created_at!: chrono::DateTime<chrono::Utc>"
            "#,
            sa_user_id,
            catalog_name.as_str(),
            display_name,
            claims.sub,
        )
        .fetch_one(&mut *txn)
        .await?;

        for grant in &grants {
            crate::grants::upsert_user_grant(
                sa_user_id,
                grant.prefix.as_str(),
                grant.capability,
                Some("service account grant".to_string()),
                &mut txn,
            )
            .await?;
        }

        txn.commit().await?;

        tracing::info!(
            %catalog_name,
            ?grants,
            %claims.sub,
            %sa_user_id,
            "created service account"
        );

        Ok(ServiceAccount {
            user_id: sa_user_id,
            display_name,
            catalog_name,
            created_by: claims.sub,
            created_at: now,
            updated_at: now,
            last_used_at: None,
            api_keys: vec![],
        })
    }

    /// Add a user_grant to a service account.
    ///
    /// The caller must manage the service account (admin capability on its
    /// catalog name) AND have admin capability on the granted prefix — the
    /// same authorization as granting a human user. The second requirement
    /// prevents a caller from extending an account's access beyond what they
    /// themselves administer.
    async fn add_service_account_grant(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "serviceAccountId")] user_id: uuid::Uuid,
        prefix: models::Prefix,
        capability: models::Capability,
    ) -> async_graphql::Result<bool> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        if let Err(err) = validator::Validate::validate(&prefix) {
            return Err(async_graphql::Error::new(format!(
                "invalid grant prefix {}: {err}",
                prefix.as_str(),
            )));
        }
        // `none` confers no access until bundles are wired, so reject it
        // rather than mint a no-op grant.
        if capability == models::Capability::None {
            return Err(async_graphql::Error::new(
                "grant capability must be one of: read, write, admin",
            ));
        }

        let catalog_name = lookup_service_account(&env.pg_pool, user_id).await?;
        super::verify_authorization(env, &catalog_name, models::Capability::Admin).await?;
        super::verify_authorization(env, prefix.as_str(), models::Capability::Admin).await?;

        let mut txn = env.pg_pool.begin().await?;
        crate::grants::upsert_user_grant(
            user_id,
            prefix.as_str(),
            capability,
            Some("service account grant".to_string()),
            &mut txn,
        )
        .await?;
        txn.commit().await?;

        tracing::info!(
            %user_id,
            %catalog_name,
            %prefix,
            ?capability,
            %claims.sub,
            "added service account grant"
        );

        Ok(true)
    }

    /// Remove a user_grant from a service account.
    ///
    /// The caller must manage the service account (admin capability on its
    /// catalog name). Unlike addServiceAccountGrant, no capability on the
    /// grant's prefix is required: removal only ever narrows the account's
    /// access, so managers may remove ANY grant — including grants to
    /// prefixes they don't themselves administer.
    async fn remove_service_account_grant(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "serviceAccountId")] user_id: uuid::Uuid,
        prefix: models::Prefix,
    ) -> async_graphql::Result<bool> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        let catalog_name = lookup_service_account(&env.pg_pool, user_id).await?;
        super::verify_authorization(env, &catalog_name, models::Capability::Admin).await?;

        let deleted = sqlx::query!(
            "DELETE FROM public.user_grants WHERE user_id = $1 AND object_role = $2",
            user_id,
            prefix.as_str(),
        )
        .execute(&env.pg_pool)
        .await?;

        if deleted.rows_affected() == 0 {
            return Err(async_graphql::Error::new("grant not found"));
        }

        tracing::info!(
            %user_id,
            %catalog_name,
            %prefix,
            %claims.sub,
            "removed service account grant"
        );

        Ok(true)
    }

    /// Create an API key for a service account.
    ///
    /// Returns the key_id and the plaintext secret (flow_sa_...).
    /// The secret is returned exactly once and cannot be retrieved again.
    ///
    /// The key is presented directly as an `Authorization: Bearer` credential.
    /// It is verified statefully against the database on every request and is
    /// never exchanged for an access token, so revoking it cuts off access
    /// immediately.
    async fn create_api_key(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "serviceAccountId")] user_id: uuid::Uuid,
        label: String,
        #[graphql(desc = "ISO 8601 duration for key validity (e.g. P90D, P1Y)")] valid_for: String,
    ) -> async_graphql::Result<CreateApiKeyResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        let catalog_name = lookup_service_account(&env.pg_pool, user_id).await?;
        super::verify_authorization(env, &catalog_name, models::Capability::Admin).await?;

        // valid_for is documented as an ISO 8601 duration (e.g. P90D, P1Y).
        // Reject anything that isn't ISO 8601 up front: the `::interval` cast
        // below would otherwise also accept Postgres's own syntax ("90 days"),
        // silently widening the contract and contradicting the field's docs and
        // error messages. ISO 8601 durations always start with 'P'; no Postgres
        // traditional unit does, so this prefix check cleanly distinguishes them.
        if !valid_for.trim_start().starts_with('P') {
            return Err(async_graphql::Error::new(
                "valid_for must be an ISO 8601 duration, e.g. P90D or P1Y",
            ));
        }

        // Bound the lifetime so a key can't become an effectively-permanent
        // credential, and require it to be positive. Postgres does the interval
        // math, which is calendar-aware for the P1Y / P3M cases.
        let within_bounds = sqlx::query_scalar!(
            r#"
            SELECT $1::text::interval > interval '0'
               AND $1::text::interval <= interval '1 year' AS "ok!: bool"
            "#,
            valid_for,
        )
        .fetch_one(&env.pg_pool)
        .await;

        let within_bounds = match within_bounds {
            Ok(ok) => ok,
            // A 'P'-prefixed but malformed duration still fails the `::interval`
            // cast (SQLSTATE 22007/22008); surface it as a client error, not a 500.
            Err(sqlx::Error::Database(db))
                if matches!(db.code().as_deref(), Some("22007") | Some("22008")) =>
            {
                return Err(async_graphql::Error::new(
                    "invalid valid_for: expected an ISO 8601 duration, e.g. P90D or P1Y",
                ));
            }
            Err(err) => return Err(err.into()),
        };

        if !within_bounds {
            return Err(async_graphql::Error::new(
                "valid_for must be a positive duration no greater than 1 year",
            ));
        }

        let row = sqlx::query!(
            r#"
            WITH new_key AS (
                SELECT
                    internal.id_generator() AS id,
                    -- 256 bits from pgcrypto's CSPRNG; the SHA-256 hashing
                    -- below rests on secrets being high-entropy.
                    encode(gen_random_bytes(32), 'hex') AS secret
            )
            INSERT INTO internal.api_keys (id, service_account_id, secret_hash, label, expires_at, created_by)
            SELECT
                nk.id,
                $1,
                -- SHA-256 rather than bcrypt: API keys are only ever
                -- verified statefully -- every bearer presentation re-checks
                -- this hash, and a key is never exchanged for a signed JWT --
                -- placing verification squarely in the per-request hot path,
                -- where a fast hash matters. A slow hash would buy nothing
                -- anyway: the secret is high-entropy random, and offline
                -- brute-force resistance only matters for low-entropy
                -- passwords. This matches how GitHub stores PATs.
                -- Refresh-token secrets will migrate to the same scheme once
                -- the legacy postgREST token functions are retired.
                encode(digest(nk.secret, 'sha256'), 'hex'),
                $2,
                now() + $3::text::interval,
                $4
            FROM new_key nk
            RETURNING
                id AS "id!: models::Id",
                (SELECT secret FROM new_key) AS "secret!: String"
            "#,
            user_id,
            label,
            valid_for,
            claims.sub,
        )
        .fetch_one(&env.pg_pool)
        .await?;

        use base64::Engine;
        let payload = format!("{}:{}", row.id, row.secret);
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.as_bytes());
        let full_secret = format!("flow_sa_{encoded}");

        tracing::info!(
            key_id = %row.id,
            %user_id,
            %label,
            %claims.sub,
            "created api key for service account"
        );

        Ok(CreateApiKeyResult {
            key_id: row.id,
            secret: full_secret,
        })
    }

    /// Revoke an API key.
    ///
    /// The caller must have admin capability on the owning service account's
    /// catalog name.
    ///
    /// Rather than deleting the row, we stamp `revoked_at`, which makes the key
    /// inert (excluded from bearer authentication and listings) while
    /// preserving the audit trail. Already-revoked keys are treated as not
    /// found.
    ///
    /// Revocation is immediate: keys are verified statefully on every request
    /// and never exchanged for access tokens, so there are no outstanding
    /// minted credentials to wait out. The next request presenting this key
    /// fails authentication.
    async fn revoke_api_key(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "id")] key_id: models::Id,
    ) -> async_graphql::Result<bool> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        let key_owner = sqlx::query!(
            r#"
            SELECT ak.service_account_id
            FROM internal.api_keys ak
            WHERE ak.id = $1 AND ak.revoked_at IS NULL
            "#,
            key_id as models::Id,
        )
        .fetch_optional(&env.pg_pool)
        .await?;

        let key_owner = match key_owner {
            Some(row) => row.service_account_id,
            None => return Err(async_graphql::Error::new("API key not found")),
        };

        let catalog_name = lookup_service_account(&env.pg_pool, key_owner).await?;
        super::verify_authorization(env, &catalog_name, models::Capability::Admin).await?;

        sqlx::query!(
            "UPDATE internal.api_keys SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL",
            key_id as models::Id
        )
        .execute(&env.pg_pool)
        .await?;

        tracing::info!(
            %key_id,
            service_account = %key_owner,
            %claims.sub,
            "revoked api key"
        );

        Ok(true)
    }

    /// Revoke ALL of a service account's API keys.
    ///
    /// The caller must manage the service account (admin capability on its
    /// catalog name).
    ///
    /// This is the kill switch for a compromised or retired account: API keys
    /// are its only means of authentication and are verified statefully on
    /// every request, so revoking them all cuts off access immediately — the
    /// account's next request fails authentication. Its user_grants are
    /// untouched and may be cleaned up separately via
    /// removeServiceAccountGrant.
    ///
    /// Idempotent: returns the number of keys revoked, zero if none remained.
    async fn revoke_api_keys(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "serviceAccountId")] user_id: uuid::Uuid,
    ) -> async_graphql::Result<i32> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        let catalog_name = lookup_service_account(&env.pg_pool, user_id).await?;
        super::verify_authorization(env, &catalog_name, models::Capability::Admin).await?;

        let revoked = sqlx::query!(
            r#"
            UPDATE internal.api_keys
            SET revoked_at = now()
            WHERE service_account_id = $1 AND revoked_at IS NULL
            "#,
            user_id,
        )
        .execute(&env.pg_pool)
        .await?;

        tracing::info!(
            %user_id,
            %catalog_name,
            revoked = revoked.rows_affected(),
            %claims.sub,
            "revoked all service account api keys"
        );

        Ok(revoked.rows_affected() as i32)
    }
}

/// Returns whether `user_id` is backed by a service account. Used to deny
/// SA principals operations reserved for human users (e.g. refresh tokens).
pub(super) async fn is_service_account(
    pg_pool: &sqlx::PgPool,
    user_id: uuid::Uuid,
) -> async_graphql::Result<bool> {
    let exists = sqlx::query_scalar!(
        r#"SELECT EXISTS(SELECT 1 FROM internal.service_accounts WHERE user_id = $1) AS "exists!""#,
        user_id,
    )
    .fetch_one(pg_pool)
    .await?;

    Ok(exists)
}

/// Look up a service account's management anchor: admins of a prefix covering
/// this catalog name may manage the account, independent of whatever its
/// user_grants authorize.
async fn lookup_service_account(
    pg_pool: &sqlx::PgPool,
    user_id: uuid::Uuid,
) -> async_graphql::Result<String> {
    let row = sqlx::query_scalar!(
        r#"
        SELECT catalog_name AS "catalog_name!: String"
        FROM internal.service_accounts
        WHERE user_id = $1
        "#,
        user_id,
    )
    .fetch_optional(pg_pool)
    .await?;

    row.ok_or_else(|| async_graphql::Error::new("service account not found"))
}

#[cfg(test)]
mod test {
    use crate::test_server;

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_service_account_lifecycle(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let alice_token = server.make_access_token(
            uuid::Uuid::from_bytes([0x11; 16]),
            Some("alice@example.test"),
        );

        // Create a bob user who does NOT have admin on aliceCo/.
        sqlx::query("INSERT INTO auth.users (id, email) VALUES ('22222222-2222-2222-2222-222222222222', 'bob@example.test')")
            .execute(&pool)
            .await
            .unwrap();

        let bob_token =
            server.make_access_token(uuid::Uuid::from_bytes([0x22; 16]), Some("bob@example.test"));

        // === Create a service account with multiple seeded grants ===
        let create_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($catalogName: Name!, $name: String!, $grants: [ServiceAccountGrantInput!]!) {
                        createServiceAccount(
                            catalogName: $catalogName
                            displayName: $name
                            grants: $grants
                        ) {
                            id
                            displayName
                            catalogName
                            createdBy
                            createdAt
                            updatedAt
                            lastUsedAt
                            apiKeys { id }
                        }
                    }"#,
                    "variables": {
                        "catalogName": "aliceCo/ci-deploy-bot",
                        "name": "CI Deploy Bot",
                        "grants": [
                            { "prefix": "aliceCo/", "capability": "admin" },
                            { "prefix": "aliceCo/data/", "capability": "read" }
                        ]
                    }
                }),
                Some(&alice_token),
            )
            .await;

        assert!(
            create_response["errors"].is_null(),
            "create should succeed: {create_response}"
        );
        let sa = &create_response["data"]["createServiceAccount"];
        let sa_user_id = sa["id"].as_str().expect("should have id");
        assert_eq!(sa["displayName"], "CI Deploy Bot");
        assert_eq!(sa["catalogName"], "aliceCo/ci-deploy-bot");
        assert_eq!(sa["apiKeys"].as_array().unwrap().len(), 0);
        assert_eq!(
            grant_count(&pool, sa_user_id).await,
            2,
            "each requested grant should mint a user_grants row"
        );
        // Provenance and timestamp fields are populated on creation: createdBy
        // is the calling admin (alice), the timestamps are set, and a freshly
        // created account has never been used.
        assert_eq!(
            sa["createdBy"], "11111111-1111-1111-1111-111111111111",
            "createdBy should be the calling admin: {create_response}"
        );
        assert!(sa["createdAt"].is_string(), "createdAt should be set: {sa}");
        assert!(sa["updatedAt"].is_string(), "updatedAt should be set: {sa}");
        assert!(
            sa["lastUsedAt"].is_null(),
            "a never-used account should have null lastUsedAt: {sa}"
        );

        // === Bob cannot create a service account for aliceCo/ ===
        let unauthorized: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createServiceAccount(
                            catalogName: "aliceCo/hacker-bot"
                            displayName: "hacker bot"
                            grants: [{ prefix: "aliceCo/", capability: read }]
                        ) { id }
                    }"#
                }),
                Some(&bob_token),
            )
            .await;

        assert!(unauthorized["errors"].is_array());

        // === create_service_account input validation ===
        // An invalid catalog name is rejected (before authorization), even
        // for an admin caller.
        let bad_name: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createServiceAccount(catalogName: "Not A Name", displayName: "x", grants: []) { id }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            bad_name["errors"][0]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("invalid catalog name"),
            "invalid catalog name should be rejected: {bad_name}"
        );

        // capability `none` confers no access until bundles are wired, so it is
        // rejected rather than minting a no-op grant.
        let none_capability: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createServiceAccount(
                            catalogName: "aliceCo/no-op-bot"
                            displayName: "x"
                            grants: [{ prefix: "aliceCo/", capability: none }]
                        ) { id }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            none_capability["errors"][0]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("capability must be one of"),
            "Capability::None should be rejected: {none_capability}"
        );

        // Every requested grant is independently authorized: alice admins
        // aliceCo/ but not bobCo/, so seeding a bobCo/ grant must fail even
        // though the account itself is homed under her prefix.
        let foreign_grant: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createServiceAccount(
                            catalogName: "aliceCo/overreach-bot"
                            displayName: "overreach bot"
                            grants: [
                                { prefix: "aliceCo/", capability: read },
                                { prefix: "bobCo/", capability: read }
                            ]
                        ) { id }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            foreign_grant["errors"].is_array(),
            "a grant to an unadministered prefix should be rejected: {foreign_grant}"
        );

        // === Create an API key ===
        let create_key: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($userId: UUID!, $label: String!, $validFor: String!) {
                        createApiKey(
                            serviceAccountId: $userId
                            label: $label
                            validFor: $validFor
                        ) {
                            id
                            secret
                        }
                    }"#,
                    "variables": {
                        "userId": sa_user_id,
                        "label": "GitHub Actions",
                        "validFor": "P90D"
                    }
                }),
                Some(&alice_token),
            )
            .await;

        assert!(
            create_key["errors"].is_null(),
            "create key should succeed: {create_key}"
        );
        let key_data = &create_key["data"]["createApiKey"];
        let key_id = key_data["id"].as_str().expect("should have id");
        let secret = key_data["secret"].as_str().expect("should have secret");
        assert!(secret.starts_with("flow_sa_"));

        // === valid_for validation ===
        // Each case must be rejected, and the error message identifies the
        // specific branch: non-ISO syntax, malformed ISO, non-positive, and
        // over the one-year cap.
        for (valid_for, want) in [
            ("90 days", "ISO 8601"),           // Postgres syntax, not ISO 8601
            ("Pfoo", "invalid valid_for"),     // 'P'-prefixed but unparseable
            ("P0D", "positive"),               // zero duration
            ("P2Y", "no greater than 1 year"), // exceeds the cap
        ] {
            let rejected: serde_json::Value = server
                .graphql(
                    &serde_json::json!({
                        "query": r#"
                        mutation($userId: UUID!, $label: String!, $validFor: String!) {
                            createApiKey(serviceAccountId: $userId, label: $label, validFor: $validFor) { id }
                        }"#,
                        "variables": {
                            "userId": sa_user_id,
                            "label": "bad valid_for",
                            "validFor": valid_for,
                        }
                    }),
                    Some(&alice_token),
                )
                .await;
            assert!(
                rejected["errors"][0]["message"]
                    .as_str()
                    .unwrap_or_default()
                    .contains(want),
                "valid_for {valid_for:?} should be rejected mentioning {want:?}: {rejected}"
            );
        }

        // === The API key authenticates directly as a bearer credential ===
        // The Envelope verifies it against the database and constructs claims
        // for the service account without minting (or verifying) a JWT. The
        // refreshTokens listing is empty (the account owns none), but a data
        // response proves authentication resolved to the account's identity.
        let via_bearer: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"query { refreshTokens { edges { node { id } } } }"#
                }),
                Some(secret),
            )
            .await;
        assert!(
            via_bearer["data"]["refreshTokens"]["edges"].is_array(),
            "bearer-authenticated request should succeed: {via_bearer}"
        );

        // === List service accounts ===
        let list: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        serviceAccounts {
                            edges {
                                node {
                                    id
                                    displayName
                                    catalogName
                                    apiKeys {
                                        id
                                        label
                                        createdBy
                                        createdAt
                                        expiresAt
                                        lastUsedAt
                                    }
                                }
                            }
                        }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;

        let edges = list["data"]["serviceAccounts"]["edges"]
            .as_array()
            .expect("should have edges");
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0]["node"]["displayName"], "CI Deploy Bot");
        let listed_key = &edges[0]["node"]["apiKeys"][0];
        assert_eq!(edges[0]["node"]["apiKeys"].as_array().unwrap().len(), 1);
        assert_eq!(listed_key["label"], "GitHub Actions");
        assert_eq!(
            listed_key["createdBy"], "11111111-1111-1111-1111-111111111111",
            "key createdBy should be the calling admin: {list}"
        );
        assert!(
            listed_key["createdAt"].is_string() && listed_key["expiresAt"].is_string(),
            "key createdAt/expiresAt should be set: {list}"
        );
        // The key was presented as a bearer credential above, so its
        // last_used_at is now populated — the stamp is fused into the bearer
        // verification query.
        assert!(
            listed_key["lastUsedAt"].is_string(),
            "lastUsedAt should be set after a successful bearer use: {list}"
        );

        // Bob sees no service accounts.
        let bob_list: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        serviceAccounts { edges { node { id } } }
                    }"#
                }),
                Some(&bob_token),
            )
            .await;

        let bob_edges = bob_list["data"]["serviceAccounts"]["edges"]
            .as_array()
            .expect("should have edges");
        assert_eq!(bob_edges.len(), 0);

        // === Revoke the API key ===
        let revoke: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($keyId: Id!) {
                        revokeApiKey(id: $keyId)
                    }"#,
                    "variables": { "keyId": key_id }
                }),
                Some(&alice_token),
            )
            .await;

        assert!(
            revoke["errors"].is_null(),
            "revoke should succeed: {revoke}"
        );

        // The row is preserved with revoked_at stamped — revocation is a soft
        // delete for audit purposes. The exchange and listing assertions below
        // can't observe this distinction, so check the table directly.
        let parsed_key_id: models::Id = key_id.parse().unwrap();
        let revoked_row = sqlx::query!(
            r#"SELECT revoked_at FROM internal.api_keys WHERE id = $1"#,
            parsed_key_id as models::Id,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            revoked_row.revoked_at.is_some(),
            "revocation must stamp revoked_at, not delete the row"
        );

        // Revoking again fails: already-revoked keys are treated as not found.
        let revoke_again: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($keyId: Id!) {
                        revokeApiKey(id: $keyId)
                    }"#,
                    "variables": { "keyId": key_id }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            revoke_again["errors"][0]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("API key not found"),
            "re-revoking should report not found: {revoke_again}"
        );

        // The revoked key no longer authenticates — immediately, since every
        // request re-verifies the credential against the database. Revoked
        // keys are excluded from the verification query, so this falls
        // through to the same 401 rejection as a nonexistent key.
        let rejected = server
            .rest_client()
            .post(
                "/api/graphql",
                &serde_json::json!({ "query": "query { refreshTokens { edges { node { id } } } }" }),
                Some(secret),
            )
            .send()
            .await
            .unwrap();
        let status = rejected.status();
        let body = rejected.text().await.unwrap();
        assert_eq!(
            status,
            reqwest::StatusCode::UNAUTHORIZED,
            "revoked key should be rejected with 401: {body}"
        );
        assert!(
            body.contains("invalid, expired, or revoked api key"),
            "revoked key rejection body: {body}"
        );

        // The revoked key is excluded from listings, even though its row remains.
        let list_after_revoke: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        serviceAccounts { edges { node { apiKeys { id } } } }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            list_after_revoke["data"]["serviceAccounts"]["edges"][0]["node"]["apiKeys"]
                .as_array()
                .unwrap()
                .len(),
            0,
            "revoked keys must not appear in listings: {list_after_revoke}"
        );

        // Count the service account's user_grants rows directly: the
        // grant-management assertions below observe access changes through
        // this, since bearer authentication succeeds whether or not grants
        // remain (access is wholly determined by user_grants).
        async fn grant_count(pool: &sqlx::PgPool, user_id: &str) -> i64 {
            sqlx::query_scalar!(
                r#"SELECT count(*) AS "count!" FROM public.user_grants WHERE user_id = $1"#,
                uuid::Uuid::parse_str(user_id).unwrap(),
            )
            .fetch_one(pool)
            .await
            .unwrap()
        }

        // === Grant management ===
        // Adding a grant requires BOTH managing the account and admin on the
        // granted prefix. Bob has neither, so he can't add a grant.
        let add_unmanaged: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($userId: UUID!) {
                        addServiceAccountGrant(serviceAccountId: $userId, prefix: "aliceCo/", capability: read)
                    }"#,
                    "variables": { "userId": sa_user_id }
                }),
                Some(&bob_token),
            )
            .await;
        assert!(
            add_unmanaged["errors"].is_array(),
            "a non-manager must not add grants: {add_unmanaged}"
        );

        // Alice manages the account but doesn't admin bobCo/: extending the
        // account's access beyond what she administers is denied.
        let add_foreign: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($userId: UUID!) {
                        addServiceAccountGrant(serviceAccountId: $userId, prefix: "bobCo/", capability: read)
                    }"#,
                    "variables": { "userId": sa_user_id }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            add_foreign["errors"].is_array(),
            "a grant to an unadministered prefix must be rejected: {add_foreign}"
        );

        // Happy path: alice manages the account and admins aliceCo/ops/.
        let add: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($userId: UUID!) {
                        addServiceAccountGrant(serviceAccountId: $userId, prefix: "aliceCo/ops/", capability: write)
                    }"#,
                    "variables": { "userId": sa_user_id }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(add["errors"].is_null(), "add grant should succeed: {add}");
        assert_eq!(grant_count(&pool, sa_user_id).await, 3);

        // Removal requires only account management — no capability on the
        // grant's prefix. Seed a grant to bobCo/ directly (adding one via the
        // API requires admin on it), then alice removes it despite having no
        // bobCo/ access of her own.
        sqlx::query("INSERT INTO user_grants (user_id, object_role, capability) VALUES ($1, 'bobCo/', 'read')")
            .bind(uuid::Uuid::parse_str(sa_user_id).unwrap())
            .execute(&pool)
            .await
            .unwrap();

        let remove_foreign: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($userId: UUID!) {
                        removeServiceAccountGrant(serviceAccountId: $userId, prefix: "bobCo/")
                    }"#,
                    "variables": { "userId": sa_user_id }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            remove_foreign["errors"].is_null(),
            "a manager may remove ANY grant, including one to a prefix they don't administer: {remove_foreign}"
        );
        assert_eq!(grant_count(&pool, sa_user_id).await, 3);

        // Removing an absent grant reports not found.
        let remove_again: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($userId: UUID!) {
                        removeServiceAccountGrant(serviceAccountId: $userId, prefix: "bobCo/")
                    }"#,
                    "variables": { "userId": sa_user_id }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            remove_again["errors"][0]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("grant not found"),
            "re-removing should report not found: {remove_again}"
        );

        // Bob cannot remove grants of an account he doesn't manage.
        let remove_unmanaged: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($userId: UUID!) {
                        removeServiceAccountGrant(serviceAccountId: $userId, prefix: "aliceCo/data/")
                    }"#,
                    "variables": { "userId": sa_user_id }
                }),
                Some(&bob_token),
            )
            .await;
        assert!(remove_unmanaged["errors"].is_array());

        // === Bulk key revocation is the kill switch ===
        // Mint a fresh key (the first was revoked above) and prove it works.
        let create_key2: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($userId: UUID!, $label: String!, $validFor: String!) {
                        createApiKey(serviceAccountId: $userId, label: $label, validFor: $validFor) {
                            id
                            secret
                        }
                    }"#,
                    "variables": {
                        "userId": sa_user_id,
                        "label": "temp key",
                        "validFor": "P30D"
                    }
                }),
                Some(&alice_token),
            )
            .await;
        let secret2 = create_key2["data"]["createApiKey"]["secret"]
            .as_str()
            .unwrap();

        let live: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"query { refreshTokens { edges { node { id } } } }"#
                }),
                Some(secret2),
            )
            .await;
        assert!(
            live["data"]["refreshTokens"]["edges"].is_array(),
            "fresh key should authenticate: {live}"
        );

        // Bob cannot bulk-revoke keys of an account he doesn't manage.
        let bulk_unmanaged: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($userId: UUID!) {
                        revokeApiKeys(serviceAccountId: $userId)
                    }"#,
                    "variables": { "userId": sa_user_id }
                }),
                Some(&bob_token),
            )
            .await;
        assert!(bulk_unmanaged["errors"].is_array());

        // Revoking every key disables the account: keys are its only means
        // of authentication, and they're re-verified on every request, so the
        // cutoff is immediate.
        let bulk: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($userId: UUID!) {
                        revokeApiKeys(serviceAccountId: $userId)
                    }"#,
                    "variables": { "userId": sa_user_id }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            bulk["errors"].is_null(),
            "bulk revoke should succeed: {bulk}"
        );
        assert_eq!(
            bulk["data"]["revokeApiKeys"].as_i64(),
            Some(1),
            "the one live key is revoked: {bulk}"
        );

        let rejected = server
            .rest_client()
            .post(
                "/api/graphql",
                &serde_json::json!({ "query": "query { refreshTokens { edges { node { id } } } }" }),
                Some(secret2),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(rejected.status(), reqwest::StatusCode::UNAUTHORIZED);

        // Revoking again is an idempotent no-op reporting zero keys revoked.
        let bulk_again: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($userId: UUID!) {
                        revokeApiKeys(serviceAccountId: $userId)
                    }"#,
                    "variables": { "userId": sa_user_id }
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            bulk_again["data"]["revokeApiKeys"].as_i64(),
            Some(0),
            "repeat bulk revoke is a no-op: {bulk_again}"
        );
    }
}
