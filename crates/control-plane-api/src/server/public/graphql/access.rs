use super::{TimestampCursor, filters};
use async_graphql::{Context, types::connection};

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct ServiceAccount {
    // Exposed as `id`: a service account's identifier happens to be its
    // backing auth.users id, but that's an implementation detail we don't
    // surface in the public schema.
    #[graphql(name = "id")]
    pub user_id: uuid::Uuid,
    pub display_name: String,
    pub prefix: models::Prefix,
    pub capability: models::Capability,
    pub created_by: uuid::Uuid,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
    pub disabled_at: Option<chrono::DateTime<chrono::Utc>>,
    pub api_keys: Vec<ApiKeyInfo>,
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

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct RefreshTokenResult {
    pub id: models::Id,
    pub secret: String,
}

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct RefreshTokenInfo {
    pub id: models::Id,
    pub detail: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub multi_use: bool,
    pub valid_for: String,
    pub uses: i32,
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

pub type PaginatedRefreshTokens = connection::Connection<
    TimestampCursor,
    RefreshTokenInfo,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct ServiceAccountsFilter {
    pub catalog_prefix: Option<filters::PrefixFilter>,
}

#[derive(Debug, Default)]
pub struct AccessQuery;

const DEFAULT_PAGE_SIZE: usize = 25;
const MAX_PREFIXES: usize = 20;

#[async_graphql::Object]
impl AccessQuery {
    async fn service_accounts(
        &self,
        ctx: &Context<'_>,
        filter: Option<ServiceAccountsFilter>,
        after: Option<String>,
        first: Option<i32>,
    ) -> async_graphql::Result<PaginatedServiceAccounts> {
        let env = ctx.data::<crate::Envelope>()?;

        let prefix_starts_with = filter
            .and_then(|f| f.catalog_prefix)
            .and_then(|f| f.starts_with);

        let admin_prefixes = super::authorized_prefixes::authorized_prefixes(
            &env.snapshot().role_grants,
            &env.snapshot().user_grants,
            env.claims()?.sub,
            models::Capability::Admin,
            prefix_starts_with.as_deref(),
        );

        if admin_prefixes.is_empty() {
            return Ok(PaginatedServiceAccounts::new(false, false));
        }
        if admin_prefixes.len() > MAX_PREFIXES {
            return Err(async_graphql::Error::new(
                "Too many admin prefixes; narrow results with a prefix filter",
            ));
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
                        sa.prefix AS "prefix!: String",
                        sa.capability AS "capability!: models::Capability",
                        sa.created_by,
                        sa.created_at AS "created_at!: chrono::DateTime<chrono::Utc>",
                        sa.updated_at AS "updated_at!: chrono::DateTime<chrono::Utc>",
                        sa.last_used_at AS "last_used_at: chrono::DateTime<chrono::Utc>",
                        sa.disabled_at AS "disabled_at: chrono::DateTime<chrono::Utc>"
                    FROM internal.service_accounts sa
                    WHERE sa.prefix::text ^@ ANY($1)
                      AND ($2::timestamptz IS NULL OR sa.created_at < $2)
                    ORDER BY sa.created_at DESC
                    LIMIT $3 + 1
                    "#,
                    &admin_prefixes,
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
                                prefix: models::Prefix::new(&r.prefix),
                                capability: r.capability,
                                created_by: r.created_by,
                                created_at: r.created_at,
                                updated_at: r.updated_at,
                                last_used_at: r.last_used_at,
                                disabled_at: r.disabled_at,
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

    /// List refresh tokens owned by the authenticated user.
    async fn refresh_tokens(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        first: Option<i32>,
    ) -> async_graphql::Result<PaginatedRefreshTokens> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        connection::query_with::<TimestampCursor, _, _, _, async_graphql::Error>(
            after,
            None,
            first,
            None,
            |after, _, first, _| async move {
                let after_created_at = after.map(|c| c.0);
                let limit = first.unwrap_or(DEFAULT_PAGE_SIZE);

                let rows = sqlx::query!(
                    r#"
                    SELECT
                        id AS "id!: models::Id",
                        detail,
                        created_at AS "created_at!: chrono::DateTime<chrono::Utc>",
                        updated_at AS "updated_at!: chrono::DateTime<chrono::Utc>",
                        multi_use AS "multi_use!: bool",
                        valid_for::text AS "valid_for!: String",
                        uses AS "uses!: i32"
                    FROM refresh_tokens
                    WHERE user_id = $1
                      AND ($2::timestamptz IS NULL OR created_at < $2)
                    ORDER BY created_at DESC
                    LIMIT $3 + 1
                    "#,
                    claims.sub,
                    after_created_at,
                    limit as i64,
                )
                .fetch_all(&env.pg_pool)
                .await?;

                let has_next = rows.len() > limit;

                let edges: Vec<_> = rows
                    .into_iter()
                    .take(limit)
                    .map(|r| {
                        connection::Edge::new(
                            TimestampCursor(r.created_at),
                            RefreshTokenInfo {
                                id: r.id,
                                detail: r.detail,
                                created_at: r.created_at,
                                updated_at: r.updated_at,
                                multi_use: r.multi_use,
                                valid_for: r.valid_for,
                                uses: r.uses,
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
pub struct AccessMutation;

#[async_graphql::Object]
impl AccessMutation {
    /// Create a service account with a grant to the specified prefix.
    ///
    /// The caller must have admin capability on the prefix.
    /// Creates an auth.users row, an internal.service_accounts row,
    /// and a user_grants row for the service account.
    async fn create_service_account(
        &self,
        ctx: &Context<'_>,
        prefix: models::Prefix,
        capability: models::Capability,
        display_name: String,
    ) -> async_graphql::Result<ServiceAccount> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        if let Err(err) = validator::Validate::validate(&prefix) {
            return Err(async_graphql::Error::new(format!(
                "invalid catalog prefix: {err}"
            )));
        }

        // `none` is permitted by the table's check constraint (reserved for the
        // future bundles-only path) but confers no access until bundles are
        // wired, so reject it here rather than mint a no-op grant.
        if capability == models::Capability::None {
            return Err(async_graphql::Error::new(
                "capability must be one of: read, write, admin",
            ));
        }

        super::verify_authorization(env, prefix.as_str(), models::Capability::Admin).await?;

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
            INSERT INTO internal.service_accounts (user_id, prefix, capability, display_name, created_by)
            VALUES ($1, $2::text::catalog_prefix, $3, $4, $5)
            RETURNING created_at AS "created_at!: chrono::DateTime<chrono::Utc>"
            "#,
            sa_user_id,
            prefix.as_str(),
            capability as models::Capability,
            display_name,
            claims.sub,
        )
        .fetch_one(&mut *txn)
        .await?;

        crate::grants::upsert_user_grant(
            sa_user_id,
            prefix.as_str(),
            capability,
            Some("service account grant".to_string()),
            &mut txn,
        )
        .await?;

        txn.commit().await?;

        tracing::info!(
            %prefix,
            ?capability,
            %claims.sub,
            %sa_user_id,
            "created service account"
        );

        Ok(ServiceAccount {
            user_id: sa_user_id,
            display_name,
            prefix,
            capability,
            created_by: claims.sub,
            created_at: now,
            updated_at: now,
            last_used_at: None,
            disabled_at: None,
            api_keys: vec![],
        })
    }

    /// Disable a service account, revoking all API keys and grants.
    ///
    /// The caller must have admin capability on the service account's prefix.
    /// The auth.users row is preserved for audit trail / FK integrity.
    ///
    /// Unlike revoking a single key, disabling removes the service account's
    /// grants, so access tokens already issued from its keys resolve to zero
    /// capability on their next authorization check (bounded by snapshot-refresh
    /// lag, not the token's full ~1h lifetime). Use this to cut off an
    /// active service account, not just stop new tokens.
    ///
    /// Idempotent: returns `true` if this call disabled the account, `false`
    /// if it was already disabled.
    async fn disable_service_account(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "id")] user_id: uuid::Uuid,
    ) -> async_graphql::Result<bool> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        let sa = lookup_service_account(&env.pg_pool, user_id).await?;
        super::verify_authorization(env, &sa.prefix, models::Capability::Admin).await?;

        let mut txn = env.pg_pool.begin().await?;

        // The conditional UPDATE only matches a currently-enabled account, so
        // concurrent disables serialize on the row and exactly one performs the
        // transition. A caller that finds it already disabled returns a no-op
        // success (`false`) and skips the cleanup below.
        let disabled = sqlx::query!(
            "UPDATE internal.service_accounts SET disabled_at = now(), updated_at = now() WHERE user_id = $1 AND disabled_at IS NULL",
            user_id,
        )
        .execute(&mut *txn)
        .await?;

        if disabled.rows_affected() == 0 {
            return Ok(false);
        }

        sqlx::query!(
            "DELETE FROM internal.api_keys WHERE service_account_id = $1",
            user_id
        )
        .execute(&mut *txn)
        .await?;

        sqlx::query!("DELETE FROM public.user_grants WHERE user_id = $1", user_id)
            .execute(&mut *txn)
            .await?;

        txn.commit().await?;

        tracing::info!(
            %user_id,
            prefix = %sa.prefix,
            %claims.sub,
            "disabled service account"
        );

        Ok(true)
    }

    /// Re-enable a disabled service account, restoring its user_grants row.
    ///
    /// Does NOT restore previously revoked API keys — new ones must be minted.
    ///
    /// Idempotent: returns `true` if this call enabled the account, `false`
    /// if it was not disabled.
    async fn enable_service_account(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "id")] user_id: uuid::Uuid,
    ) -> async_graphql::Result<bool> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        let sa = lookup_service_account(&env.pg_pool, user_id).await?;
        super::verify_authorization(env, &sa.prefix, models::Capability::Admin).await?;

        let mut txn = env.pg_pool.begin().await?;

        // The conditional UPDATE only matches a currently-disabled account, so
        // concurrent enables serialize on the row and exactly one performs the
        // transition. A caller that finds it already enabled returns a no-op
        // success (`false`) and skips restoring the grant below.
        let enabled = sqlx::query!(
            "UPDATE internal.service_accounts SET disabled_at = NULL, updated_at = now() WHERE user_id = $1 AND disabled_at IS NOT NULL",
            user_id,
        )
        .execute(&mut *txn)
        .await?;

        if enabled.rows_affected() == 0 {
            return Ok(false);
        }

        crate::grants::upsert_user_grant(
            user_id,
            &sa.prefix,
            sa.capability,
            Some("service account grant".to_string()),
            &mut txn,
        )
        .await?;

        txn.commit().await?;

        tracing::info!(
            %user_id,
            prefix = %sa.prefix,
            %claims.sub,
            "enabled service account"
        );

        Ok(true)
    }

    /// Create an API key for a service account.
    ///
    /// Returns the key_id and the plaintext secret (flow_sa_...).
    /// The secret is returned exactly once and cannot be retrieved again.
    async fn create_api_key(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "serviceAccountId")] user_id: uuid::Uuid,
        label: String,
        #[graphql(desc = "ISO 8601 duration for key validity (e.g. P90D, P1Y)")] valid_for: String,
    ) -> async_graphql::Result<CreateApiKeyResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        let sa = lookup_service_account(&env.pg_pool, user_id).await?;
        super::verify_authorization(env, &sa.prefix, models::Capability::Admin).await?;

        // Fast-path rejection that avoids opening a transaction for an account
        // that's already visibly disabled. The authoritative check happens
        // below under a row lock.
        if sa.disabled_at.is_some() {
            return Err(async_graphql::Error::new(
                "cannot create API key for a disabled service account",
            ));
        }

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

        // Lock the service-account row, then re-check disabled state and insert
        // the key in the same transaction. `disable_service_account` takes the
        // same row lock via its UPDATE, so the two cannot interleave: without
        // this, a disable committing between the check and the INSERT would
        // leave an orphan key that becomes live on a later enable.
        let mut txn = env.pg_pool.begin().await?;

        let disabled_at = sqlx::query_scalar!(
            "SELECT disabled_at FROM internal.service_accounts WHERE user_id = $1 FOR UPDATE",
            user_id,
        )
        .fetch_one(&mut *txn)
        .await?;

        if disabled_at.is_some() {
            return Err(async_graphql::Error::new(
                "cannot create API key for a disabled service account",
            ));
        }

        let row = sqlx::query!(
            r#"
            WITH new_key AS (
                SELECT
                    internal.id_generator() AS id,
                    gen_random_uuid()::text AS secret
            )
            INSERT INTO internal.api_keys (id, service_account_id, secret_hash, label, expires_at, created_by)
            SELECT
                nk.id,
                $1,
                crypt(nk.secret, gen_salt('bf')),
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
        .fetch_one(&mut *txn)
        .await?;

        txn.commit().await?;

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

    /// Revoke (delete) an API key.
    ///
    /// The caller must have admin capability on the owning service account's prefix.
    ///
    /// Revocation stops *new* access tokens from being minted with this key, but
    /// does not invalidate access tokens already issued from it — those remain
    /// valid until they expire (up to ~1h), since the service account's grants
    /// are untouched. To cut off active sessions immediately, disable the
    /// service account (which removes its grants).
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
            WHERE ak.id = $1
            "#,
            key_id as models::Id,
        )
        .fetch_optional(&env.pg_pool)
        .await?;

        let key_owner = match key_owner {
            Some(row) => row.service_account_id,
            None => return Err(async_graphql::Error::new("API key not found")),
        };

        let sa = lookup_service_account(&env.pg_pool, key_owner).await?;
        super::verify_authorization(env, &sa.prefix, models::Capability::Admin).await?;

        let result = sqlx::query!(
            "DELETE FROM internal.api_keys WHERE id = $1",
            key_id as models::Id
        )
        .execute(&env.pg_pool)
        .await?;

        // The key existed when we looked up its owner above; a zero-row delete
        // means it was concurrently revoked. Report not-found rather than a
        // misleading success, matching delete_refresh_token.
        if result.rows_affected() == 0 {
            return Err(async_graphql::Error::new("API key not found"));
        }

        tracing::info!(
            %key_id,
            service_account = %key_owner,
            %claims.sub,
            "revoked api key"
        );

        Ok(true)
    }

    /// Create a refresh token for the authenticated user.
    async fn create_refresh_token(
        &self,
        ctx: &Context<'_>,
        #[graphql(
            desc = "ISO 8601 duration for token validity (e.g. P90D)",
            default_with = "String::from(\"P90D\")"
        )]
        valid_for: String,
        #[graphql(default = true)] multi_use: bool,
        #[graphql(default)] detail: Option<String>,
    ) -> async_graphql::Result<RefreshTokenResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        // Service accounts authenticate exclusively via API keys, which are
        // expiring, revocable, and respect the account's disabled state. A
        // refresh token bypasses all three, so deny issuance to SA principals.
        if is_service_account(&env.pg_pool, claims.sub).await? {
            return Err(async_graphql::Error::new(
                "service accounts cannot create refresh tokens; authenticate with an API key instead",
            ));
        }

        // valid_for is documented as an ISO 8601 duration (e.g. P90D). Reject
        // anything that isn't up front: the `::interval` cast below would
        // otherwise also accept Postgres's own syntax ("90 days"), silently
        // widening the contract. ISO 8601 durations always start with 'P'; no
        // Postgres traditional unit does. Mirrors create_api_key.
        if !valid_for.trim_start().starts_with('P') {
            return Err(async_graphql::Error::new(
                "valid_for must be an ISO 8601 duration, e.g. P90D",
            ));
        }

        let row = sqlx::query!(
            r#"
            WITH new_token AS (
                SELECT gen_random_uuid()::text AS secret
            )
            INSERT INTO refresh_tokens (user_id, multi_use, valid_for, hash, detail)
            SELECT
                $1,
                $2,
                $3::text::interval,
                crypt(nt.secret, gen_salt('bf')),
                $4
            FROM new_token nt
            RETURNING
                id AS "id!: models::Id",
                (SELECT secret FROM new_token) AS "secret!: String"
            "#,
            claims.sub,
            multi_use,
            valid_for,
            detail.as_deref(),
        )
        .fetch_one(&env.pg_pool)
        .await;

        let row = match row {
            Ok(row) => row,
            // A 'P'-prefixed but malformed duration still fails the `::interval`
            // cast (SQLSTATE 22007/22008); surface it as a client error, not a 500.
            Err(sqlx::Error::Database(db))
                if matches!(db.code().as_deref(), Some("22007") | Some("22008")) =>
            {
                return Err(async_graphql::Error::new(
                    "invalid valid_for: expected an ISO 8601 duration, e.g. P90D",
                ));
            }
            Err(err) => return Err(err.into()),
        };

        tracing::info!(
            refresh_token_id = %row.id,
            %claims.sub,
            "created refresh token"
        );

        Ok(RefreshTokenResult {
            id: row.id,
            secret: row.secret,
        })
    }

    /// Delete a refresh token owned by the authenticated user.
    async fn delete_refresh_token(
        &self,
        ctx: &Context<'_>,
        id: models::Id,
    ) -> async_graphql::Result<bool> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        let result = sqlx::query!(
            "DELETE FROM refresh_tokens WHERE id = $1 AND user_id = $2",
            id as models::Id,
            claims.sub,
        )
        .execute(&env.pg_pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(async_graphql::Error::new("refresh token not found"));
        }

        tracing::info!(
            refresh_token_id = %id,
            %claims.sub,
            "deleted refresh token"
        );

        Ok(true)
    }
}

struct ServiceAccountRow {
    prefix: String,
    capability: models::Capability,
    disabled_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Returns whether `user_id` is backed by a service account. Used to deny
/// SA principals operations reserved for human users (e.g. refresh tokens).
async fn is_service_account(
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

async fn lookup_service_account(
    pg_pool: &sqlx::PgPool,
    user_id: uuid::Uuid,
) -> async_graphql::Result<ServiceAccountRow> {
    let row = sqlx::query!(
        r#"
        SELECT
            prefix AS "prefix!: String",
            capability AS "capability!: models::Capability",
            disabled_at AS "disabled_at: chrono::DateTime<chrono::Utc>"
        FROM internal.service_accounts
        WHERE user_id = $1
        "#,
        user_id,
    )
    .fetch_optional(pg_pool)
    .await?;

    match row {
        Some(r) => Ok(ServiceAccountRow {
            prefix: r.prefix,
            capability: r.capability,
            disabled_at: r.disabled_at,
        }),
        None => Err(async_graphql::Error::new("service account not found")),
    }
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

        // === Create a service account ===
        let create_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($prefix: Prefix!, $capability: Capability!, $name: String!) {
                        createServiceAccount(
                            prefix: $prefix
                            capability: $capability
                            displayName: $name
                        ) {
                            id
                            displayName
                            prefix
                            capability
                            createdBy
                            createdAt
                            updatedAt
                            lastUsedAt
                            disabledAt
                            apiKeys { id }
                        }
                    }"#,
                    "variables": {
                        "prefix": "aliceCo/",
                        "capability": "admin",
                        "name": "CI Deploy Bot"
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
        assert_eq!(sa["prefix"], "aliceCo/");
        assert_eq!(sa["capability"], "admin");
        assert!(sa["disabledAt"].is_null());
        assert_eq!(sa["apiKeys"].as_array().unwrap().len(), 0);
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
                            prefix: "aliceCo/"
                            capability: read
                            displayName: "hacker bot"
                        ) { id }
                    }"#
                }),
                Some(&bob_token),
            )
            .await;

        assert!(unauthorized["errors"].is_array());

        // === create_service_account input validation ===
        // An invalid catalog prefix is rejected (before authorization), even
        // for an admin caller.
        let bad_prefix: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createServiceAccount(prefix: "Not A Prefix", capability: read, displayName: "x") { id }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            bad_prefix["errors"][0]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("invalid catalog prefix"),
            "invalid prefix should be rejected: {bad_prefix}"
        );

        // capability `none` confers no access until bundles are wired, so it is
        // rejected rather than minting a no-op grant.
        let none_capability: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createServiceAccount(prefix: "aliceCo/", capability: none, displayName: "x") { id }
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
            ("90 days", "ISO 8601"),               // Postgres syntax, not ISO 8601
            ("Pfoo", "invalid valid_for"),         // 'P'-prefixed but unparseable
            ("P0D", "positive"),                   // zero duration
            ("P2Y", "no greater than 1 year"),     // exceeds the cap
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

        // === Exchange the API key for an access token ===
        let exchange_result: serde_json::Value = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &serde_json::json!({
                    "grant_type": "api_key",
                    "api_key": secret,
                }),
                None,
            )
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(exchange_result["access_token"].is_string());

        // === List service accounts ===
        let list: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        serviceAccounts(filter: { catalogPrefix: { startsWith: "aliceCo/" } }) {
                            edges {
                                node {
                                    id
                                    displayName
                                    prefix
                                    capability
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
        // The key was exchanged above, so its last_used_at is now populated —
        // this also exercises the best-effort last_used_at write on exchange.
        assert!(
            listed_key["lastUsedAt"].is_string(),
            "lastUsedAt should be set after a successful exchange: {list}"
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

        // Exchanging the revoked key fails: the key no longer matches any row,
        // so verification falls through to a 401 "invalid or expired" rejection.
        let exchange_fail = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &serde_json::json!({
                    "grant_type": "api_key",
                    "api_key": secret,
                }),
                None,
            )
            .send()
            .await
            .unwrap();
        let status = exchange_fail.status();
        let body = exchange_fail.text().await.unwrap();
        assert_eq!(
            status,
            reqwest::StatusCode::UNAUTHORIZED,
            "revoked key should be rejected with 401: {body}"
        );
        assert!(
            body.contains("invalid or expired api key"),
            "revoked key rejection body: {body}"
        );

        // === Create a new key and then disable the service account ===
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

        // Count the service account's user_grants rows directly. The grant
        // deletion is the disable-vs-revoke differentiator (it drops already
        // issued tokens to zero capability on their next authz check), and the
        // token-exchange assertions can't observe it: those fail on key
        // deletion alone and would pass even if the grant were left in place.
        async fn grant_count(pool: &sqlx::PgPool, user_id: &str) -> i64 {
            sqlx::query_scalar!(
                r#"SELECT count(*) AS "count!" FROM public.user_grants WHERE user_id = $1"#,
                uuid::Uuid::parse_str(user_id).unwrap(),
            )
            .fetch_one(pool)
            .await
            .unwrap()
        }

        assert_eq!(
            grant_count(&pool, sa_user_id).await,
            1,
            "service account should have a grant before disable"
        );

        let disable: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($userId: UUID!) {
                        disableServiceAccount(id: $userId)
                    }"#,
                    "variables": { "userId": sa_user_id }
                }),
                Some(&alice_token),
            )
            .await;

        assert!(
            disable["errors"].is_null(),
            "disable should succeed: {disable}"
        );
        assert_eq!(
            disable["data"]["disableServiceAccount"].as_bool(),
            Some(true),
            "first disable performs the transition: {disable}"
        );

        assert_eq!(
            grant_count(&pool, sa_user_id).await,
            0,
            "disable must delete the user_grants row, not just the keys"
        );

        // API key from the disabled account fails. Disable deleted the account's
        // keys, so the exchange falls through to the same 401 "invalid or
        // expired" path as a revoked key (the dedicated disabled-account branch
        // in exchange_api_key is unreachable once the keys are gone).
        let exchange_disabled = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &serde_json::json!({
                    "grant_type": "api_key",
                    "api_key": secret2,
                }),
                None,
            )
            .send()
            .await
            .unwrap();
        let status = exchange_disabled.status();
        let body = exchange_disabled.text().await.unwrap();
        assert_eq!(
            status,
            reqwest::StatusCode::UNAUTHORIZED,
            "key for disabled account should be rejected with 401: {body}"
        );
        assert!(
            body.contains("invalid or expired api key"),
            "disabled-account key rejection body: {body}"
        );

        // Cannot create key for disabled account.
        let key_while_disabled: serde_json::Value = server
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
                        "label": "should fail",
                        "validFor": "P30D"
                    }
                }),
                Some(&alice_token),
            )
            .await;

        assert!(key_while_disabled["errors"].is_array());

        // Disabling again is an idempotent no-op: success, reporting no change.
        let disable_again: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($userId: UUID!) {
                        disableServiceAccount(id: $userId)
                    }"#,
                    "variables": { "userId": sa_user_id }
                }),
                Some(&alice_token),
            )
            .await;

        assert!(
            disable_again["errors"].is_null(),
            "repeat disable should not error: {disable_again}"
        );
        assert_eq!(
            disable_again["data"]["disableServiceAccount"].as_bool(),
            Some(false),
            "repeat disable is a no-op: {disable_again}"
        );

        // === Re-enable the service account ===
        let enable: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($userId: UUID!) {
                        enableServiceAccount(id: $userId)
                    }"#,
                    "variables": { "userId": sa_user_id }
                }),
                Some(&alice_token),
            )
            .await;

        assert!(
            enable["errors"].is_null(),
            "enable should succeed: {enable}"
        );
        assert_eq!(
            enable["data"]["enableServiceAccount"].as_bool(),
            Some(true),
            "first enable performs the transition: {enable}"
        );

        assert_eq!(
            grant_count(&pool, sa_user_id).await,
            1,
            "enable must restore the user_grants row"
        );

        // Re-enabled account can have new keys created.
        let key_after_enable: serde_json::Value = server
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
                        "label": "post-enable key",
                        "validFor": "P90D"
                    }
                }),
                Some(&alice_token),
            )
            .await;

        assert!(
            key_after_enable["errors"].is_null(),
            "create key after enable should succeed: {key_after_enable}"
        );

        let secret3 = key_after_enable["data"]["createApiKey"]["secret"]
            .as_str()
            .unwrap();

        // Exchange works again.
        let exchange_reenabled = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &serde_json::json!({
                    "grant_type": "api_key",
                    "api_key": secret3,
                }),
                None,
            )
            .send()
            .await
            .unwrap();
        assert!(exchange_reenabled.status().is_success());

        // Enabling an already-enabled account is an idempotent no-op.
        let enable_again: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($userId: UUID!) {
                        enableServiceAccount(id: $userId)
                    }"#,
                    "variables": { "userId": sa_user_id }
                }),
                Some(&alice_token),
            )
            .await;

        assert!(
            enable_again["errors"].is_null(),
            "repeat enable should not error: {enable_again}"
        );
        assert_eq!(
            enable_again["data"]["enableServiceAccount"].as_bool(),
            Some(false),
            "repeat enable is a no-op: {enable_again}"
        );
    }

    /// Covers the refresh-token GraphQL surface (create → list → delete, plus
    /// the not-found idempotency guard), the `/api/v1/auth/token`
    /// refresh-token dispatch, and the guard denying refresh tokens to
    /// service-account principals.
    ///
    /// The happy-path *exchange* — `generate_access_token` actually signing a
    /// JWT — is intentionally not exercised here: it reads `app.jwt_secret` from
    /// `vault.decrypted_secrets` and calls pgjwt's `sign()`, neither of which
    /// exists in the sqlx::test DB (only `auth`/`stripe` are polyfilled). That
    /// signing path is covered by the pgTAP `test_generate_access_token`. We
    /// instead assert the endpoint routes the `refresh_token` grant and rejects
    /// a bad secret — which fails in `generate_access_token` *before* signing,
    /// so it's deterministic without the vault/pgjwt setup.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_refresh_token_management(pool: sqlx::PgPool) {
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

        // === Create a refresh token ===
        let create: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createRefreshToken(validFor: "P30D", detail: "test token") {
                            id
                            secret
                        }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;

        assert!(
            create["errors"].is_null(),
            "create should succeed: {create}"
        );
        let created = &create["data"]["createRefreshToken"];
        let token_id = created["id"].as_str().expect("should have id").to_string();
        assert!(
            created["secret"].as_str().is_some(),
            "should return a secret"
        );

        // === List refresh tokens (scoped to the authenticated user) ===
        let list: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        refreshTokens {
                            edges { node { id detail multiUse uses } }
                        }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;

        let edges = list["data"]["refreshTokens"]["edges"]
            .as_array()
            .expect("should have edges");
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0]["node"]["id"], token_id);
        assert_eq!(edges[0]["node"]["detail"], "test token");
        assert_eq!(edges[0]["node"]["multiUse"], true);
        assert_eq!(edges[0]["node"]["uses"], 0);

        // === The endpoint dispatches the refresh_token grant ===
        // A wrong secret is rejected inside generate_access_token (before it
        // reaches signing), so this exercises routing + error-shaping for the
        // refresh-token branch without depending on the vault/pgjwt signing path.
        let bad_secret = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &serde_json::json!({
                    "grant_type": "refresh_token",
                    "refresh_token_id": token_id,
                    "secret": "not-the-real-secret",
                }),
                None,
            )
            .send()
            .await
            .unwrap();
        let status = bad_secret.status();
        let body = bad_secret.text().await.unwrap();
        assert_eq!(
            status,
            reqwest::StatusCode::UNAUTHORIZED,
            "bad refresh secret should be rejected with 401: {body}"
        );
        assert!(
            body.contains("failed to exchange refresh token"),
            "bad refresh secret rejection body: {body}"
        );

        // === Delete the refresh token ===
        let delete: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation($id: Id!) { deleteRefreshToken(id: $id) }"#,
                    "variables": { "id": token_id }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            delete["errors"].is_null(),
            "delete should succeed: {delete}"
        );
        assert_eq!(delete["data"]["deleteRefreshToken"], true);

        // It's gone from the list.
        let list_after: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"query { refreshTokens { edges { node { id } } } }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            list_after["data"]["refreshTokens"]["edges"]
                .as_array()
                .unwrap()
                .len(),
            0
        );

        // Deleting again fails (not-found guard).
        let delete_again: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation($id: Id!) { deleteRefreshToken(id: $id) }"#,
                    "variables": { "id": token_id }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(delete_again["errors"].is_array());

        // === Service accounts cannot create refresh tokens ===
        // They authenticate via API keys, which are expiring, revocable, and
        // respect the account's disabled state; a refresh token would bypass
        // all three, so issuance to an SA principal must be denied.
        let create_sa: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createServiceAccount(
                            prefix: "aliceCo/"
                            capability: admin
                            displayName: "refresh-token bot"
                        ) { id }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            create_sa["errors"].is_null(),
            "create SA should succeed: {create_sa}"
        );
        let sa_user_id = create_sa["data"]["createServiceAccount"]["id"]
            .as_str()
            .expect("should have id");

        // Mint an access token whose `sub` is the service account, mirroring
        // what `exchange_api_key` issues (no email for an SA principal).
        let sa_token =
            server.make_access_token(uuid::Uuid::parse_str(sa_user_id).unwrap(), None);

        let sa_create_rt: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation { createRefreshToken(validFor: "P30D") { id secret } }"#
                }),
                Some(&sa_token),
            )
            .await;
        assert!(
            sa_create_rt["errors"].is_array(),
            "service account should be denied a refresh token: {sa_create_rt}"
        );
    }
}
