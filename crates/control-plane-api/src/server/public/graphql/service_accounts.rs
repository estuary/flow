use super::{Sensitive, TimestampCursor};
use async_graphql::{Context, types::connection};

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct ServiceAccount {
    pub catalog_name: models::Name,
    /// Email of the user who created the account. Null if that user has no
    /// email on file.
    pub created_by_email: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
    pub grants: Vec<UserGrant>,
    pub api_keys: Vec<ServiceAccountApiKey>,
}

/// A user_grant to seed a service account with at creation time.
#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct UserGrantInput {
    pub prefix: models::Prefix,
    pub capability: models::Capability,
}

/// A user_grant held by a service account: the prefix it may act on and the
/// capability it holds there. An account's access is the union of its grants,
/// which may span multiple prefixes independent of its catalog_name anchor.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct UserGrant {
    pub prefix: models::Prefix,
    pub capability: models::Capability,
    pub detail: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// A service-account credential: a multi-use refresh token owned by the account
/// and minted by an administrator. The secret itself is returned only once at
/// creation (see [`CreateApiKeyResult`]).
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct ServiceAccountApiKey {
    pub id: models::Id,
    pub detail: Option<String>,
    /// Email of the user who minted the token. Null if that user has no email
    /// on file.
    pub created_by_email: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct CreateApiKeyResult {
    pub id: models::Id,
    /// The bearer credential, returned exactly once. Present it as an
    /// `Authorization: Bearer` token or exchange it at `POST /api/v1/auth/token`.
    pub secret: Sensitive,
    /// The owning account in its post-mint state, so the new token merges into
    /// client caches without a follow-up query.
    pub service_account: ServiceAccount,
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
        // Service accounts are visible to callers holding QueryServiceAccounts
        // on a prefix covering the account's catalog_name.
        let user_accessible_prefixes = super::authorized_prefixes::authorized_prefixes(
            &snapshot.role_grants,
            &snapshot.user_grants,
            env.claims()?.sub,
            models::authz::Capability::QueryServiceAccounts,
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
                        sa.catalog_name AS "catalog_name!: String",
                        creator.email AS "created_by_email: String",
                        sa.created_at AS "created_at!: chrono::DateTime<chrono::Utc>",
                        -- The account's "last used" is the max updated_at across
                        -- its tokens that have actually been exchanged (uses > 0;
                        -- revoked included). Each exchange bumps the token's
                        -- updated_at, so the tokens are the single source of truth.
                        (
                            SELECT max(rt.updated_at)
                            FROM public.refresh_tokens rt
                            WHERE rt.user_id = sa.user_id
                              AND rt.uses > 0
                        ) AS "last_used_at: chrono::DateTime<chrono::Utc>"
                    FROM internal.service_accounts sa
                    LEFT JOIN auth.users creator ON creator.id = sa.created_by
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

                // Tokens and grants are batch-loaded for the whole page in one
                // query each (no N+1). The tradeoff is that they load even when
                // the caller selected neither field.
                let mut api_keys_by_sa = load_api_keys_by_user(&env.pg_pool, &user_ids).await?;
                let mut grants_by_sa = load_grants_by_user(&env.pg_pool, &user_ids).await?;

                let edges: Vec<_> = sa_rows
                    .into_iter()
                    .take(limit)
                    .map(|r| {
                        let api_keys = api_keys_by_sa.remove(&r.user_id).unwrap_or_default();
                        let grants = grants_by_sa.remove(&r.user_id).unwrap_or_default();
                        connection::Edge::new(
                            TimestampCursor(r.created_at),
                            ServiceAccount {
                                catalog_name: models::Name::new(&r.catalog_name),
                                created_by_email: r.created_by_email,
                                created_at: r.created_at,
                                last_used_at: r.last_used_at,
                                grants,
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
    /// The caller must have CreateServiceAccount on the catalog name AND
    /// CreateGrant on each granted prefix. Creates an auth.users row, an
    /// internal.service_accounts row, and a user_grants row per requested
    /// grant.
    async fn create_service_account(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
        grants: Vec<UserGrantInput>,
    ) -> async_graphql::Result<ServiceAccount> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        if let Err(err) = validator::Validate::validate(&catalog_name) {
            return Err(async_graphql::Error::new(format!(
                "invalid catalog name: {err}"
            )));
        }
        // Creating the account (here, under this anchor) requires
        // CreateServiceAccount on the catalog name. This is deliberately
        // narrower than full Admin.
        super::verify_authorization(
            env,
            catalog_name.as_str(),
            models::authz::Capability::CreateServiceAccount,
        )
        .await?;

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

        // Granting the account access to a prefix requires CreateGrant on that
        // prefix — the anti-escalation guard, distinct from managing the
        // account: a caller can't hand a service account reach they couldn't
        // grant anyone. (Human-user grant creation still lives in PostgREST;
        // when it migrates to GraphQL it should gate on this same CreateGrant
        // capability.)
        for grant in &grants {
            super::verify_authorization(
                env,
                grant.prefix.as_str(),
                models::authz::Capability::CreateGrant,
            )
            .await?;
        }

        let mut txn = env.pg_pool.begin().await?;

        let sa_user_id = uuid::Uuid::new_v4();

        // Both the synthetic email and the catalog_name are unique and derived
        // from the same handle, so either insert can raise the duplicate
        // (SQLSTATE 23505) — and which one fires first depends on the
        // environment (real Supabase enforces a unique email; the local stub
        // does not). Map either violation to one clear message.
        let duplicate_err = |err: sqlx::Error| -> async_graphql::Error {
            if err.as_database_error().and_then(|e| e.code()).as_deref() == Some("23505") {
                async_graphql::Error::new(format!(
                    "a service account already exists for catalog name '{}'",
                    catalog_name.as_str(),
                ))
            } else {
                err.into()
            }
        };

        sqlx::query!(
            r#"
            INSERT INTO auth.users (id, email, raw_user_meta_data)
            VALUES ($1, $2, $3)
            "#,
            sa_user_id,
            format!("{}@service_accounts.estuary.dev", catalog_name.as_str()),
            serde_json::json!({
                "full_name": catalog_name.as_str(),
            }),
        )
        .execute(&mut *txn)
        .await
        .map_err(duplicate_err)?;

        sqlx::query!(
            r#"
            INSERT INTO internal.service_accounts (user_id, catalog_name, created_by)
            VALUES ($1, $2::text::catalog_name, $3)
            "#,
            sa_user_id,
            catalog_name.as_str(),
            claims.sub,
        )
        .execute(&mut *txn)
        .await
        .map_err(duplicate_err)?;

        for grant in &grants {
            crate::grants::overwrite_user_grant(
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

        // Read the account back through the shared loader so the create
        // response is byte-for-byte what a subsequent query returns —
        // including createdByEmail resolved from auth.users rather than the
        // caller's token claims.
        load_service_account(&env.pg_pool, sa_user_id).await
    }

    /// Add a user_grant to a service account.
    ///
    /// The caller must have CreateGrant on BOTH the account's catalog name and
    /// the granted prefix. Adding a grant is grant creation, so the account
    /// anchor gates on that same capability, while the per-prefix check
    /// prevents a caller from extending an account's access beyond what they
    /// could grant anyone. (Human-user grant creation still lives in
    /// PostgREST; when it migrates to GraphQL it should gate on this same
    /// CreateGrant capability.)
    async fn add_service_account_grant(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
        prefix: models::Prefix,
        capability: models::Capability,
    ) -> async_graphql::Result<ServiceAccount> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        super::verify_authorization(
            env,
            catalog_name.as_str(),
            models::authz::Capability::CreateGrant,
        )
        .await?;

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

        super::verify_authorization(env, prefix.as_str(), models::authz::Capability::CreateGrant)
            .await?;

        let user_id = resolve_service_account(&env.pg_pool, catalog_name.as_str()).await?;

        // Overwrite rather than upsert: addServiceAccountGrant replaces the
        // grant's capability outright, so a manager can narrow an existing
        // grant (e.g. admin -> read) in a single call. `upsert_user_grant`
        // would only ever raise the capability, silently ignoring a downgrade.
        let mut txn = env.pg_pool.begin().await?;
        crate::grants::overwrite_user_grant(
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

        load_service_account(&env.pg_pool, user_id).await
    }

    /// Remove a user_grant from a service account, returning the account in its
    /// post-removal state.
    ///
    /// The caller must have CreateServiceAccount on the catalog name. Unlike addServiceAccountGrant, no capability on the
    /// grant's prefix is required: removal only ever narrows the account's
    /// access, so managers may remove ANY grant — including grants to
    /// prefixes they don't themselves administer.
    ///
    /// Removal is idempotent: removing a grant the account doesn't hold is a
    /// no-op that returns the unchanged account rather than an error.
    async fn remove_service_account_grant(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
        prefix: models::Prefix,
    ) -> async_graphql::Result<ServiceAccount> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        super::verify_authorization(
            env,
            catalog_name.as_str(),
            models::authz::Capability::CreateServiceAccount,
        )
        .await?;

        let user_id = resolve_service_account(&env.pg_pool, catalog_name.as_str()).await?;

        let deleted = sqlx::query!(
            "DELETE FROM public.user_grants WHERE user_id = $1 AND object_role = $2",
            user_id,
            prefix.as_str(),
        )
        .execute(&env.pg_pool)
        .await?;

        tracing::info!(
            %user_id,
            %catalog_name,
            %prefix,
            removed = deleted.rows_affected(),
            %claims.sub,
            "removed service account grant"
        );

        load_service_account(&env.pg_pool, user_id).await
    }

    /// Remove ALL user_grants from a service account, stripping its access in
    /// one call and returning the account with `grants: []`.
    ///
    /// The caller must manage the service account (CreateServiceAccount on its
    /// catalog name). As with removeServiceAccountGrant, no capability on the
    /// grants' prefixes is required: removal only narrows access, so a manager
    /// may clear grants to prefixes they don't themselves administer. Clearing
    /// an account that already has no grants is an idempotent no-op.
    async fn remove_all_service_account_grants(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
    ) -> async_graphql::Result<ServiceAccount> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        super::verify_authorization(
            env,
            catalog_name.as_str(),
            models::authz::Capability::CreateServiceAccount,
        )
        .await?;

        let user_id = resolve_service_account(&env.pg_pool, catalog_name.as_str()).await?;

        let deleted = sqlx::query!("DELETE FROM public.user_grants WHERE user_id = $1", user_id,)
            .execute(&env.pg_pool)
            .await?;

        tracing::info!(
            %user_id,
            %catalog_name,
            removed = deleted.rows_affected(),
            %claims.sub,
            "removed all service account grants"
        );

        load_service_account(&env.pg_pool, user_id).await
    }

    /// Mint a credential for a service account.
    ///
    /// The credential is a multi-use refresh token owned by the account: its
    /// secret never rotates and its validity window of `valid_for` slides with
    /// use, like any refresh token. Returns the token id and the bearer secret,
    /// which is returned exactly once and cannot be retrieved again. Present it
    /// as an `Authorization: Bearer` credential or exchange it for a 1-hour
    /// access token via `POST /api/v1/auth/token`.
    ///
    /// The caller must have CreateApiKey on the account's catalog name.
    async fn create_api_key(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
        detail: String,
        #[graphql(desc = "ISO 8601 duration for token validity (e.g. P90D, P1Y)")]
        valid_for: String,
    ) -> async_graphql::Result<CreateApiKeyResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        super::verify_authorization(
            env,
            catalog_name.as_str(),
            models::authz::Capability::CreateApiKey,
        )
        .await?;

        let user_id = resolve_service_account(&env.pg_pool, catalog_name.as_str()).await?;

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

        // Mint the credential as a multi_use refresh token owned by the service
        // account. The lifetime is bounded to at most one year and required to
        // be positive; Postgres does the calendar-aware interval math (the WHERE
        // clause yields zero rows when out of bounds). The secret is a random
        // UUID bcrypt-hashed at rest, matching create_refresh_token.
        let row = sqlx::query!(
            r#"
            WITH new_token AS (
                SELECT gen_random_uuid()::text AS secret
            )
            INSERT INTO public.refresh_tokens
                (user_id, multi_use, valid_for, hash, detail, created_by)
            SELECT
                $1,
                true,
                v.valid_for,
                crypt(nt.secret, gen_salt('bf')),
                $3,
                $4
            FROM new_token nt, (SELECT $2::text::interval AS valid_for) v
            WHERE v.valid_for > interval '0' AND v.valid_for <= interval '366 days'
            RETURNING
                id AS "id!: models::Id",
                (SELECT secret FROM new_token) AS "secret!: String"
            "#,
            user_id,
            valid_for,
            detail,
            claims.sub,
        )
        .fetch_optional(&env.pg_pool)
        .await
        .map_err(|err| {
            // A 'P'-prefixed value can still fail the `::interval` cast: Postgres
            // raises SQLSTATE 22007 (invalid_datetime_format) / 22008
            // (datetime_field_overflow) for a malformed duration and 22015
            // (interval_field_overflow) for one too extreme to parse. All are
            // client errors, not internal faults, so surface a sanitized message.
            let code = err.as_database_error().and_then(|e| e.code());
            if matches!(code.as_deref(), Some("22007" | "22008" | "22015")) {
                async_graphql::Error::new(
                    "invalid valid_for: expected an ISO 8601 duration, e.g. P90D or P1Y",
                )
            } else {
                tracing::error!(?err, "failed to create service account API key");
                async_graphql::Error::new("failed to create service account API key")
            }
        })?
        .ok_or_else(|| {
            async_graphql::Error::new(
                "valid_for must be a positive duration no greater than 1 year",
            )
        })?;

        // The bearer form accepted by the Envelope extractor and the
        // token-exchange endpoint: standard base64 of `{"id": ..., "secret": ...}`.
        use base64::Engine;
        let secret = base64::engine::general_purpose::STANDARD.encode(
            serde_json::json!({ "id": row.id.to_string(), "secret": row.secret }).to_string(),
        );

        tracing::info!(
            api_key_id = %row.id,
            %user_id,
            %detail,
            %claims.sub,
            "created service account API key"
        );

        let service_account = load_service_account(&env.pg_pool, user_id).await?;

        Ok(CreateApiKeyResult {
            id: row.id,
            secret: Sensitive::new(secret),
            service_account,
        })
    }

    /// Revoke a service-account token, returning the owning account in its
    /// post-revocation state.
    ///
    /// The caller must have RevokeApiKey on the owning service account's
    /// catalog name. The account is resolved from the token id.
    ///
    /// Rather than deleting the row, we zero its `valid_for` interval, which
    /// makes the token inert (it fails the exchange's expiry check and is
    /// excluded from listings) while preserving the audit trail. Revocation is
    /// idempotent: revoking an already-inert token is a no-op that still returns
    /// the account. Only an id that maps to no service-account token errors.
    async fn revoke_api_key(
        &self,
        ctx: &Context<'_>,
        id: models::Id,
    ) -> async_graphql::Result<ServiceAccount> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        // Resolve the owning account regardless of the token's current validity,
        // so revoking an already-inert token still finds it and is a no-op (the
        // UPDATE below only touches still-active tokens).
        let owner = sqlx::query!(
            r#"
            SELECT
                rt.user_id,
                sa.catalog_name AS "catalog_name!: String"
            FROM public.refresh_tokens rt
            JOIN internal.service_accounts sa ON sa.user_id = rt.user_id
            WHERE rt.id = $1
            "#,
            id as models::Id,
        )
        .fetch_optional(&env.pg_pool)
        .await?;

        let owner = match owner {
            Some(owner) => owner,
            None => {
                return Err(async_graphql::Error::new(
                    "service account API key not found",
                ));
            }
        };

        // The owner (and thus its catalog name) is resolved from the token id
        // before authorizing — unavoidable, since the account to authorize
        // against comes from the token. A raw denial would surface the owning
        // catalog name, letting a caller who holds a token id they aren't
        // authorized for learn which account owns it. Collapse denial into the
        // same "not found" error the unknown-id branch returns, so existence
        // and denial are indistinguishable.
        super::verify_authorization(
            env,
            &owner.catalog_name,
            models::authz::Capability::RevokeApiKey,
        )
        .await
        .map_err(hide_denial_as_not_found)?;

        sqlx::query!(
            "UPDATE public.refresh_tokens SET valid_for = interval '0' \
             WHERE id = $1 AND valid_for <> interval '0'",
            id as models::Id
        )
        .execute(&env.pg_pool)
        .await?;

        tracing::info!(
            api_key_id = %id,
            service_account = %owner.catalog_name,
            %claims.sub,
            "revoked service account API key"
        );

        load_service_account(&env.pg_pool, owner.user_id).await
    }

    /// Revoke ALL of a service account's API keys at once — the credential kill
    /// switch — returning the account with no active keys.
    ///
    /// The caller must have RevokeApiKey on the account's catalog name.
    /// Like revokeApiKey, each key is made inert by zeroing its
    /// `valid_for` interval (preserving the audit trail) rather than deleted;
    /// already-revoked keys are skipped. A service account's user_id only ever
    /// owns its own minted credentials, so this targets exactly those. An
    /// account with no active keys is an idempotent no-op.
    async fn revoke_all_api_keys(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
    ) -> async_graphql::Result<ServiceAccount> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        super::verify_authorization(
            env,
            catalog_name.as_str(),
            models::authz::Capability::RevokeApiKey,
        )
        .await?;

        let user_id = resolve_service_account(&env.pg_pool, catalog_name.as_str()).await?;

        let revoked = sqlx::query!(
            "UPDATE public.refresh_tokens SET valid_for = interval '0' \
             WHERE user_id = $1 AND valid_for <> interval '0'",
            user_id,
        )
        .execute(&env.pg_pool)
        .await?;

        tracing::info!(
            %user_id,
            %catalog_name,
            revoked = revoked.rows_affected(),
            %claims.sub,
            "revoked all service account API keys"
        );

        load_service_account(&env.pg_pool, user_id).await
    }
}

/// Verify that `user_id` is not a service-account identity, erroring if it is.
///
/// Service-account credentials are administered through createApiKey /
/// revokeApiKey. The self-service refresh-token mutations reject a
/// service-account caller: a valid key could otherwise mint replacement
/// credentials for its own account — sidestepping the CreateApiKey gate and
/// the admin-chosen expiry — or revoke keys outside the admin-facing flow.
pub(super) async fn verify_not_service_account(
    pg_pool: &sqlx::PgPool,
    user_id: uuid::Uuid,
) -> async_graphql::Result<()> {
    let is_service_account = sqlx::query_scalar!(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM internal.service_accounts WHERE user_id = $1
        ) AS "is_service_account!"
        "#,
        user_id,
    )
    .fetch_one(pg_pool)
    .await?;

    if is_service_account {
        return Err(async_graphql::Error::new(
            "service accounts cannot manage refresh tokens: their API keys are \
             administered via createApiKey and revokeApiKey",
        ));
    }
    Ok(())
}

/// Rewrite a terminal permission-denied authorization error into the generic
/// "service account API key not found" error, so a denial is indistinguishable
/// from a missing token id.
///
/// Only terminal denials (`tonic::Code::PermissionDenied`) are rewritten. A
/// provisional [`crate::ApiError::AuthZRetry`] — which drives the snapshot
/// refresh-and-retry redirect — is passed through untouched, as is any other
/// error, so this never swallows a retry or an internal fault.
fn hide_denial_as_not_found(err: async_graphql::Error) -> async_graphql::Error {
    let is_permission_denied = err
        .source
        .as_ref()
        .and_then(|source| source.downcast_ref::<crate::ApiError>())
        .is_some_and(|api_error| {
            matches!(
                api_error,
                crate::ApiError::Status(status)
                    if status.code() == tonic::Code::PermissionDenied
            )
        });

    if is_permission_denied {
        async_graphql::Error::new("service account API key not found")
    } else {
        err
    }
}

/// Resolve a service account's backing `user_id` from its `catalog_name` handle.
///
/// Service accounts are addressed publicly by catalog name; the writes still
/// need the backing auth.users id. Callers authorize against the catalog name
/// *before* resolving, so a "not found" here is for an authorized namespace.
async fn resolve_service_account(
    pg_pool: &sqlx::PgPool,
    catalog_name: &str,
) -> async_graphql::Result<uuid::Uuid> {
    let row = sqlx::query_scalar!(
        r#"
        SELECT user_id
        FROM internal.service_accounts
        WHERE catalog_name = $1::text::catalog_name
        "#,
        catalog_name,
    )
    .fetch_optional(pg_pool)
    .await?;

    row.ok_or_else(|| async_graphql::Error::new("service account not found"))
}

/// Load a single service account in its current state — its profile fields
/// plus its grants and active tokens. Mutations return this so clients
/// reconcile by catalogName without a follow-up query. Errors if no account
/// is homed at `user_id` (e.g. it was concurrently deleted).
async fn load_service_account(
    pg_pool: &sqlx::PgPool,
    user_id: uuid::Uuid,
) -> async_graphql::Result<ServiceAccount> {
    let row = sqlx::query!(
        r#"
        SELECT
            sa.catalog_name AS "catalog_name!: String",
            creator.email AS "created_by_email: String",
            sa.created_at AS "created_at!: chrono::DateTime<chrono::Utc>",
            -- See the listing query: "last used" is the max updated_at across
            -- the account's exchanged tokens (uses > 0; revoked included).
            (
                SELECT max(rt.updated_at)
                FROM public.refresh_tokens rt
                WHERE rt.user_id = sa.user_id
                  AND rt.uses > 0
            ) AS "last_used_at: chrono::DateTime<chrono::Utc>"
        FROM internal.service_accounts sa
        LEFT JOIN auth.users creator ON creator.id = sa.created_by
        WHERE sa.user_id = $1
        "#,
        user_id,
    )
    .fetch_optional(pg_pool)
    .await?
    .ok_or_else(|| async_graphql::Error::new("service account not found"))?;

    // A single account is the degenerate one-element case of the batch loaders.
    let mut grants_by_sa = load_grants_by_user(pg_pool, &[user_id]).await?;
    let mut api_keys_by_sa = load_api_keys_by_user(pg_pool, &[user_id]).await?;

    Ok(ServiceAccount {
        catalog_name: models::Name::new(&row.catalog_name),
        created_by_email: row.created_by_email,
        created_at: row.created_at,
        last_used_at: row.last_used_at,
        grants: grants_by_sa.remove(&user_id).unwrap_or_default(),
        api_keys: api_keys_by_sa.remove(&user_id).unwrap_or_default(),
    })
}

/// Batch-load the grants of a set of service accounts, keyed by user_id and
/// ordered by prefix within each account. An account's reach is the union of
/// its grants, so they're a list rather than a single capability.
async fn load_grants_by_user(
    pg_pool: &sqlx::PgPool,
    user_ids: &[uuid::Uuid],
) -> sqlx::Result<std::collections::HashMap<uuid::Uuid, Vec<UserGrant>>> {
    let mut by_user: std::collections::HashMap<uuid::Uuid, Vec<UserGrant>> =
        std::collections::HashMap::new();
    if user_ids.is_empty() {
        return Ok(by_user);
    }

    let rows = sqlx::query!(
        r#"
        SELECT
            g.user_id,
            g.object_role AS "prefix!: models::Prefix",
            g.capability AS "capability!: models::Capability",
            g.detail,
            g.created_at AS "created_at!: chrono::DateTime<chrono::Utc>",
            g.updated_at AS "updated_at!: chrono::DateTime<chrono::Utc>"
        FROM public.user_grants g
        WHERE g.user_id = ANY($1)
        ORDER BY g.object_role
        "#,
        user_ids,
    )
    .fetch_all(pg_pool)
    .await?;

    for gr in rows {
        by_user.entry(gr.user_id).or_default().push(UserGrant {
            prefix: gr.prefix,
            capability: gr.capability,
            detail: gr.detail,
            created_at: gr.created_at,
            updated_at: gr.updated_at,
        });
    }

    Ok(by_user)
}

/// Batch-load the active (non-revoked) tokens of a set of service accounts,
/// keyed by user_id and newest first. Revoked tokens (valid_for zeroed) are
/// excluded, matching the listing surface.
async fn load_api_keys_by_user(
    pg_pool: &sqlx::PgPool,
    user_ids: &[uuid::Uuid],
) -> sqlx::Result<std::collections::HashMap<uuid::Uuid, Vec<ServiceAccountApiKey>>> {
    let mut by_user: std::collections::HashMap<uuid::Uuid, Vec<ServiceAccountApiKey>> =
        std::collections::HashMap::new();
    if user_ids.is_empty() {
        return Ok(by_user);
    }

    let rows = sqlx::query!(
        r#"
        SELECT
            rt.id AS "id!: models::Id",
            rt.user_id,
            rt.detail,
            creator.email AS "created_by_email: String",
            rt.created_at AS "created_at!: chrono::DateTime<chrono::Utc>",
            (rt.updated_at + rt.valid_for) AS "expires_at!: chrono::DateTime<chrono::Utc>",
            CASE WHEN rt.uses > 0 THEN rt.updated_at END AS "last_used_at: chrono::DateTime<chrono::Utc>"
        FROM public.refresh_tokens rt
        LEFT JOIN auth.users creator ON creator.id = rt.created_by
        WHERE rt.user_id = ANY($1)
          AND rt.valid_for <> interval '0'
        ORDER BY rt.created_at DESC
        "#,
        user_ids,
    )
    .fetch_all(pg_pool)
    .await?;

    for tr in rows {
        by_user
            .entry(tr.user_id)
            .or_default()
            .push(ServiceAccountApiKey {
                id: tr.id,
                detail: tr.detail,
                created_by_email: tr.created_by_email,
                created_at: tr.created_at,
                expires_at: tr.expires_at,
                last_used_at: tr.last_used_at,
            });
    }

    Ok(by_user)
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
                    mutation($catalogName: Name!, $grants: [UserGrantInput!]!) {
                        createServiceAccount(
                            catalogName: $catalogName
                            grants: $grants
                        ) {
                            catalogName
                            createdByEmail
                            createdAt
                            lastUsedAt
                            grants { prefix capability detail createdAt updatedAt }
                            apiKeys { id }
                        }
                    }"#,
                    "variables": {
                        "catalogName": "aliceCo/ci-deploy-bot",
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
        // The public API doesn't expose the backing user_id; fetch it from the
        // DB for the row-level assertions below.
        let sa_user_id = service_account_user_id(&pool, "aliceCo/ci-deploy-bot").await;
        assert_eq!(sa["catalogName"], "aliceCo/ci-deploy-bot");
        assert_eq!(sa["apiKeys"].as_array().unwrap().len(), 0);

        // The create response echoes the seeded grants in request order, each
        // carrying the capability, the standard "service account grant" detail,
        // and creation timestamps.
        let created_grants = sa["grants"].as_array().unwrap();
        assert_eq!(created_grants.len(), 2, "both seeded grants returned: {sa}");
        assert_eq!(created_grants[0]["prefix"], "aliceCo/");
        assert_eq!(created_grants[0]["capability"], "admin");
        assert_eq!(created_grants[0]["detail"], "service account grant");
        assert!(created_grants[0]["createdAt"].is_string());
        assert!(created_grants[0]["updatedAt"].is_string());
        assert_eq!(created_grants[1]["prefix"], "aliceCo/data/");
        assert_eq!(created_grants[1]["capability"], "read");

        // === A catalog name is unique to one service account ===
        // A second account cannot claim the same handle, even for an authorized
        // caller.
        let dup: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createServiceAccount(
                            catalogName: "aliceCo/ci-deploy-bot"
                            grants: [{ prefix: "aliceCo/", capability: admin }]
                        ) { catalogName }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            dup["errors"]
                .as_array()
                .is_some_and(|errs| errs.iter().any(|e| e["message"]
                    .as_str()
                    .is_some_and(|m| m.contains("already exists")))),
            "duplicate catalog name should be rejected: {dup}"
        );
        assert_eq!(
            grant_count(&pool, &sa_user_id).await,
            2,
            "each requested grant should mint a user_grants row"
        );
        // Provenance and timestamp fields are populated on creation:
        // createdByEmail is the creator's email resolved from auth.users (alice,
        // per the fixture), the timestamps are set, and a freshly created
        // account has never been used.
        assert_eq!(
            sa["createdByEmail"], "alice@example.com",
            "createdByEmail should be the creator's email from auth.users: {create_response}"
        );
        assert!(sa["createdAt"].is_string(), "createdAt should be set: {sa}");
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
                            grants: [{ prefix: "aliceCo/", capability: read }]
                        ) { catalogName }
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
                        createServiceAccount(catalogName: "Not A Name", grants: []) { catalogName }
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
                            grants: [{ prefix: "aliceCo/", capability: none }]
                        ) { catalogName }
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
                            grants: [
                                { prefix: "aliceCo/", capability: read },
                                { prefix: "bobCo/", capability: read }
                            ]
                        ) { catalogName }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            foreign_grant["errors"].is_array(),
            "a grant to an unadministered prefix should be rejected: {foreign_grant}"
        );

        // === Mint a service-account token ===
        let create_key: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($detail: String!, $validFor: String!) {
                        createApiKey(
                            catalogName: "aliceCo/ci-deploy-bot"
                            detail: $detail
                            validFor: $validFor
                        ) {
                            id
                            secret
                            serviceAccount {
                                catalogName
                                apiKeys { id detail createdByEmail }
                            }
                        }
                    }"#,
                    "variables": {
                        "detail": "GitHub Actions",
                        "validFor": "P90D"
                    }
                }),
                Some(&alice_token),
            )
            .await;

        assert!(
            create_key["errors"].is_null(),
            "create token should succeed: {create_key}"
        );
        let api_key_data = &create_key["data"]["createApiKey"];
        let api_key_id = api_key_data["id"]
            .as_str()
            .expect("should have id")
            .to_string();
        let secret = api_key_data["secret"]
            .as_str()
            .expect("should have secret")
            .to_string();
        // The result embeds the owning account in its post-mint state, so a
        // client can merge the new token into its cache without a refetch: the
        // just-minted token appears among its tokens.
        let minted_account = &api_key_data["serviceAccount"];
        assert_eq!(minted_account["catalogName"], "aliceCo/ci-deploy-bot");
        let minted_keys = minted_account["apiKeys"].as_array().unwrap();
        assert_eq!(
            minted_keys.len(),
            1,
            "the new token is present: {create_key}"
        );
        assert_eq!(minted_keys[0]["id"], api_key_id);
        assert_eq!(minted_keys[0]["detail"], "GitHub Actions");
        assert_eq!(
            minted_keys[0]["createdByEmail"], "alice@example.com",
            "token createdByEmail should be the minting admin's email: {create_key}"
        );

        // === valid_for validation ===
        // Each case must be rejected, and the error message identifies the
        // specific branch: non-ISO syntax, malformed ISO, interval overflow,
        // non-positive, and over the one-year cap.
        for (valid_for, want) in [
            ("90 days", "ISO 8601"),                 // Postgres syntax, not ISO 8601
            ("Pfoo", "invalid valid_for"),           // 'P'-prefixed but unparseable
            ("P300000000000Y", "invalid valid_for"), // overflows interval parsing (SQLSTATE 22015)
            ("P0D", "positive"),                     // zero duration
            ("P2Y", "no greater than 1 year"),       // exceeds the cap
        ] {
            let rejected: serde_json::Value = server
                .graphql(
                    &serde_json::json!({
                        "query": r#"
                        mutation($detail: String!, $validFor: String!) {
                            createApiKey(catalogName: "aliceCo/ci-deploy-bot", detail: $detail, validFor: $validFor) { id }
                        }"#,
                        "variables": {
                            "detail": "bad valid_for",
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

        // === A service account cannot self-manage credentials ===
        // The self-service refresh-token mutations reject a service-account
        // caller outright: a valid key could otherwise mint replacement
        // credentials for its own account (sidestepping CreateApiKey and the
        // admin-chosen expiry) or revoke keys outside the admin-facing flow.
        // Fabricate an access token for the account's identity — equivalent to
        // what its bearer key exchanges into — and try both mutations.
        let sa_token = server.make_access_token(uuid::Uuid::parse_str(&sa_user_id).unwrap(), None);
        let sa_mint: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation { createRefreshToken(validFor: "P30D") { id } }"#
                }),
                Some(&sa_token),
            )
            .await;
        assert!(
            sa_mint["errors"][0]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("service accounts cannot manage refresh tokens"),
            "a service account must not mint refresh tokens for itself: {sa_mint}"
        );
        let sa_revoke: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation($id: Id!) { revokeRefreshToken(id: $id) }"#,
                    "variables": { "id": api_key_id }
                }),
                Some(&sa_token),
            )
            .await;
        assert!(
            sa_revoke["errors"][0]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("service accounts cannot manage refresh tokens"),
            "a service account must not revoke its own keys via revokeRefreshToken: {sa_revoke}"
        );

        // The happy-path exchange (generate_access_token actually signing a JWT)
        // is intentionally not exercised here: signing reads app.jwt_secret from
        // vault.decrypted_secrets and calls pgjwt's sign(), neither of which
        // exists in the sqlx::test DB. That path is covered by the pgTAP
        // refresh-token tests. The assertions below all resolve *before* signing
        // (a bad secret, or a revoked/expired token), so they're deterministic
        // without that setup.

        // Simulate a successful exchange so the read-side last_used_at derivation
        // is observable: generate_access_token bumps uses and updated_at, which
        // the listing surfaces. We can't run the real exchange (it would sign),
        // so stamp the row directly.
        let parsed_api_key_id: models::Id = api_key_id.parse().unwrap();
        sqlx::query!(
            "UPDATE public.refresh_tokens SET uses = 1, updated_at = now() WHERE id = $1",
            parsed_api_key_id as models::Id,
        )
        .execute(&pool)
        .await
        .unwrap();

        // === List service accounts ===
        let list: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        serviceAccounts {
                            edges {
                                node {
                                    catalogName
                                    createdByEmail
                                    lastUsedAt
                                    grants { prefix capability detail }
                                    apiKeys {
                                        id
                                        detail
                                        createdByEmail
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
        assert_eq!(edges[0]["node"]["catalogName"], "aliceCo/ci-deploy-bot");
        // The listing resolves the creator's email via the auth.users join.
        assert_eq!(
            edges[0]["node"]["createdByEmail"], "alice@example.com",
            "listing should resolve createdByEmail from auth.users: {list}"
        );
        let listed_key = &edges[0]["node"]["apiKeys"][0];
        assert_eq!(edges[0]["node"]["apiKeys"].as_array().unwrap().len(), 1);
        assert_eq!(listed_key["detail"], "GitHub Actions");
        assert_eq!(
            listed_key["createdByEmail"], "alice@example.com",
            "token createdByEmail should be the minting admin's email: {list}"
        );
        assert!(
            listed_key["createdAt"].is_string() && listed_key["expiresAt"].is_string(),
            "token createdAt/expiresAt should be set: {list}"
        );
        // We stamped a use above, so the derived last_used_at is populated.
        assert!(
            listed_key["lastUsedAt"].is_string(),
            "lastUsedAt should be derived from a used token's updated_at: {list}"
        );
        // The account's lastUsedAt is derived as the max across its tokens.
        assert!(
            edges[0]["node"]["lastUsedAt"].is_string(),
            "account lastUsedAt should be derived from its tokens' use: {list}"
        );
        // The read-side grants resolver batch-loads from user_grants, ordered
        // by prefix: the two seeded grants come back with their persisted
        // capability and detail.
        let listed_grants = edges[0]["node"]["grants"].as_array().unwrap();
        assert_eq!(listed_grants.len(), 2, "both grants listed: {list}");
        assert_eq!(listed_grants[0]["prefix"], "aliceCo/");
        assert_eq!(listed_grants[0]["capability"], "admin");
        assert_eq!(listed_grants[0]["detail"], "service account grant");
        assert_eq!(listed_grants[1]["prefix"], "aliceCo/data/");
        assert_eq!(listed_grants[1]["capability"], "read");

        // Bob sees no service accounts.
        let bob_list: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        serviceAccounts { edges { node { catalogName } } }
                    }"#
                }),
                Some(&bob_token),
            )
            .await;

        let bob_edges = bob_list["data"]["serviceAccounts"]["edges"]
            .as_array()
            .expect("should have edges");
        assert_eq!(bob_edges.len(), 0);

        // Bob can't revoke a key he isn't authorized for. The owning account is
        // resolved from the token id before authorizing, so a raw denial would
        // embed the owner's catalog name. The response must instead be the same
        // generic "not found" an unknown id returns — indistinguishable, and
        // leaking no catalog name — even though the id names a real token.
        let bob_revoke: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($id: Id!) {
                        revokeApiKey(id: $id) { catalogName }
                    }"#,
                    "variables": { "id": api_key_id }
                }),
                Some(&bob_token),
            )
            .await;
        let bob_revoke_msg = bob_revoke["errors"][0]["message"]
            .as_str()
            .unwrap_or_default();
        assert!(
            bob_revoke_msg.contains("service account API key not found"),
            "an unauthorized revoke should report not found: {bob_revoke}"
        );
        assert!(
            !bob_revoke_msg.contains("aliceCo/ci-deploy-bot"),
            "an unauthorized revoke must not leak the owning catalog name: {bob_revoke}"
        );

        // === A bad secret is rejected statefully ===
        // generate_access_token checks the secret before signing, so this 401s
        // deterministically even without the signing setup. Build a bearer form
        // for the real token id but a wrong secret.
        use base64::Engine;
        let bad_bearer = base64::engine::general_purpose::STANDARD
            .encode(serde_json::json!({ "id": api_key_id, "secret": "wrong-secret" }).to_string());
        let bad = server
            .rest_client()
            .post(
                "/api/graphql",
                &serde_json::json!({ "query": "query { __typename }" }),
                Some(&bad_bearer),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(
            bad.status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "a wrong secret must be rejected with 401"
        );

        // === Revoke the token ===
        // The mutation returns the owning account in its post-revocation state:
        // the revoked token drops out of its active tokens, leaving none.
        let revoke: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($id: Id!) {
                        revokeApiKey(id: $id) {
                            catalogName
                            apiKeys { id }
                        }
                    }"#,
                    "variables": { "id": api_key_id }
                }),
                Some(&alice_token),
            )
            .await;

        assert!(
            revoke["errors"].is_null(),
            "revoke should succeed: {revoke}"
        );
        assert_eq!(
            revoke["data"]["revokeApiKey"]["catalogName"],
            "aliceCo/ci-deploy-bot"
        );
        assert_eq!(
            revoke["data"]["revokeApiKey"]["apiKeys"]
                .as_array()
                .unwrap()
                .len(),
            0,
            "the revoked token should no longer be active: {revoke}"
        );

        // The row is preserved with valid_for zeroed — revocation is a soft
        // delete for audit purposes. The listing assertion below can't observe
        // this distinction, so check the table directly.
        let zeroed = sqlx::query_scalar!(
            r#"SELECT valid_for = interval '0' AS "zeroed!" FROM public.refresh_tokens WHERE id = $1"#,
            parsed_api_key_id as models::Id,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(zeroed, "revocation must zero valid_for, not delete the row");

        // Revoking again is an idempotent no-op: the token still exists (just
        // inert), so it resolves the account and returns it unchanged rather
        // than erroring. Only an id mapping to no service-account token errors.
        let revoke_again: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($id: Id!) {
                        revokeApiKey(id: $id) {
                            catalogName
                            apiKeys { id }
                        }
                    }"#,
                    "variables": { "id": api_key_id }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            revoke_again["errors"].is_null(),
            "re-revoking an inert token should be an idempotent no-op: {revoke_again}"
        );
        assert_eq!(
            revoke_again["data"]["revokeApiKey"]["apiKeys"]
                .as_array()
                .unwrap()
                .len(),
            0,
            "no active tokens remain after re-revoking: {revoke_again}"
        );

        // An unknown token id still errors: there's no account to resolve.
        let revoke_unknown: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($id: Id!) {
                        revokeApiKey(id: $id) { catalogName }
                    }"#,
                    "variables": { "id": "00:00:00:00:00:00:00:01" }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            revoke_unknown["errors"][0]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("service account API key not found"),
            "an unknown token id should report not found: {revoke_unknown}"
        );

        // The revoked token no longer authenticates — immediately, since every
        // request re-verifies the credential against the database. A zeroed
        // valid_for fails generate_access_token's expiry check (before signing),
        // yielding the same 401 as an unknown token.
        let rejected = server
            .rest_client()
            .post(
                "/api/graphql",
                &serde_json::json!({ "query": "query { __typename }" }),
                Some(&secret),
            )
            .send()
            .await
            .unwrap();
        let status = rejected.status();
        let body = rejected.text().await.unwrap();
        assert_eq!(
            status,
            reqwest::StatusCode::UNAUTHORIZED,
            "revoked token should be rejected with 401: {body}"
        );
        assert!(
            body.contains("invalid, expired, or unknown credential"),
            "revoked token rejection body: {body}"
        );

        // The revoked token is excluded from listings, even though its row remains.
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
            "revoked tokens must not appear in listings: {list_after_revoke}"
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

        // The public API addresses service accounts by catalog name; tests that
        // assert at the row level still need the backing user_id.
        async fn service_account_user_id(pool: &sqlx::PgPool, catalog_name: &str) -> String {
            sqlx::query_scalar!(
                r#"SELECT user_id FROM internal.service_accounts WHERE catalog_name = $1::text::catalog_name"#,
                catalog_name,
            )
            .fetch_one(pool)
            .await
            .unwrap()
            .to_string()
        }

        // === Grant management ===
        // Adding a grant requires BOTH managing the account and admin on the
        // granted prefix. Bob has neither, so he can't add a grant.
        let add_unmanaged: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        addServiceAccountGrant(catalogName: "aliceCo/ci-deploy-bot", prefix: "aliceCo/", capability: read) { catalogName }
                    }"#
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
                    mutation {
                        addServiceAccountGrant(catalogName: "aliceCo/ci-deploy-bot", prefix: "bobCo/", capability: read) { catalogName }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            add_foreign["errors"].is_array(),
            "a grant to an unadministered prefix must be rejected: {add_foreign}"
        );

        // Happy path: alice manages the account and admins aliceCo/ops/. The
        // mutation returns the account in its post-add state, so the new grant
        // is present in the returned grants (ordered by prefix).
        let add: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        addServiceAccountGrant(catalogName: "aliceCo/ci-deploy-bot", prefix: "aliceCo/ops/", capability: write) {
                            grants { prefix capability }
                        }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(add["errors"].is_null(), "add grant should succeed: {add}");
        assert_eq!(grant_count(&pool, &sa_user_id).await, 3);
        let added_grants = add["data"]["addServiceAccountGrant"]["grants"]
            .as_array()
            .unwrap();
        assert_eq!(added_grants.len(), 3, "the new grant is returned: {add}");
        assert!(
            added_grants
                .iter()
                .any(|g| g["prefix"] == "aliceCo/ops/" && g["capability"] == "write"),
            "the added aliceCo/ops/ write grant should be present: {add}"
        );

        // aliceCo/ops/ now holds `write`. Adding a LOWER capability overwrites
        // the existing grant in place: addServiceAccountGrant replaces the
        // capability rather than only ever raising it, so a manager can narrow
        // a grant in one call without removing it first.
        let downgrade: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        addServiceAccountGrant(catalogName: "aliceCo/ci-deploy-bot", prefix: "aliceCo/ops/", capability: read) {
                            grants { prefix capability }
                        }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            downgrade["errors"].is_null(),
            "narrowing a grant should succeed by overwriting it: {downgrade}"
        );
        // The returned account reflects the narrowed capability in place.
        let downgraded_grants = downgrade["data"]["addServiceAccountGrant"]["grants"]
            .as_array()
            .unwrap();
        assert!(
            downgraded_grants
                .iter()
                .any(|g| g["prefix"] == "aliceCo/ops/" && g["capability"] == "read"),
            "aliceCo/ops/ should now be read in the returned account: {downgrade}"
        );
        // The grant is overwritten in place: capability lowered to read, with
        // no additional row.
        let ops_capability = sqlx::query_scalar!(
            r#"SELECT capability AS "capability!: models::Capability"
               FROM public.user_grants WHERE user_id = $1 AND object_role = 'aliceCo/ops/'"#,
            uuid::Uuid::parse_str(&sa_user_id).unwrap(),
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            ops_capability,
            models::Capability::Read,
            "the grant should be overwritten to the requested lower capability"
        );
        assert_eq!(grant_count(&pool, &sa_user_id).await, 3);

        // Removal requires only account management — no capability on the
        // grant's prefix. Seed a grant to bobCo/ directly (adding one via the
        // API requires admin on it), then alice removes it despite having no
        // bobCo/ access of her own.
        sqlx::query("INSERT INTO user_grants (user_id, object_role, capability) VALUES ($1, 'bobCo/', 'read')")
            .bind(uuid::Uuid::parse_str(&sa_user_id).unwrap())
            .execute(&pool)
            .await
            .unwrap();

        let remove_foreign: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        removeServiceAccountGrant(catalogName: "aliceCo/ci-deploy-bot", prefix: "bobCo/") {
                            grants { prefix }
                        }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            remove_foreign["errors"].is_null(),
            "a manager may remove ANY grant, including one to a prefix they don't administer: {remove_foreign}"
        );
        assert_eq!(grant_count(&pool, &sa_user_id).await, 3);
        // The returned account no longer carries the bobCo/ grant.
        assert!(
            remove_foreign["data"]["removeServiceAccountGrant"]["grants"]
                .as_array()
                .unwrap()
                .iter()
                .all(|g| g["prefix"] != "bobCo/"),
            "bobCo/ should be gone from the returned account: {remove_foreign}"
        );

        // Removing an absent grant is an idempotent no-op: it returns the
        // unchanged account rather than erroring.
        let remove_again: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        removeServiceAccountGrant(catalogName: "aliceCo/ci-deploy-bot", prefix: "bobCo/") {
                            grants { prefix }
                        }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            remove_again["errors"].is_null(),
            "re-removing an absent grant should be an idempotent no-op: {remove_again}"
        );
        assert_eq!(
            grant_count(&pool, &sa_user_id).await,
            3,
            "a no-op removal must not change the grant set"
        );

        // Bob cannot remove grants of an account he doesn't manage.
        let remove_unmanaged: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        removeServiceAccountGrant(catalogName: "aliceCo/ci-deploy-bot", prefix: "aliceCo/data/") { catalogName }
                    }"#
                }),
                Some(&bob_token),
            )
            .await;
        assert!(remove_unmanaged["errors"].is_array());

        // === Kill switches: revoke all tokens, remove all grants ===

        // Mint two fresh credentials so revokeAllApiKeys has
        // something to act on (the token minted earlier was already revoked).
        for detail in ["ci-one", "ci-two"] {
            let minted: serde_json::Value = server
                .graphql(
                    &serde_json::json!({
                        "query": r#"
                        mutation($detail: String!) {
                            createApiKey(catalogName: "aliceCo/ci-deploy-bot", detail: $detail, validFor: "P30D") { id }
                        }"#,
                        "variables": { "detail": detail }
                    }),
                    Some(&alice_token),
                )
                .await;
            assert!(minted["errors"].is_null(), "mint should succeed: {minted}");
        }

        // A non-manager cannot trip the kill switch.
        let bob_revoke_all: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation { revokeAllApiKeys(catalogName: "aliceCo/ci-deploy-bot") { catalogName } }"#
                }),
                Some(&bob_token),
            )
            .await;
        assert!(
            bob_revoke_all["errors"].is_array(),
            "a non-manager must not revoke all tokens: {bob_revoke_all}"
        );

        // The manager revokes both active tokens in one call; the returned
        // account has no active tokens left.
        let revoke_all: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation { revokeAllApiKeys(catalogName: "aliceCo/ci-deploy-bot") { apiKeys { id } } }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            revoke_all["errors"].is_null(),
            "revoke all should succeed: {revoke_all}"
        );
        assert_eq!(
            revoke_all["data"]["revokeAllApiKeys"]["apiKeys"]
                .as_array()
                .unwrap()
                .len(),
            0,
            "no active tokens should remain after the kill switch: {revoke_all}"
        );

        // A second call is an idempotent no-op (not an error), and the account
        // still reports no active tokens — proving the first call persisted.
        let revoke_all_again: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation { revokeAllApiKeys(catalogName: "aliceCo/ci-deploy-bot") { apiKeys { id } } }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            revoke_all_again["errors"].is_null(),
            "revoking all again should be an idempotent no-op: {revoke_all_again}"
        );
        assert_eq!(
            revoke_all_again["data"]["revokeAllApiKeys"]["apiKeys"]
                .as_array()
                .unwrap()
                .len(),
            0,
            "still no active tokens on the second call: {revoke_all_again}"
        );

        // A non-manager cannot strip an account's grants either.
        let bob_remove_all: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation { removeAllServiceAccountGrants(catalogName: "aliceCo/ci-deploy-bot") { catalogName } }"#
                }),
                Some(&bob_token),
            )
            .await;
        assert!(
            bob_remove_all["errors"].is_array(),
            "a non-manager must not remove all grants: {bob_remove_all}"
        );

        // The manager strips every grant in one call; the returned account has
        // an empty grant set. It currently holds three: aliceCo/, aliceCo/data/,
        // and aliceCo/ops/.
        let remove_all: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation { removeAllServiceAccountGrants(catalogName: "aliceCo/ci-deploy-bot") { grants { prefix } } }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            remove_all["errors"].is_null(),
            "remove all should succeed: {remove_all}"
        );
        assert_eq!(
            remove_all["data"]["removeAllServiceAccountGrants"]["grants"]
                .as_array()
                .unwrap()
                .len(),
            0,
            "the returned account should have no grants: {remove_all}"
        );
        assert_eq!(grant_count(&pool, &sa_user_id).await, 0);

        // A second call is an idempotent no-op.
        let remove_all_again: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation { removeAllServiceAccountGrants(catalogName: "aliceCo/ci-deploy-bot") { grants { prefix } } }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            remove_all_again["errors"].is_null(),
            "removing all again should be an idempotent no-op: {remove_all_again}"
        );
        assert_eq!(
            remove_all_again["data"]["removeAllServiceAccountGrants"]["grants"]
                .as_array()
                .unwrap()
                .len(),
            0,
            "still no grants on the second call: {remove_all_again}"
        );
    }

    /// The management gates accept the fine-grained capabilities the feature
    /// defines, not only the full `Admin` bundle: a caller holding `TeamAdmin`
    /// (which confers `ManageServiceAccounts` + `CreateGrant`) but NOT `Admin`
    /// can manage service accounts, while the per-grant `CreateGrant` check
    /// still bounds how far they can extend an account's reach.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_team_admin_manages_without_full_admin(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        // Carol holds the TeamAdmin bundle on aliceCo/ and nothing else: her
        // grant carries no legacy capability ('none'), so her bits come solely
        // from the bundle — ManageServiceAccounts and CreateGrant, but none of
        // the wider Admin-bundle bits. This is the caller class the gates were
        // narrowed to admit. Seeded before the snapshot so authorization
        // observes it.
        let carol_uid = uuid::Uuid::from_bytes([0x33; 16]);
        sqlx::query("INSERT INTO auth.users (id, email) VALUES ($1, 'carol@example.test')")
            .bind(carol_uid)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO public.user_grants (user_id, object_role, capability, bundles)
             VALUES ($1, 'aliceCo/', 'none', ARRAY['team_admin']::capability_bundle[])",
        )
        .bind(carol_uid)
        .execute(&pool)
        .await
        .unwrap();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let carol_token = server.make_access_token(carol_uid, Some("carol@example.test"));

        // Create succeeds: the anchor gate accepts ManageServiceAccounts, and
        // the per-grant gate accepts CreateGrant on aliceCo/data/ (covered by
        // Carol's aliceCo/ bundle) — all without her holding full Admin.
        let create: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($grants: [UserGrantInput!]!) {
                        createServiceAccount(
                            catalogName: "aliceCo/team-bot"
                            grants: $grants
                        ) { catalogName createdByEmail }
                    }"#,
                    "variables": {
                        "grants": [ { "prefix": "aliceCo/data/", "capability": "read" } ]
                    }
                }),
                Some(&carol_token),
            )
            .await;
        assert!(
            create["errors"].is_null(),
            "a TeamAdmin without full Admin should create a service account: {create}"
        );
        assert_eq!(
            create["data"]["createServiceAccount"]["createdByEmail"], "carol@example.test",
            "createdByEmail should be the calling team admin's email: {create}"
        );

        // The anchor-only mutation createApiKey gates on CreateApiKey, which
        // Carol's bundle also confers.
        let token: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createApiKey(catalogName: "aliceCo/team-bot", detail: "ci", validFor: "P30D") { id }
                    }"#
                }),
                Some(&carol_token),
            )
            .await;
        assert!(
            token["errors"].is_null(),
            "a TeamAdmin should mint a service account token: {token}"
        );

        // addServiceAccountGrant to a prefix Carol can confer (she holds
        // CreateGrant across aliceCo/) succeeds.
        let add_ok: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        addServiceAccountGrant(catalogName: "aliceCo/team-bot", prefix: "aliceCo/ops/", capability: write) { catalogName }
                    }"#
                }),
                Some(&carol_token),
            )
            .await;
        assert!(
            add_ok["errors"].is_null(),
            "granting a prefix the team admin can confer should succeed: {add_ok}"
        );

        // Anti-escalation: Carol lacks CreateGrant on bobCo/, so she cannot
        // extend the account there — managing an account does not let her widen
        // its reach beyond what she could grant. This is the boundary now
        // sitting at CreateGrant rather than Admin.
        let add_escalation: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        addServiceAccountGrant(catalogName: "aliceCo/team-bot", prefix: "bobCo/", capability: read) { catalogName }
                    }"#
                }),
                Some(&carol_token),
            )
            .await;
        assert!(
            add_escalation["errors"].is_array(),
            "a team admin must not grant a prefix she lacks CreateGrant on: {add_escalation}"
        );

        // The same boundary binds at creation time: seeding a foreign grant fails.
        let create_escalation: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createServiceAccount(
                            catalogName: "aliceCo/overreach-bot"
                            grants: [{ prefix: "bobCo/", capability: read }]
                        ) { catalogName }
                    }"#
                }),
                Some(&carol_token),
            )
            .await;
        assert!(
            create_escalation["errors"].is_array(),
            "seeding a grant beyond the team admin's CreateGrant must fail: {create_escalation}"
        );
    }
}
