use super::TimestampCursor;
use async_graphql::{Context, types::connection};

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct ServiceAccount {
    pub catalog_name: models::Name,
    pub created_by: uuid::Uuid,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
    pub grants: Vec<ServiceAccountGrant>,
    pub tokens: Vec<ServiceAccountTokenInfo>,
}

/// A user_grant to seed a service account with at creation time.
#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct ServiceAccountGrantInput {
    pub prefix: models::Prefix,
    pub capability: models::Capability,
}

/// A user_grant held by a service account: the prefix it may act on and the
/// capability it holds there. An account's access is the union of its grants,
/// which may span multiple prefixes independent of its catalog_name anchor.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct ServiceAccountGrant {
    pub prefix: models::Prefix,
    pub capability: models::Capability,
    pub detail: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// A service-account credential: a multi-use refresh token owned by the account
/// and minted by an administrator. The secret itself is returned only once at
/// creation (see [`CreateServiceAccountTokenResult`]).
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct ServiceAccountTokenInfo {
    pub id: models::Id,
    pub detail: Option<String>,
    pub created_by: uuid::Uuid,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct CreateServiceAccountTokenResult {
    pub id: models::Id,
    /// The bearer credential, returned exactly once. Present it as an
    /// `Authorization: Bearer` token or exchange it at `POST /api/v1/auth/token`.
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
        // holding ManageServiceAccount on a prefix covering the account's
        // catalog_name.
        let user_accessible_prefixes = super::authorized_prefixes::authorized_prefixes(
            &snapshot.role_grants,
            &snapshot.user_grants,
            env.claims()?.sub,
            models::authz::Capability::ManageServiceAccount,
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
                        sa.created_by,
                        sa.created_at AS "created_at!: chrono::DateTime<chrono::Utc>",
                        sa.updated_at AS "updated_at!: chrono::DateTime<chrono::Utc>",
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

                // Tokens are batch-loaded for the whole page in one query (no
                // N+1). The tradeoff is that this runs even when the caller
                // didn't select `tokens`.
                let token_rows = if user_ids.is_empty() {
                    vec![]
                } else {
                    sqlx::query!(
                        r#"
                        SELECT
                            rt.id AS "id!: models::Id",
                            rt.user_id,
                            rt.detail,
                            rt.created_by AS "created_by!: uuid::Uuid",
                            rt.created_at AS "created_at!: chrono::DateTime<chrono::Utc>",
                            (rt.updated_at + rt.valid_for) AS "expires_at!: chrono::DateTime<chrono::Utc>",
                            CASE WHEN rt.uses > 0 THEN rt.updated_at END AS "last_used_at: chrono::DateTime<chrono::Utc>"
                        FROM public.refresh_tokens rt
                        WHERE rt.user_id = ANY($1)
                          AND rt.valid_for <> interval '0'
                        ORDER BY rt.created_at DESC
                        "#,
                        &user_ids,
                    )
                    .fetch_all(&env.pg_pool)
                    .await?
                };

                let mut tokens_by_sa: std::collections::HashMap<
                    uuid::Uuid,
                    Vec<ServiceAccountTokenInfo>,
                > = std::collections::HashMap::new();
                for tr in token_rows {
                    tokens_by_sa
                        .entry(tr.user_id)
                        .or_default()
                        .push(ServiceAccountTokenInfo {
                            id: tr.id,
                            detail: tr.detail,
                            created_by: tr.created_by,
                            created_at: tr.created_at,
                            expires_at: tr.expires_at,
                            last_used_at: tr.last_used_at,
                        });
                }

                // Grants are batch-loaded for the whole page in one query,
                // mirroring tokens (same N+1 tradeoff). An account's reach is
                // the union of these grants, so they're a list rather than a
                // single capability.
                let grant_rows = if user_ids.is_empty() {
                    vec![]
                } else {
                    sqlx::query!(
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
                        &user_ids,
                    )
                    .fetch_all(&env.pg_pool)
                    .await?
                };

                let mut grants_by_sa: std::collections::HashMap<
                    uuid::Uuid,
                    Vec<ServiceAccountGrant>,
                > = std::collections::HashMap::new();
                for gr in grant_rows {
                    grants_by_sa
                        .entry(gr.user_id)
                        .or_default()
                        .push(ServiceAccountGrant {
                            prefix: gr.prefix,
                            capability: gr.capability,
                            detail: gr.detail,
                            created_at: gr.created_at,
                            updated_at: gr.updated_at,
                        });
                }

                let edges: Vec<_> = sa_rows
                    .into_iter()
                    .take(limit)
                    .map(|r| {
                        let tokens = tokens_by_sa.remove(&r.user_id).unwrap_or_default();
                        let grants = grants_by_sa.remove(&r.user_id).unwrap_or_default();
                        connection::Edge::new(
                            TimestampCursor(r.created_at),
                            ServiceAccount {
                                catalog_name: models::Name::new(&r.catalog_name),
                                created_by: r.created_by,
                                created_at: r.created_at,
                                updated_at: r.updated_at,
                                last_used_at: r.last_used_at,
                                grants,
                                tokens,
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
    /// The caller must have ManageServiceAccount on the catalog name AND
    /// CreateGrant on each granted prefix. Creates an auth.users row, an
    /// internal.service_accounts row, and a user_grants row per requested
    /// grant.
    async fn create_service_account(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
        grants: Vec<ServiceAccountGrantInput>,
    ) -> async_graphql::Result<ServiceAccount> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        if let Err(err) = validator::Validate::validate(&catalog_name) {
            return Err(async_graphql::Error::new(format!(
                "invalid catalog name: {err}"
            )));
        }
        // Managing the account (here, creating it under this anchor) requires
        // ManageServiceAccount on the catalog name — the same capability that
        // gates listing in ServiceAccountsQuery, so the read and write surfaces
        // agree. This is deliberately narrower than full Admin.
        super::verify_authorization(
            env,
            catalog_name.as_str(),
            models::authz::Capability::ManageServiceAccount,
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

        let now = sqlx::query_scalar!(
            r#"
            INSERT INTO internal.service_accounts (user_id, catalog_name, created_by)
            VALUES ($1, $2::text::catalog_name, $3)
            RETURNING created_at AS "created_at!: chrono::DateTime<chrono::Utc>"
            "#,
            sa_user_id,
            catalog_name.as_str(),
            claims.sub,
        )
        .fetch_one(&mut *txn)
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

        // The seeded grants were just written in this transaction, so their
        // detail and timestamps match what `overwrite_user_grant` persisted.
        let grants = grants
            .into_iter()
            .map(|grant| ServiceAccountGrant {
                prefix: grant.prefix,
                capability: grant.capability,
                detail: Some("service account grant".to_string()),
                created_at: now,
                updated_at: now,
            })
            .collect();

        Ok(ServiceAccount {
            catalog_name,
            created_by: claims.sub,
            created_at: now,
            updated_at: now,
            last_used_at: None,
            grants,
            tokens: vec![],
        })
    }

    /// Add a user_grant to a service account.
    ///
    /// The caller must manage the service account (ManageServiceAccount on its
    /// catalog name) AND have CreateGrant on the granted prefix. The second
    /// requirement prevents a caller from extending an account's access beyond
    /// what they could grant anyone. (Human-user grant creation still lives in
    /// PostgREST; when it migrates to GraphQL it should gate on this same
    /// CreateGrant capability.)
    async fn add_service_account_grant(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
        prefix: models::Prefix,
        capability: models::Capability,
    ) -> async_graphql::Result<bool> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        super::verify_authorization(
            env,
            catalog_name.as_str(),
            models::authz::Capability::ManageServiceAccount,
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

        Ok(true)
    }

    /// Remove a user_grant from a service account.
    ///
    /// The caller must manage the service account (ManageServiceAccount on its
    /// catalog name). Unlike addServiceAccountGrant, no capability on the
    /// grant's prefix is required: removal only ever narrows the account's
    /// access, so managers may remove ANY grant — including grants to
    /// prefixes they don't themselves administer.
    async fn remove_service_account_grant(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
        prefix: models::Prefix,
    ) -> async_graphql::Result<bool> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        super::verify_authorization(
            env,
            catalog_name.as_str(),
            models::authz::Capability::ManageServiceAccount,
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

    /// Remove ALL user_grants from a service account, stripping its access in
    /// one call.
    ///
    /// The caller must manage the service account (ManageServiceAccount on its
    /// catalog name). As with removeServiceAccountGrant, no capability on the
    /// grants' prefixes is required: removal only narrows access, so a manager
    /// may clear grants to prefixes they don't themselves administer. Returns
    /// the number of grants removed (0 if the account had none — not an error).
    async fn remove_all_service_account_grants(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
    ) -> async_graphql::Result<i32> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        super::verify_authorization(
            env,
            catalog_name.as_str(),
            models::authz::Capability::ManageServiceAccount,
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

        Ok(deleted.rows_affected() as i32)
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
    /// The caller must have ManageServiceAccount on the account's catalog name.
    async fn create_service_account_token(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
        detail: String,
        #[graphql(desc = "ISO 8601 duration for token validity (e.g. P90D, P1Y)")]
        valid_for: String,
    ) -> async_graphql::Result<CreateServiceAccountTokenResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        super::verify_authorization(
            env,
            catalog_name.as_str(),
            models::authz::Capability::ManageServiceAccount,
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
                tracing::error!(?err, "failed to create service account token");
                async_graphql::Error::new("failed to create service account token")
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
            token_id = %row.id,
            %user_id,
            %detail,
            %claims.sub,
            "created service account token"
        );

        Ok(CreateServiceAccountTokenResult { id: row.id, secret })
    }

    /// Revoke a service-account token.
    ///
    /// The caller must have ManageServiceAccount capability on the owning service
    /// account's catalog name.
    ///
    /// Rather than deleting the row, we zero its `valid_for` interval, which
    /// makes the token inert (it fails the exchange's expiry check and is
    /// excluded from listings) while preserving the audit trail. Already-revoked
    /// tokens are treated as not found.
    async fn revoke_service_account_token(
        &self,
        ctx: &Context<'_>,
        id: models::Id,
    ) -> async_graphql::Result<bool> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        let catalog_name = sqlx::query_scalar!(
            r#"
            SELECT sa.catalog_name AS "catalog_name!: String"
            FROM public.refresh_tokens rt
            JOIN internal.service_accounts sa ON sa.user_id = rt.user_id
            WHERE rt.id = $1 AND rt.valid_for <> interval '0'
            "#,
            id as models::Id,
        )
        .fetch_optional(&env.pg_pool)
        .await?;

        let catalog_name = match catalog_name {
            Some(name) => name,
            None => return Err(async_graphql::Error::new("service account token not found")),
        };

        super::verify_authorization(
            env,
            &catalog_name,
            models::authz::Capability::ManageServiceAccount,
        )
        .await?;

        sqlx::query!(
            "UPDATE public.refresh_tokens SET valid_for = interval '0' \
             WHERE id = $1 AND valid_for <> interval '0'",
            id as models::Id
        )
        .execute(&env.pg_pool)
        .await?;

        tracing::info!(
            token_id = %id,
            service_account = %catalog_name,
            %claims.sub,
            "revoked service account token"
        );

        Ok(true)
    }

    /// Revoke ALL of a service account's tokens at once — the credential kill
    /// switch.
    ///
    /// The caller must have ManageServiceAccount on the account's catalog name.
    /// Like revokeServiceAccountToken, each token is made inert by zeroing its
    /// `valid_for` interval (preserving the audit trail) rather than deleted;
    /// already-revoked tokens are skipped. A service account's user_id only ever
    /// owns its own minted credentials, so this targets exactly those. Returns
    /// the number of tokens revoked (0 if none were active — not an error).
    async fn revoke_all_service_account_tokens(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
    ) -> async_graphql::Result<i32> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        super::verify_authorization(
            env,
            catalog_name.as_str(),
            models::authz::Capability::ManageServiceAccount,
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
            "revoked all service account tokens"
        );

        Ok(revoked.rows_affected() as i32)
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
                    mutation($catalogName: Name!, $grants: [ServiceAccountGrantInput!]!) {
                        createServiceAccount(
                            catalogName: $catalogName
                            grants: $grants
                        ) {
                            catalogName
                            createdBy
                            createdAt
                            updatedAt
                            lastUsedAt
                            grants { prefix capability detail createdAt updatedAt }
                            tokens { id }
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
        assert_eq!(sa["tokens"].as_array().unwrap().len(), 0);

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
        let create_token: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($detail: String!, $validFor: String!) {
                        createServiceAccountToken(
                            catalogName: "aliceCo/ci-deploy-bot"
                            detail: $detail
                            validFor: $validFor
                        ) {
                            id
                            secret
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
            create_token["errors"].is_null(),
            "create token should succeed: {create_token}"
        );
        let token_data = &create_token["data"]["createServiceAccountToken"];
        let token_id = token_data["id"]
            .as_str()
            .expect("should have id")
            .to_string();
        let secret = token_data["secret"]
            .as_str()
            .expect("should have secret")
            .to_string();
        // The credential is the unified refresh-token bearer form (base64 JSON),
        // not the retired flow_sa_ key format.
        assert!(
            !secret.starts_with("flow_sa_"),
            "credential should be the refresh-token bearer form: {secret}"
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
                            createServiceAccountToken(catalogName: "aliceCo/ci-deploy-bot", detail: $detail, validFor: $validFor) { id }
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
        let parsed_token_id: models::Id = token_id.parse().unwrap();
        sqlx::query!(
            "UPDATE public.refresh_tokens SET uses = 1, updated_at = now() WHERE id = $1",
            parsed_token_id as models::Id,
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
                                    lastUsedAt
                                    grants { prefix capability detail }
                                    tokens {
                                        id
                                        detail
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
        assert_eq!(edges[0]["node"]["catalogName"], "aliceCo/ci-deploy-bot");
        let listed_token = &edges[0]["node"]["tokens"][0];
        assert_eq!(edges[0]["node"]["tokens"].as_array().unwrap().len(), 1);
        assert_eq!(listed_token["detail"], "GitHub Actions");
        assert_eq!(
            listed_token["createdBy"], "11111111-1111-1111-1111-111111111111",
            "token createdBy should be the calling admin: {list}"
        );
        assert!(
            listed_token["createdAt"].is_string() && listed_token["expiresAt"].is_string(),
            "token createdAt/expiresAt should be set: {list}"
        );
        // We stamped a use above, so the derived last_used_at is populated.
        assert!(
            listed_token["lastUsedAt"].is_string(),
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

        // === A bad secret is rejected statefully ===
        // generate_access_token checks the secret before signing, so this 401s
        // deterministically even without the signing setup. Build a bearer form
        // for the real token id but a wrong secret.
        use base64::Engine;
        let bad_bearer = base64::engine::general_purpose::STANDARD
            .encode(serde_json::json!({ "id": token_id, "secret": "wrong-secret" }).to_string());
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
        let revoke: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($id: Id!) {
                        revokeServiceAccountToken(id: $id)
                    }"#,
                    "variables": { "id": token_id }
                }),
                Some(&alice_token),
            )
            .await;

        assert!(
            revoke["errors"].is_null(),
            "revoke should succeed: {revoke}"
        );

        // The row is preserved with valid_for zeroed — revocation is a soft
        // delete for audit purposes. The listing assertion below can't observe
        // this distinction, so check the table directly.
        let zeroed = sqlx::query_scalar!(
            r#"SELECT valid_for = interval '0' AS "zeroed!" FROM public.refresh_tokens WHERE id = $1"#,
            parsed_token_id as models::Id,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(zeroed, "revocation must zero valid_for, not delete the row");

        // Revoking again fails: already-revoked tokens are treated as not found.
        let revoke_again: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($id: Id!) {
                        revokeServiceAccountToken(id: $id)
                    }"#,
                    "variables": { "id": token_id }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            revoke_again["errors"][0]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("service account token not found"),
            "re-revoking should report not found: {revoke_again}"
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
            body.contains("invalid, expired, or unknown refresh token"),
            "revoked token rejection body: {body}"
        );

        // The revoked token is excluded from listings, even though its row remains.
        let list_after_revoke: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        serviceAccounts { edges { node { tokens { id } } } }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            list_after_revoke["data"]["serviceAccounts"]["edges"][0]["node"]["tokens"]
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
                        addServiceAccountGrant(catalogName: "aliceCo/ci-deploy-bot", prefix: "aliceCo/", capability: read)
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
                        addServiceAccountGrant(catalogName: "aliceCo/ci-deploy-bot", prefix: "bobCo/", capability: read)
                    }"#
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
                    mutation {
                        addServiceAccountGrant(catalogName: "aliceCo/ci-deploy-bot", prefix: "aliceCo/ops/", capability: write)
                    }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(add["errors"].is_null(), "add grant should succeed: {add}");
        assert_eq!(grant_count(&pool, &sa_user_id).await, 3);

        // aliceCo/ops/ now holds `write`. Adding a LOWER capability overwrites
        // the existing grant in place: addServiceAccountGrant replaces the
        // capability rather than only ever raising it, so a manager can narrow
        // a grant in one call without removing it first.
        let downgrade: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        addServiceAccountGrant(catalogName: "aliceCo/ci-deploy-bot", prefix: "aliceCo/ops/", capability: read)
                    }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            downgrade["errors"].is_null(),
            "narrowing a grant should succeed by overwriting it: {downgrade}"
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
                        removeServiceAccountGrant(catalogName: "aliceCo/ci-deploy-bot", prefix: "bobCo/")
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

        // Removing an absent grant reports not found.
        let remove_again: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        removeServiceAccountGrant(catalogName: "aliceCo/ci-deploy-bot", prefix: "bobCo/")
                    }"#
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
                    mutation {
                        removeServiceAccountGrant(catalogName: "aliceCo/ci-deploy-bot", prefix: "aliceCo/data/")
                    }"#
                }),
                Some(&bob_token),
            )
            .await;
        assert!(remove_unmanaged["errors"].is_array());

        // === Kill switches: revoke all tokens, remove all grants ===

        // Mint two fresh credentials so revokeAllServiceAccountTokens has
        // something to act on (the token minted earlier was already revoked).
        for detail in ["ci-one", "ci-two"] {
            let minted: serde_json::Value = server
                .graphql(
                    &serde_json::json!({
                        "query": r#"
                        mutation($detail: String!) {
                            createServiceAccountToken(catalogName: "aliceCo/ci-deploy-bot", detail: $detail, validFor: "P30D") { id }
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
                    "query": r#"mutation { revokeAllServiceAccountTokens(catalogName: "aliceCo/ci-deploy-bot") }"#
                }),
                Some(&bob_token),
            )
            .await;
        assert!(
            bob_revoke_all["errors"].is_array(),
            "a non-manager must not revoke all tokens: {bob_revoke_all}"
        );

        // The manager revokes both active tokens in one call.
        let revoke_all: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation { revokeAllServiceAccountTokens(catalogName: "aliceCo/ci-deploy-bot") }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            revoke_all["data"]["revokeAllServiceAccountTokens"], 2,
            "both active tokens should be revoked: {revoke_all}"
        );

        // A second call is an idempotent zero-count no-op (not an error),
        // proving the first call's revocation persisted.
        let revoke_all_again: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation { revokeAllServiceAccountTokens(catalogName: "aliceCo/ci-deploy-bot") }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            revoke_all_again["data"]["revokeAllServiceAccountTokens"], 0,
            "revoking again should be a zero-count no-op: {revoke_all_again}"
        );

        // A non-manager cannot strip an account's grants either.
        let bob_remove_all: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation { removeAllServiceAccountGrants(catalogName: "aliceCo/ci-deploy-bot") }"#
                }),
                Some(&bob_token),
            )
            .await;
        assert!(
            bob_remove_all["errors"].is_array(),
            "a non-manager must not remove all grants: {bob_remove_all}"
        );

        // The manager strips every grant in one call. The account currently
        // holds three: aliceCo/, aliceCo/data/, and aliceCo/ops/.
        let remove_all: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation { removeAllServiceAccountGrants(catalogName: "aliceCo/ci-deploy-bot") }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            remove_all["data"]["removeAllServiceAccountGrants"], 3,
            "all three grants should be removed: {remove_all}"
        );
        assert_eq!(grant_count(&pool, &sa_user_id).await, 0);

        // A second call is an idempotent zero-count no-op.
        let remove_all_again: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation { removeAllServiceAccountGrants(catalogName: "aliceCo/ci-deploy-bot") }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            remove_all_again["data"]["removeAllServiceAccountGrants"], 0,
            "removing again should be a zero-count no-op: {remove_all_again}"
        );
    }

    /// The management gates accept the fine-grained capabilities the feature
    /// defines, not only the full `Admin` bundle: a caller holding `TeamAdmin`
    /// (which confers `ManageServiceAccount` + `CreateGrant`) but NOT `Admin`
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
        // from the bundle — ManageServiceAccount and CreateGrant, but none of
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

        // Create succeeds: the anchor gate accepts ManageServiceAccount, and
        // the per-grant gate accepts CreateGrant on aliceCo/data/ (covered by
        // Carol's aliceCo/ bundle) — all without her holding full Admin.
        let create: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($grants: [ServiceAccountGrantInput!]!) {
                        createServiceAccount(
                            catalogName: "aliceCo/team-bot"
                            grants: $grants
                        ) { catalogName createdBy }
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
            create["data"]["createServiceAccount"]["createdBy"],
            "33333333-3333-3333-3333-333333333333",
            "createdBy should be the calling team admin: {create}"
        );

        // The anchor-only mutation createServiceAccountToken also accepts ManageServiceAccount.
        let token: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createServiceAccountToken(catalogName: "aliceCo/team-bot", detail: "ci", validFor: "P30D") { id }
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
                        addServiceAccountGrant(catalogName: "aliceCo/team-bot", prefix: "aliceCo/ops/", capability: write)
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
                        addServiceAccountGrant(catalogName: "aliceCo/team-bot", prefix: "bobCo/", capability: read)
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
