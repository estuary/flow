use super::TimestampCursor;
use async_graphql::{Context, types::connection};

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
    pub uses: i32,
    /// True once the token's validity window has elapsed
    /// (now is past `updated_at + valid_for`).
    pub expired: bool,
}

pub type PaginatedRefreshTokens = connection::Connection<
    TimestampCursor,
    RefreshTokenInfo,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

#[derive(Debug, Default)]
pub struct RefreshTokensQuery;

const DEFAULT_PAGE_SIZE: usize = 25;

#[async_graphql::Object]
impl RefreshTokensQuery {
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
                        uses AS "uses!: i32",
                        (now() > updated_at + valid_for) AS "expired!: bool"
                    FROM refresh_tokens
                    WHERE user_id = $1
                      AND valid_for <> interval '0'
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
                                uses: r.uses,
                                expired: r.expired,
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
pub struct RefreshTokensMutation;

#[async_graphql::Object]
impl RefreshTokensMutation {
    /// Create a refresh token for the authenticated user.
    async fn create_refresh_token(
        &self,
        ctx: &Context<'_>,
        #[graphql(
            desc = "ISO 8601 duration for token validity (e.g. P90D); must be greater than zero and at most one year",
            default_with = "String::from(\"P90D\")"
        )]
        valid_for: String,
        #[graphql(default = true)] multi_use: bool,
        #[graphql(default)] detail: Option<String>,
    ) -> async_graphql::Result<RefreshTokenResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        // ISO 8601 durations begin with 'P'; considering this cheap and good enough validation for now.
        if !valid_for.starts_with('P') {
            return Err(async_graphql::Error::new(
                "validFor must be an ISO 8601 duration (e.g. P90D)",
            ));
        }

        // Service accounts authenticate exclusively via API keys, which are
        // expiring and revocable. A refresh token bypasses both, so deny
        // issuance to SA principals.
        if super::service_accounts::is_service_account(&env.pg_pool, claims.sub).await? {
            return Err(async_graphql::Error::new(
                "service accounts cannot create refresh tokens; authenticate with an API key instead",
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
                v.valid_for,
                crypt(nt.secret, gen_salt('bf')),
                $4
            FROM new_token nt, (SELECT $3::text::interval AS valid_for) v
            WHERE v.valid_for > interval '0' AND v.valid_for <= interval '366 days'
            RETURNING
                id AS "id!: models::Id",
                (SELECT secret FROM new_token) AS "secret!: String"
            "#,
            claims.sub,
            multi_use,
            valid_for,
            detail.as_deref(),
        )
        .fetch_optional(&env.pg_pool)
        .await
        .map_err(|err| {
            // Postgres raises SQLSTATE 22007 (invalid_datetime_format) for a
            // malformed interval and 22015 (interval_field_overflow) for one too
            // extreme to parse; both are client errors rather than internal faults.
            let code = err.as_database_error().and_then(|e| e.code());
            if matches!(code.as_deref(), Some("22007" | "22015")) {
                async_graphql::Error::new("validFor must be a valid ISO 8601 duration (e.g. P90D)")
            } else {
                tracing::error!(?err, "failed to create refresh token");
                async_graphql::Error::new("failed to create refresh token")
            }
        })?
        .ok_or_else(|| {
            async_graphql::Error::new("validFor must be greater than zero and at most one year")
        })?;

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

    /// Revoke a refresh token owned by the authenticated user.
    ///
    /// Rather than deleting the row, we zero its `valid_for` interval, which
    /// marks the token as expired/invalid while preserving the audit trail.
    /// Already-zeroed (revoked) tokens are treated as not found.
    async fn revoke_refresh_token(
        &self,
        ctx: &Context<'_>,
        id: models::Id,
    ) -> async_graphql::Result<bool> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        let result = sqlx::query!(
            "UPDATE refresh_tokens SET valid_for = interval '0' \
             WHERE id = $1 AND user_id = $2 AND valid_for <> interval '0'",
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
            "revoked refresh token"
        );

        Ok(true)
    }
}

#[cfg(test)]
mod test {
    use crate::test_server;

    /// Encode a refresh token as the bearer-credential form accepted by the
    /// Envelope extractor: standard base64 of `{"id": ..., "secret": ...}`.
    fn bearer_refresh_token(id: &str, secret: &str) -> String {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD
            .encode(serde_json::json!({ "id": id, "secret": secret }).to_string())
    }

    /// Covers the refresh-token GraphQL surface (create → list → revoke, plus
    /// the `validFor` validation and not-found idempotency guards), the
    /// `/api/v1/auth/token` refresh-token dispatch, rejection of a refresh
    /// token presented as a bearer credential when its secret is bad or it has
    /// been revoked, and the guard denying refresh tokens to service-account
    /// principals.
    ///
    /// The happy-path *exchange* — `generate_access_token` actually signing a
    /// JWT — is intentionally not exercised here: it reads `app.jwt_secret` from
    /// `vault.decrypted_secrets` and calls pgjwt's `sign()`, neither of which
    /// exists in the sqlx::test DB (only `auth`/`stripe` are polyfilled). That
    /// signing path is covered by the pgTAP `test_generate_access_token`. The
    /// assertions here all fail inside `generate_access_token` *before* signing
    /// (bad secret, expired/revoked token, or an unknown grant), so they're
    /// deterministic without the vault/pgjwt setup.
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
        let token_secret = created["secret"]
            .as_str()
            .expect("should return a secret")
            .to_string();

        // === Invalid validFor values are rejected at the boundary ===
        // Zero (which aliases the revoked sentinel), over a year, and
        // non-ISO-8601 syntax all fail rather than creating an unusable token.
        for bad in ["PT0S", "P2Y", "90 days"] {
            let rejected: serde_json::Value = server
                .graphql(
                    &serde_json::json!({
                        "query": r#"
                        mutation($v: String!) {
                            createRefreshToken(validFor: $v) { id }
                        }"#,
                        "variables": { "v": bad }
                    }),
                    Some(&alice_token),
                )
                .await;
            assert!(
                rejected["errors"].is_array(),
                "validFor {bad:?} should be rejected: {rejected}"
            );
        }

        // An interval too extreme for Postgres to even parse (SQLSTATE 22015,
        // interval_field_overflow) is surfaced as the same sanitized client
        // error, not a leaked DB string or an internal fault.
        let overflow: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($v: String!) {
                        createRefreshToken(validFor: $v) { id }
                    }"#,
                    "variables": { "v": "P300000000000Y" }
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            overflow["errors"][0]["message"],
            "validFor must be a valid ISO 8601 duration (e.g. P90D)",
            "an unparseable validFor should yield the sanitized client error: {overflow}"
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

        // === A bad secret presented as a bearer credential is rejected ===
        // The Envelope exchanges a refresh-token bearer for an access token via
        // generate_access_token; a wrong secret fails there (before signing),
        // so the request is rejected with 401.
        let bad_bearer = bearer_refresh_token(&token_id, "not-the-real-secret");
        let rejected = server
            .rest_client()
            .post(
                "/api/graphql",
                &serde_json::json!({ "query": "query { refreshTokens { edges { node { id } } } }" }),
                Some(&bad_bearer),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(rejected.status(), reqwest::StatusCode::UNAUTHORIZED);

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
            body.contains("invalid, expired, or unknown refresh token"),
            "bad refresh secret rejection body: {body}"
        );

        // === Revoke the refresh token ===
        let revoke: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation($id: Id!) { revokeRefreshToken(id: $id) }"#,
                    "variables": { "id": token_id }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            revoke["errors"].is_null(),
            "revoke should succeed: {revoke}"
        );
        assert_eq!(revoke["data"]["revokeRefreshToken"], true);

        // A revoked token no longer authenticates as a bearer credential:
        // revocation zeroes its validity window, which generate_access_token
        // rejects as expired (before signing).
        let bearer = bearer_refresh_token(&token_id, &token_secret);
        let rejected = server
            .rest_client()
            .post(
                "/api/graphql",
                &serde_json::json!({ "query": "query { refreshTokens { edges { node { id } } } }" }),
                Some(&bearer),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(rejected.status(), reqwest::StatusCode::UNAUTHORIZED);

        // It's revoked, so it no longer appears in the list.
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
            0,
            "the revoked token is the only one, so the list is now empty"
        );

        // Revoking again fails (not-found guard).
        let revoke_again: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation($id: Id!) { revokeRefreshToken(id: $id) }"#,
                    "variables": { "id": token_id }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(revoke_again["errors"].is_array());

        // === Service accounts cannot create refresh tokens ===
        // They authenticate via API keys, which are expiring and revocable; a
        // refresh token would bypass both, so issuance to an SA principal must
        // be denied.
        let create_sa: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createServiceAccount(
                            catalogName: "aliceCo/refresh-token-bot"
                            grants: [{ prefix: "aliceCo/", capability: admin }]
                        ) { catalogName }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            create_sa["errors"].is_null(),
            "create SA should succeed: {create_sa}"
        );

        // The API doesn't expose the SA's backing user_id, so read it from the
        // DB to mint a token whose `sub` is the service account (an SA principal
        // has no email), standing in for an authenticated SA caller.
        let sa_user_id: uuid::Uuid = sqlx::query_scalar(
            "SELECT user_id FROM internal.service_accounts WHERE catalog_name = 'aliceCo/refresh-token-bot'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let sa_token = server.make_access_token(sa_user_id, None);

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
