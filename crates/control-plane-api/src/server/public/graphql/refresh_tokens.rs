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
    pub valid_for: String,
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
                        valid_for::text AS "valid_for!: String",
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
                                valid_for: r.valid_for,
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
            desc = "ISO 8601 duration for token validity (e.g. P90D)",
            default_with = "String::from(\"P90D\")"
        )]
        valid_for: String,
        #[graphql(default = true)] multi_use: bool,
        #[graphql(default)] detail: Option<String>,
    ) -> async_graphql::Result<RefreshTokenResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        let row = sqlx::query!(
            r#"
            WITH new_token AS (
                -- 256 bits from pgcrypto's CSPRNG, up from a UUID's 122. The
                -- SHA-256 hashing below rests on secrets being high-entropy.
                SELECT encode(gen_random_bytes(32), 'hex') AS secret
            )
            INSERT INTO refresh_tokens (user_id, multi_use, valid_for, hash, detail)
            SELECT
                $1,
                $2,
                $3::text::interval,
                -- SHA-256 rather than bcrypt: the secret is high-entropy
                -- random, so a slow hash adds no protection while a fast
                -- hash keeps per-request bearer verification cheap. See
                -- migration 20260611120000_refresh_token_sha256.sql.
                encode(digest(nt.secret, 'sha256'), 'hex'),
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
        .await?;

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
    /// the not-found idempotency guard), the `/api/v1/auth/token`
    /// refresh-token dispatch, and direct bearer authentication with a
    /// refresh token via the Envelope extractor.
    ///
    /// The happy-path *exchange* — `generate_access_token` actually signing a
    /// JWT — is intentionally not exercised here: it reads `app.jwt_secret` from
    /// `vault.decrypted_secrets` and calls pgjwt's `sign()`, neither of which
    /// exists in the sqlx::test DB (only `auth`/`stripe` are polyfilled). That
    /// signing path is covered by the pgTAP `test_generate_access_token`. We
    /// instead assert the endpoint routes the `refresh_token` grant and rejects
    /// a bad secret — which fails in `generate_access_token` *before* signing,
    /// so it's deterministic without the vault/pgjwt setup.
    ///
    /// The *bearer* authentication path needs no signing at all — the Envelope
    /// validates the credential against the database and constructs claims
    /// directly — so its happy path IS exercised here.
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

        // === The refresh token authenticates directly as a bearer credential ===
        // The Envelope validates it against the database and constructs claims
        // without minting (or verifying) a JWT. The listing returned by this
        // very request proves authentication resolved to alice's identity, and
        // shows `uses` incremented by the authentication that served it.
        let bearer = bearer_refresh_token(&token_id, &token_secret);
        let via_bearer: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"query { refreshTokens { edges { node { id uses } } } }"#
                }),
                Some(&bearer),
            )
            .await;
        let edges = via_bearer["data"]["refreshTokens"]["edges"]
            .as_array()
            .expect("bearer-authenticated request should succeed");
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0]["node"]["id"], token_id);
        assert_eq!(edges[0]["node"]["uses"], 1);

        // A bad secret is rejected at the envelope.
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

        // A single-use token is rejected as a bearer credential: it exists for
        // one-shot exchange via /api/v1/auth/token, and accepting it here would
        // silently consume it.
        let create_single: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createRefreshToken(validFor: "P30D", multiUse: false) {
                            id
                            secret
                        }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;
        let single = &create_single["data"]["createRefreshToken"];
        let single_bearer = bearer_refresh_token(
            single["id"].as_str().unwrap(),
            single["secret"].as_str().unwrap(),
        );
        let rejected = server
            .rest_client()
            .post(
                "/api/graphql",
                &serde_json::json!({ "query": "query { refreshTokens { edges { node { id } } } }" }),
                Some(&single_bearer),
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
            body.contains("failed to exchange refresh token"),
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

        // A revoked token no longer authenticates as a bearer credential.
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
            1,
            "only the single-use token remains"
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
    }

    /// Covers the bcrypt → SHA-256 hash migration for refresh tokens:
    /// newly-created tokens store SHA-256 digests, and legacy bcrypt rows
    /// still authenticate as bearer credentials and are rewritten to SHA-256
    /// on first successful use ("rehash on use").
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_refresh_token_hash_migration(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let alice_id = uuid::Uuid::from_bytes([0x11; 16]);
        let alice_token = server.make_access_token(alice_id, Some("alice@example.test"));

        // === Newly-created tokens store SHA-256, not bcrypt ===
        let create: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation { createRefreshToken(validFor: "P30D") { id secret } }"#
                }),
                Some(&alice_token),
            )
            .await;
        let created = &create["data"]["createRefreshToken"];
        let token_id: models::Id = created["id"].as_str().unwrap().parse().unwrap();

        let hash = sqlx::query_scalar!(
            r#"SELECT hash FROM refresh_tokens WHERE id = $1"#,
            token_id as models::Id,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            !hash.starts_with("$2") && hash.len() == 64,
            "new tokens should store hex SHA-256, got: {hash}"
        );

        // === A legacy bcrypt row authenticates and is rehashed on use ===
        let legacy_secret = "legacy-bcrypt-secret";
        let legacy_id = sqlx::query_scalar!(
            r#"
            INSERT INTO refresh_tokens (user_id, multi_use, valid_for, hash, detail)
            VALUES ($1, true, interval '30 days', crypt($2, gen_salt('bf')), 'legacy')
            RETURNING id AS "id!: models::Id"
            "#,
            alice_id,
            legacy_secret,
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let bearer = bearer_refresh_token(&legacy_id.to_string(), legacy_secret);
        let via_bearer: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"query { refreshTokens { edges { node { id } } } }"#
                }),
                Some(&bearer),
            )
            .await;
        assert!(
            via_bearer["data"]["refreshTokens"]["edges"].is_array(),
            "legacy bcrypt bearer should authenticate: {via_bearer}"
        );

        let rehashed = sqlx::query_scalar!(
            r#"SELECT hash FROM refresh_tokens WHERE id = $1"#,
            legacy_id as models::Id,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            !rehashed.starts_with("$2") && rehashed.len() == 64,
            "legacy hash should be rewritten to SHA-256 on use, got: {rehashed}"
        );

        // The rehashed token continues to authenticate (now via the SHA-256 path).
        let again: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"query { refreshTokens { edges { node { id uses } } } }"#
                }),
                Some(&bearer),
            )
            .await;
        assert!(
            again["data"]["refreshTokens"]["edges"].is_array(),
            "rehashed token should still authenticate: {again}"
        );
    }
}
