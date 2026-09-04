use super::{Sensitive, TimestampCursor};
use async_graphql::{Context, types::connection};

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct RefreshTokenResult {
    pub id: models::Id,
    pub secret: Sensitive,
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
    /// Catalog prefix this token is confined to, or null if it carries the
    /// owner's full authority.
    ///
    /// A scoped token's authority is intersected with what this prefix reaches
    /// through role grants, so it can only ever do less than its owner could.
    pub scope_prefix: Option<models::Prefix>,
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
                        (now() > updated_at + valid_for) AS "expired!: bool",
                        scope_prefix AS "scope_prefix: String"
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
                                scope_prefix: r.scope_prefix.map(models::Prefix::new),
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

/// Validates a requested token scope and returns it as a `models::Prefix`.
///
/// A caller may only scope a token to a prefix they can read. Two things follow
/// from checking this against the caller's own [`crate::Authority`] rather than
/// against their raw grants:
///
/// - Scoping is not an escalation path. A prefix the caller cannot read is
///   rejected, so minting a scoped token never produces authority the caller
///   lacks. (The scope is a ceiling, not a grant — the token still derives its
///   authority from its owner's grants — but a caller should not be able to
///   point a credential at a namespace they cannot see.)
/// - A scoped caller can only mint equally or more narrowly scoped tokens. Their
///   Authority is already confined, so a prefix outside their own scope fails
///   this check with no separate rule needed.
async fn validate_scope_prefix(
    env: &crate::Envelope,
    scope_prefix: &str,
) -> async_graphql::Result<models::Prefix> {
    let prefix = models::Prefix::new(scope_prefix);
    if let Err(err) = validator::Validate::validate(&prefix) {
        return Err(async_graphql::Error::new(format!(
            "invalid scopePrefix: {err}"
        )));
    }

    super::verify_authorization(env, &prefix, models::authz::Capability::CatalogRead).await?;

    Ok(prefix)
}

#[derive(Debug, Default)]
pub struct RefreshTokensMutation;

#[async_graphql::Object]
impl RefreshTokensMutation {
    /// Create a refresh token for the authenticated user.
    ///
    /// Pass `scopePrefix` to confine the token to a catalog prefix. A scoped
    /// token's authority is intersected with what that prefix reaches through
    /// role grants, so it can only ever do less than the caller could. This is
    /// how a credential is handed to something that should see one tenant's data
    /// and nothing else.
    ///
    /// Service-account callers are rejected: their API keys are administered
    /// via createApiKey and revokeApiKey.
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
        #[graphql(
            desc = "Catalog prefix to confine the token to. The caller must be able to read it. Omit for a token carrying the caller's full authority."
        )]
        scope_prefix: Option<String>,
    ) -> async_graphql::Result<RefreshTokenResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        super::service_accounts::verify_not_service_account(&env.pg_pool, claims.sub).await?;

        // ISO 8601 durations begin with 'P'; considering this cheap and good enough validation for now.
        if !valid_for.starts_with('P') {
            return Err(async_graphql::Error::new(
                "validFor must be an ISO 8601 duration (e.g. P90D)",
            ));
        }

        let scope_prefix = match scope_prefix {
            Some(prefix) => Some(validate_scope_prefix(env, &prefix).await?),
            // Omitting `scopePrefix` inherits the caller's own scope rather than
            // dropping it. Otherwise minting a token would be a complete escape
            // from a scope: this mutation is keyed on the caller's user_id and
            // touches no catalog prefix, so nothing else here would confine the
            // credential it hands back.
            None => env.authority()?.scope().prefix().map(models::Prefix::new),
        };

        let row = sqlx::query!(
            r#"
            WITH new_token AS (
                SELECT gen_random_uuid()::text AS secret
            )
            INSERT INTO refresh_tokens (user_id, multi_use, valid_for, hash, detail, scope_prefix)
            SELECT
                $1,
                $2,
                v.valid_for,
                crypt(nt.secret, gen_salt('bf')),
                $4,
                $5::text::catalog_prefix
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
            scope_prefix.as_ref().map(models::Prefix::as_str),
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
            scope_prefix = ?scope_prefix,
            "created refresh token"
        );

        Ok(RefreshTokenResult {
            id: row.id,
            secret: Sensitive::new(row.secret),
        })
    }

    /// Revoke a refresh token owned by the authenticated user.
    ///
    /// Rather than deleting the row, we zero its `valid_for` interval, which
    /// marks the token as expired/invalid while preserving the audit trail.
    /// Already-zeroed (revoked) tokens are treated as not found.
    ///
    /// Service-account callers are rejected: their API keys are administered
    /// via createApiKey and revokeApiKey.
    async fn revoke_refresh_token(
        &self,
        ctx: &Context<'_>,
        id: models::Id,
    ) -> async_graphql::Result<bool> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        super::service_accounts::verify_not_service_account(&env.pg_pool, claims.sub).await?;

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
    /// `/api/v1/auth/token` refresh-token dispatch, and rejection of a refresh
    /// token presented as a bearer credential when its secret is bad or it has
    /// been revoked.
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
            body.contains("invalid, expired, or unknown credential"),
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
    }

    const ALICE: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);

    /// Ask for a refresh token confined to `scope`, returning the raw response so
    /// callers can assert on either data or errors.
    async fn mint_scoped(
        server: &test_server::TestServer,
        token: &str,
        scope: &str,
    ) -> serde_json::Value {
        server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation($s: String!) {
                        createRefreshToken(validFor: "P30D", scopePrefix: $s) { id }
                    }"#,
                    "variables": { "s": scope }
                }),
                Some(token),
            )
            .await
    }

    /// Covers what a `scope_prefix` claim does to a live request, and the gate on
    /// requesting one.
    ///
    /// Alice administers `aliceCo/` and (added below) `otherCo/`, with a role
    /// grant `aliceCo/ -> ops/dp/public/` for read — the same grant
    /// `beta_onboard` gives every real tenant. That combination exercises both
    /// halves of what a scope means: `otherCo/` disappears because nothing
    /// connects it to `aliceCo/`, while the public data plane stays visible
    /// because a role grant does connect it.
    ///
    /// Scoped tokens here are minted directly rather than by exchanging a scoped
    /// refresh token: `generate_access_token` needs pgjwt's `sign()` and the
    /// vault-held JWT secret, neither of which exists in the `sqlx::test` DB. The
    /// SQL that stamps the claim is covered by `scoped_refresh_tokens.test.sql`;
    /// what is covered here is the claim's effect once presented.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_scoped_token_narrows_requests(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        sqlx::query!(
            "INSERT INTO user_grants (user_id, object_role, capability) \
             VALUES ($1, 'otherCo/', 'admin')",
            ALICE,
        )
        .execute(&pool)
        .await
        .unwrap();

        // Ungated: these are query assertions, and a gated source serves an empty
        // first snapshot in which nobody holds any grant.
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), false).await,
        )
        .await;

        let unscoped = server.make_access_token(ALICE, Some("alice@example.com"));
        let scoped =
            server.make_scoped_access_token(ALICE, Some("alice@example.com"), Some("aliceCo/"));

        // === A scope narrows prefix-scoped queries ===
        let prefixes = |resp: serde_json::Value| -> Vec<String> {
            resp["data"]["prefixes"]["edges"]
                .as_array()
                .expect("edges")
                .iter()
                .map(|e| e["node"]["prefix"].as_str().unwrap().to_string())
                .collect()
        };
        let query = serde_json::json!({
            "query": r#"query { prefixes(by: {minCapability: admin}) { edges { node { prefix } } } }"#
        });

        let all = prefixes(server.graphql(&query, Some(&unscoped)).await);
        assert_eq!(all, vec!["aliceCo/", "otherCo/"]);

        let confined = prefixes(server.graphql(&query, Some(&scoped)).await);
        assert_eq!(
            confined,
            vec!["aliceCo/"],
            "a scope of aliceCo/ hides the unconnected tenant"
        );

        // === A scope follows role grants ===
        // `aliceCo/ -> ops/dp/public/` keeps the public data plane in scope. A
        // scope implemented as a literal prefix match would return nothing here
        // and break data-plane selection for every scoped caller.
        let data_planes = serde_json::json!({
            "query": r#"query { dataPlanes { edges { node { name } } } }"#
        });
        let planes: serde_json::Value = server.graphql(&data_planes, Some(&scoped)).await;
        let names: Vec<String> = planes["data"]["dataPlanes"]["edges"]
            .as_array()
            .expect("edges")
            .iter()
            .map(|e| e["node"]["name"].as_str().unwrap().to_string())
            .collect();
        assert!(
            names.iter().any(|n| n == "ops/dp/public/aws-us-west-2-c1"),
            "the role-granted data plane stays in scope: {names:?}"
        );

        // === Requesting a scope requires being able to read it ===
        let denied = mint_scoped(&server, &unscoped, "nobodyCo/").await;
        assert!(
            denied["errors"].is_array(),
            "a prefix Alice cannot read is refused: {denied}"
        );

        // A scoped caller can only mint equally or more narrowly scoped tokens.
        // Their own Authority is already confined, so this needs no separate rule.
        let escalation = mint_scoped(&server, &scoped, "otherCo/").await;
        assert!(
            escalation["errors"].is_array(),
            "a scoped caller cannot mint a token outside its own scope: {escalation}"
        );
        let narrower = mint_scoped(&server, &scoped, "aliceCo/data/").await;
        assert!(
            narrower["errors"].is_null(),
            "a scoped caller can mint within its own scope: {narrower}"
        );

        // === A scoped caller that omits scopePrefix inherits its own scope ===
        // Without this a scoped token could mint an unscoped one and escape
        // completely, since this mutation is keyed on user_id and touches no
        // catalog prefix.
        let inherited: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"mutation { createRefreshToken(validFor: "P30D", detail: "inherits") { id } }"#
                }),
                Some(&scoped),
            )
            .await;
        assert!(
            inherited["errors"].is_null(),
            "minting without a scope should succeed: {inherited}"
        );

        // === Scopes are reported back on the token listing ===
        let listed: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"query { refreshTokens { edges { node { detail scopePrefix } } } }"#
                }),
                Some(&unscoped),
            )
            .await;
        let scopes: Vec<(&str, Option<&str>)> = listed["data"]["refreshTokens"]["edges"]
            .as_array()
            .expect("edges")
            .iter()
            .map(|e| {
                (
                    e["node"]["detail"].as_str().unwrap_or(""),
                    e["node"]["scopePrefix"].as_str(),
                )
            })
            .collect();
        assert_eq!(
            scopes,
            vec![("inherits", Some("aliceCo/")), ("", Some("aliceCo/data/"))],
            "the inherited token carries the caller's scope, not no scope"
        );
    }
}
