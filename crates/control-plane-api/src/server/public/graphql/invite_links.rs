use super::filters;
use async_graphql::{Context, types::connection};

/// An invite link that grants access to a catalog prefix.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct InviteLink {
    /// The secret token for this invite link.
    pub token: uuid::Uuid,
    /// The catalog prefix this invite link grants access to.
    pub catalog_prefix: models::Prefix,
    /// The capability level granted by this invite link.
    pub capability: models::Capability,
    /// Whether this invite link can only be used once.
    pub single_use: bool,
    /// Optional description of this invite link.
    pub detail: Option<String>,
    /// When this invite link was created.
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Result of redeeming an invite link.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct RedeemInviteLinkResult {
    /// The catalog prefix that was granted.
    pub catalog_prefix: models::Prefix,
    /// The capability level that was granted.
    pub capability: models::Capability,
}

pub type PaginatedInviteLinks = connection::Connection<
    String,
    InviteLink,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct InviteLinksFilter {
    pub single_use: Option<filters::BoolFilter>,
    pub catalog_prefix: Option<filters::PrefixFilter>,
}

#[derive(Debug, Default)]
pub struct InviteLinksQuery;

const DEFAULT_PAGE_SIZE: usize = 25;
const MAX_PREFIXES: usize = 20;

#[async_graphql::Object]
impl InviteLinksQuery {
    /// List invite links the caller has admin access to.
    ///
    /// Returns invite links under all prefixes where the caller has admin
    /// capability, optionally narrowed by a prefix filter.
    async fn invite_links(
        &self,
        ctx: &Context<'_>,
        filter: Option<InviteLinksFilter>,
        after: Option<String>,
        first: Option<i32>,
    ) -> async_graphql::Result<PaginatedInviteLinks> {
        let env = ctx.data::<crate::Envelope>()?;

        let single_use_eq = filter
            .as_ref()
            .and_then(|f| f.single_use.as_ref())
            .and_then(|f| f.eq);
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
            return Ok(PaginatedInviteLinks::new(false, false));
        }
        if admin_prefixes.len() > MAX_PREFIXES {
            return Err(async_graphql::Error::new(
                "Too many admin prefixes; narrow results with a prefix filter",
            ));
        }

        connection::query(
            after,
            None,
            first,
            None,
            |after: Option<String>, _, first, _| async move {
                let after_token: Option<uuid::Uuid> = match after {
                    Some(s) => Some(
                        s.parse()
                            .map_err(|_| async_graphql::Error::new("invalid cursor"))?,
                    ),
                    None => None,
                };

                let limit = first.unwrap_or(DEFAULT_PAGE_SIZE);

                let rows = sqlx::query!(
                    r#"
                SELECT
                    token,
                    catalog_prefix AS "catalog_prefix!: String",
                    capability AS "capability!: models::Capability",
                    single_use AS "single_use!: bool",
                    detail,
                    created_at AS "created_at!: chrono::DateTime<chrono::Utc>"
                FROM internal.invite_links
                WHERE catalog_prefix::text ^@ ANY($1)
                  AND ($5::text IS NULL OR catalog_prefix::text ^@ $5)
                  AND ($4::bool IS NULL OR single_use = $4)
                  AND ($2::uuid IS NULL OR token > $2)
                ORDER BY token
                LIMIT $3 + 1
                "#,
                    &admin_prefixes,
                    after_token,
                    limit as i64,
                    single_use_eq,
                    prefix_starts_with.as_deref(),
                )
                .fetch_all(&env.pg_pool)
                .await?;

                let has_next = rows.len() > limit;

                let edges: Vec<_> = rows
                    .into_iter()
                    .take(limit)
                    .map(|r| {
                        let cursor = r.token.to_string();
                        connection::Edge::new(
                            cursor,
                            InviteLink {
                                token: r.token,
                                catalog_prefix: models::Prefix::new(&r.catalog_prefix),
                                capability: r.capability,
                                single_use: r.single_use,
                                detail: r.detail,
                                created_at: r.created_at,
                            },
                        )
                    })
                    .collect();

                let mut conn = connection::Connection::new(after_token.is_some(), has_next);
                conn.edges = edges;
                async_graphql::Result::<PaginatedInviteLinks>::Ok(conn)
            },
        )
        .await
    }
}

#[derive(Debug, Default)]
pub struct InviteLinksMutation;

#[async_graphql::Object]
impl InviteLinksMutation {
    /// Create an invite link that grants access to a catalog prefix.
    ///
    /// The caller must have admin capability on the catalog prefix.
    /// Share the returned token with the intended recipient out-of-band.
    pub async fn create_invite_link(
        &self,
        ctx: &Context<'_>,
        catalog_prefix: models::Prefix,
        capability: models::Capability,
        #[graphql(default = true)] single_use: bool,
        detail: Option<String>,
    ) -> async_graphql::Result<InviteLink> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        if let Err(err) = validator::Validate::validate(&catalog_prefix) {
            return Err(async_graphql::Error::new(format!(
                "invalid catalog prefix: {err}"
            )));
        }

        verify_authorization(env, &catalog_prefix).await?;

        let row = sqlx::query!(
            r#"
            INSERT INTO internal.invite_links (catalog_prefix, capability, single_use, detail)
            VALUES ($1::text::catalog_prefix, $2, $3, $4)
            RETURNING token, created_at AS "created_at!: chrono::DateTime<chrono::Utc>"
            "#,
            catalog_prefix.as_str(),
            capability as models::Capability,
            single_use,
            detail.as_deref(),
        )
        .fetch_one(&env.pg_pool)
        .await?;

        tracing::info!(
            %catalog_prefix,
            ?capability,
            %claims.sub,
            "created invite link"
        );

        Ok(InviteLink {
            token: row.token,
            catalog_prefix,
            capability,
            single_use,
            detail,
            created_at: row.created_at,
        })
    }

    /// Redeem an invite link token, granting the caller access to the associated
    /// catalog prefix with the specified capability.
    pub async fn redeem_invite_link(
        &self,
        ctx: &Context<'_>,
        token: uuid::Uuid,
    ) -> async_graphql::Result<RedeemInviteLinkResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        let mut txn = env.pg_pool.begin().await?;

        // Look up and lock the invite link (single-use links are deleted upon redemption).
        let invite = sqlx::query!(
            r#"
            SELECT
                catalog_prefix AS "catalog_prefix!: String",
                capability AS "capability!: models::Capability",
                single_use AS "single_use!: bool"
            FROM internal.invite_links
            WHERE token = $1
            FOR UPDATE
            "#,
            token,
        )
        .fetch_optional(&mut *txn)
        .await?;

        let invite = match invite {
            Some(row) => row,
            None => {
                return Err(async_graphql::Error::new("Invalid invite link"));
            }
        };

        // Delete single-use invite links upon redemption.
        if invite.single_use {
            sqlx::query!("DELETE FROM internal.invite_links WHERE token = $1", token,)
                .execute(&mut *txn)
                .await?;
        }

        // Upsert the user grant (only upgrades capability, never downgrades).
        crate::directives::grant::upsert_user_grant(
            claims.sub,
            &invite.catalog_prefix,
            invite.capability,
            Some("granted via invite link".to_string()),
            &mut txn,
        )
        .await?;

        txn.commit().await?;

        tracing::info!(
            %claims.sub,
            %invite.catalog_prefix,
            ?invite.capability,
            "redeemed invite link"
        );

        Ok(RedeemInviteLinkResult {
            catalog_prefix: models::Prefix::new(&invite.catalog_prefix),
            capability: invite.capability,
        })
    }

    /// Delete an invite link, revoking it so it can no longer be redeemed.
    ///
    /// The caller must have admin capability on the invite link's catalog prefix.
    pub async fn delete_invite_link(
        &self,
        ctx: &Context<'_>,
        token: uuid::Uuid,
    ) -> async_graphql::Result<bool> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        let mut txn = env.pg_pool.begin().await?;

        // Look up the invite link to verify authorization.
        let invite = sqlx::query!(
            r#"
            SELECT catalog_prefix AS "catalog_prefix!: String"
            FROM internal.invite_links
            WHERE token = $1
            FOR UPDATE
            "#,
            token,
        )
        .fetch_optional(&mut *txn)
        .await?;

        let invite = match invite {
            Some(row) => row,
            None => return Err(async_graphql::Error::new("Invalid invite link")),
        };

        verify_authorization(env, &invite.catalog_prefix).await?;

        sqlx::query!("DELETE FROM internal.invite_links WHERE token = $1", token,)
            .execute(&mut *txn)
            .await?;

        txn.commit().await?;

        tracing::info!(
            %token,
            %invite.catalog_prefix,
            %claims.sub,
            "deleted invite link"
        );

        Ok(true)
    }
}

/// Ensures the user has admin capability on the catalog prefix.
async fn verify_authorization(
    envelope: &crate::Envelope,
    catalog_prefix: &str,
) -> async_graphql::Result<()> {
    let policy_result = crate::server::evaluate_names_authorization(
        envelope.snapshot(),
        envelope.claims()?,
        models::Capability::Admin,
        [catalog_prefix],
    );
    let (_expiry, ()) = envelope.authorization_outcome(policy_result).await?;
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::test_server;

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_invite_link_create_and_redeem(pool: sqlx::PgPool) {
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

        // Create a bob user who will redeem the invite.
        sqlx::query("INSERT INTO auth.users (id, email) VALUES ('22222222-2222-2222-2222-222222222222', 'bob@example.test')")
            .execute(&pool)
            .await
            .unwrap();

        let bob_token =
            server.make_access_token(uuid::Uuid::from_bytes([0x22; 16]), Some("bob@example.test"));

        // Alice creates an invite link for aliceCo/data/ with write capability.
        let create_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($prefix: Prefix!, $capability: Capability!) {
                        createInviteLink(
                            catalogPrefix: $prefix
                            capability: $capability
                            singleUse: false
                            detail: "for bob"
                        ) {
                            token
                            catalogPrefix
                            capability
                        }
                    }"#,
                    "variables": {
                        "prefix": "aliceCo/",
                        "capability": "write"
                    }
                }),
                Some(&alice_token),
            )
            .await;

        // Extract the token from the response.
        let invite_token = create_response["data"]["createInviteLink"]["token"]
            .as_str()
            .expect("should have token");

        insta::assert_json_snapshot!("create_invite", create_response, {
            ".data.createInviteLink.token" => "[token]"
        });

        // Bob redeems the invite link.
        let redeem_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        redeemInviteLink(token: $token) {
                            catalogPrefix
                            capability
                        }
                    }"#,
                    "variables": {
                        "token": invite_token
                    }
                }),
                Some(&bob_token),
            )
            .await;

        insta::assert_json_snapshot!("redeem_invite", redeem_response);

        // Multi-use link survives the first redemption — Bob can redeem again.
        let redeem_again: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        redeemInviteLink(token: $token) {
                            catalogPrefix
                            capability
                        }
                    }"#,
                    "variables": {
                        "token": invite_token
                    }
                }),
                Some(&bob_token),
            )
            .await;

        insta::assert_json_snapshot!("redeem_multi_use_again", redeem_again);

        // Verify the link still exists in the DB (was not deleted).
        let link_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM internal.invite_links WHERE token = $1::uuid")
                .bind(invite_token)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(link_count, 1, "multi-use link should not be deleted");
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_invite_link_errors(pool: sqlx::PgPool) {
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

        sqlx::query("INSERT INTO auth.users (id, email) VALUES ('22222222-2222-2222-2222-222222222222', 'bob@example.test')")
            .execute(&pool)
            .await
            .unwrap();

        let bob_token =
            server.make_access_token(uuid::Uuid::from_bytes([0x22; 16]), Some("bob@example.test"));

        // Creating an invite link with an invalid prefix (no trailing slash) returns an error.
        let invalid_prefix: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createInviteLink(
                            catalogPrefix: "noTrailingSlash"
                            capability: read
                        ) { token }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;

        insta::assert_json_snapshot!("create_invalid_prefix", invalid_prefix);

        // Alice cannot create an invite link for a prefix she doesn't admin.
        let unauthorized_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createInviteLink(
                            catalogPrefix: "notAlice/"
                            capability: read
                        ) { token }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;

        insta::assert_json_snapshot!("create_unauthorized", unauthorized_response);

        // Invalid token returns error.
        let bad_token_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        redeemInviteLink(
                            token: "00000000-0000-0000-0000-000000000000"
                        ) { catalogPrefix capability }
                    }"#
                }),
                Some(&bob_token),
            )
            .await;

        insta::assert_json_snapshot!("redeem_bad_token", bad_token_response);

        // Create a single-use invite and exhaust it.
        let create_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createInviteLink(
                            catalogPrefix: "aliceCo/"
                            capability: read
                            singleUse: true
                        ) { token }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;

        let invite_token = create_response["data"]["createInviteLink"]["token"]
            .as_str()
            .unwrap();

        // First redemption succeeds (and deletes the row).
        let _: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        redeemInviteLink(token: $token) { catalogPrefix capability }
                    }"#,
                    "variables": { "token": invite_token }
                }),
                Some(&bob_token),
            )
            .await;

        // Second redemption fails (row was deleted).
        let exhausted_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        redeemInviteLink(token: $token) { catalogPrefix capability }
                    }"#,
                    "variables": { "token": invite_token }
                }),
                Some(&bob_token),
            )
            .await;

        insta::assert_json_snapshot!("redeem_exhausted", exhausted_response);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_delete_invite_link(pool: sqlx::PgPool) {
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

        sqlx::query("INSERT INTO auth.users (id, email) VALUES ('22222222-2222-2222-2222-222222222222', 'bob@example.test')")
            .execute(&pool)
            .await
            .unwrap();

        let bob_token =
            server.make_access_token(uuid::Uuid::from_bytes([0x22; 16]), Some("bob@example.test"));

        // Alice creates an invite link.
        let create_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createInviteLink(
                            catalogPrefix: "aliceCo/"
                            capability: read
                        ) { token }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;

        let invite_token = create_response["data"]["createInviteLink"]["token"]
            .as_str()
            .unwrap();

        // Alice deletes the invite link.
        let delete_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        deleteInviteLink(token: $token)
                    }"#,
                    "variables": { "token": invite_token }
                }),
                Some(&alice_token),
            )
            .await;

        insta::assert_json_snapshot!("delete_invite", delete_response);

        // Attempting to redeem the deleted link fails.
        let redeem_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        redeemInviteLink(token: $token) { catalogPrefix capability }
                    }"#,
                    "variables": { "token": invite_token }
                }),
                Some(&bob_token),
            )
            .await;

        insta::assert_json_snapshot!("redeem_after_delete", redeem_response);

        // Bob cannot delete alice's invite links.
        let create_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createInviteLink(
                            catalogPrefix: "aliceCo/"
                            capability: read
                        ) { token }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;

        let invite_token = create_response["data"]["createInviteLink"]["token"]
            .as_str()
            .unwrap();

        let unauthorized_delete: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        deleteInviteLink(token: $token)
                    }"#,
                    "variables": { "token": invite_token }
                }),
                Some(&bob_token),
            )
            .await;

        insta::assert_json_snapshot!("delete_unauthorized", unauthorized_delete);

        // Deleting a nonexistent token returns an error.
        let bad_delete: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        deleteInviteLink(
                            token: "00000000-0000-0000-0000-000000000000"
                        )
                    }"#
                }),
                Some(&alice_token),
            )
            .await;

        insta::assert_json_snapshot!("delete_bad_token", bad_delete);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_invite_links_query(pool: sqlx::PgPool) {
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

        // Create several invite links with different capabilities and single_use settings.
        for (cap, single_use, detail) in [
            ("read", true, Some("read-only single-use")),
            ("write", false, Some("write multi-use")),
            ("admin", true, None),
        ] {
            let _: serde_json::Value = server
                .graphql(
                    &serde_json::json!({
                        "query": r#"
                        mutation($prefix: Prefix!, $capability: Capability!, $singleUse: Boolean!, $detail: String) {
                            createInviteLink(
                                catalogPrefix: $prefix
                                capability: $capability
                                singleUse: $singleUse
                                detail: $detail
                            ) { token }
                        }"#,
                        "variables": {
                            "prefix": "aliceCo/",
                            "capability": cap,
                            "singleUse": single_use,
                            "detail": detail,
                        }
                    }),
                    Some(&alice_token),
                )
                .await;
        }

        // List all invite links for aliceCo/.
        let list_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        inviteLinks(filter: { catalogPrefix: { startsWith: "aliceCo/" } }) {
                            edges {
                                node {
                                    catalogPrefix
                                    capability
                                    singleUse
                                    detail
                                }
                            }
                        }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;

        // All three links should be returned. Mask tokens/timestamps but verify structure.
        let edges = list_response["data"]["inviteLinks"]["edges"]
            .as_array()
            .expect("should have edges");
        assert_eq!(edges.len(), 3, "should list all 3 invite links");

        // Filter to only single-use links.
        let filtered_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        inviteLinks(filter: { catalogPrefix: { startsWith: "aliceCo/" }, singleUse: { eq: true } }) {
                            edges {
                                node {
                                    catalogPrefix
                                    capability
                                    singleUse
                                    detail
                                }
                            }
                        }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;

        let filtered_edges = filtered_response["data"]["inviteLinks"]["edges"]
            .as_array()
            .expect("should have edges");
        assert_eq!(
            filtered_edges.len(),
            2,
            "should list only 2 single-use links"
        );
        for edge in filtered_edges {
            assert_eq!(edge["node"]["singleUse"], true);
        }

        // Pagination: request first 2, then page forward.
        let page1: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        inviteLinks(first: 2) {
                            edges {
                                cursor
                                node { capability }
                            }
                            pageInfo { hasNextPage hasPreviousPage }
                        }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;

        let page1_edges = page1["data"]["inviteLinks"]["edges"]
            .as_array()
            .expect("should have edges");
        assert_eq!(page1_edges.len(), 2);
        assert_eq!(
            page1["data"]["inviteLinks"]["pageInfo"]["hasNextPage"],
            true
        );

        let last_cursor = page1_edges[1]["cursor"].as_str().unwrap();

        let page2: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query($after: String) {
                        inviteLinks(first: 2, after: $after) {
                            edges {
                                node { capability }
                            }
                            pageInfo { hasNextPage hasPreviousPage }
                        }
                    }"#,
                    "variables": {
                        "after": last_cursor,
                    }
                }),
                Some(&alice_token),
            )
            .await;

        let page2_edges = page2["data"]["inviteLinks"]["edges"]
            .as_array()
            .expect("should have edges");
        assert_eq!(page2_edges.len(), 1);
        assert_eq!(
            page2["data"]["inviteLinks"]["pageInfo"]["hasNextPage"],
            false
        );
        assert_eq!(
            page2["data"]["inviteLinks"]["pageInfo"]["hasPreviousPage"],
            true
        );

        // Bob cannot list alice's invite links.
        sqlx::query("INSERT INTO auth.users (id, email) VALUES ('22222222-2222-2222-2222-222222222222', 'bob@example.test')")
            .execute(&pool)
            .await
            .unwrap();

        let bob_token =
            server.make_access_token(uuid::Uuid::from_bytes([0x22; 16]), Some("bob@example.test"));

        // Bob has no admin prefixes, so listing returns empty results.
        let bob_list: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        inviteLinks(filter: { catalogPrefix: { startsWith: "aliceCo/" } }) {
                            edges { node { catalogPrefix } }
                        }
                    }"#
                }),
                Some(&bob_token),
            )
            .await;

        let bob_edges = bob_list["data"]["inviteLinks"]["edges"]
            .as_array()
            .expect("should have edges");
        assert_eq!(bob_edges.len(), 0, "bob should see no invite links");

        // Create an invite link under a sub-prefix to exercise the filter mechanism:
        // Alice has an admin grant on "aliceCo/", the filter is "aliceCo/data/", and
        // the invite link is at "aliceCo/data/invite/". The Rust filter includes the
        // "aliceCo/" grant (because "aliceCo/data/" starts with "aliceCo/") and the
        // SQL filter narrows to catalog_prefix ^@ "aliceCo/data/".
        let _: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($prefix: Prefix!, $capability: Capability!) {
                        createInviteLink(
                            catalogPrefix: $prefix
                            capability: $capability
                            singleUse: true
                        ) { token }
                    }"#,
                    "variables": {
                        "prefix": "aliceCo/data/invite/",
                        "capability": "write"
                    }
                }),
                Some(&alice_token),
            )
            .await;

        let sub_prefix_list: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        inviteLinks(filter: { catalogPrefix: { startsWith: "aliceCo/data/" } }) {
                            edges { node { catalogPrefix } }
                        }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;

        let sub_prefix_edges = sub_prefix_list["data"]["inviteLinks"]["edges"]
            .as_array()
            .expect("should have edges");
        assert_eq!(
            sub_prefix_edges.len(),
            1,
            "sub-prefix filter returns the invite link under aliceCo/data/"
        );
        assert_eq!(
            sub_prefix_edges[0]["node"]["catalogPrefix"],
            "aliceCo/data/invite/"
        );

        // Complementary scenario: filter prefix is a parent of the grant.
        // User grant is "aliceCo/", filter is "alice" — the grant starts with
        // the filter, so it's included, and all invite links under "aliceCo/"
        // are returned (the original 3 plus the one at "aliceCo/data/invite/").
        let parent_filter_list: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        inviteLinks(filter: { catalogPrefix: { startsWith: "alice" } }) {
                            edges { node { catalogPrefix } }
                        }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;

        let parent_filter_edges = parent_filter_list["data"]["inviteLinks"]["edges"]
            .as_array()
            .expect("should have edges");
        assert_eq!(
            parent_filter_edges.len(),
            4,
            "parent prefix filter returns all invite links under the grant"
        );
    }
}
