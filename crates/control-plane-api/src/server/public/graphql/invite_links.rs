use async_graphql::Context;

/// Result of creating an invite link.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct CreateInviteLinkResult {
    /// The secret token for this invite link.
    pub token: uuid::Uuid,
    /// The catalog prefix this invite link grants access to.
    pub catalog_prefix: models::Prefix,
    /// The capability level granted by this invite link.
    pub capability: models::Capability,
}

/// Result of redeeming an invite link.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct RedeemInviteLinkResult {
    /// The catalog prefix that was granted.
    pub catalog_prefix: models::Prefix,
    /// The capability level that was granted.
    pub capability: models::Capability,
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
        uses_remaining: Option<i64>,
        detail: Option<String>,
    ) -> async_graphql::Result<CreateInviteLinkResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        if let Err(err) = validator::Validate::validate(&catalog_prefix) {
            return Err(async_graphql::Error::new(format!(
                "invalid catalog prefix: {err}"
            )));
        }

        verify_authorization(env, &catalog_prefix).await?;

        let token = sqlx::query_scalar!(
            r#"
            INSERT INTO internal.invite_links (catalog_prefix, capability, uses_remaining, detail, created_by)
            VALUES ($1::text::catalog_prefix, $2, $3, $4, $5)
            RETURNING token
            "#,
            catalog_prefix.as_str(),
            capability as models::Capability,
            uses_remaining,
            detail.as_deref(),
            claims.sub,
        )
        .fetch_one(&env.pg_pool)
        .await?;

        tracing::info!(
            %catalog_prefix,
            ?capability,
            %claims.sub,
            "created invite link"
        );

        Ok(CreateInviteLinkResult {
            token,
            catalog_prefix,
            capability,
        })
    }

    /// Redeem an invite link token, granting the caller access to the associated
    /// catalog prefix.
    ///
    /// Optionally specify a `requestedPrefix` that is more specific than (a suffix of)
    /// the invite link's catalog prefix.
    pub async fn redeem_invite_link(
        &self,
        ctx: &Context<'_>,
        token: uuid::Uuid,
        requested_prefix: Option<models::Prefix>,
    ) -> async_graphql::Result<RedeemInviteLinkResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        if let Some(ref prefix) = requested_prefix {
            if let Err(err) = validator::Validate::validate(prefix) {
                return Err(async_graphql::Error::new(format!(
                    "invalid requested prefix: {err}"
                )));
            }
        }

        let mut txn = env.pg_pool.begin().await?;

        // Look up the invite link and decrement uses_remaining atomically.
        let invite = sqlx::query!(
            r#"
            UPDATE internal.invite_links
            SET
                uses_remaining = uses_remaining - 1,
                updated_at = now()
            WHERE token = $1
            AND (uses_remaining IS NULL OR uses_remaining > 0)
            RETURNING
                catalog_prefix AS "catalog_prefix!: String",
                capability AS "capability!: models::Capability"
            "#,
            token,
        )
        .fetch_optional(&mut *txn)
        .await?;

        let invite = match invite {
            Some(row) => row,
            None => {
                // Distinguish between "not found" and "exhausted".
                let exists = sqlx::query_scalar!(
                    "SELECT 1 as \"x!: i32\" FROM internal.invite_links WHERE token = $1",
                    token,
                )
                .fetch_optional(&mut *txn)
                .await?;

                return Err(if exists.is_some() {
                    async_graphql::Error::new(
                        "This invite link has been fully redeemed and can no longer be used",
                    )
                } else {
                    async_graphql::Error::new("Invalid invite link token")
                });
            }
        };

        let granted_prefix = models::Prefix::new(&invite.catalog_prefix);

        // If a more specific prefix was requested, validate it's a suffix.
        let effective_prefix = match &requested_prefix {
            Some(requested) => {
                if !requested.starts_with(granted_prefix.as_str()) {
                    return Err(async_graphql::Error::new(format!(
                        "Requested prefix '{}' is not within the invite link's prefix '{}'",
                        requested.as_str(),
                        granted_prefix.as_str(),
                    )));
                }
                requested.as_str()
            }
            None => granted_prefix.as_str(),
        };

        // Upsert the user grant (only upgrades capability, never downgrades).
        crate::directives::grant::upsert_user_grant(
            claims.sub,
            effective_prefix,
            invite.capability,
            Some("granted via invite link".to_string()),
            &mut txn,
        )
        .await?;

        txn.commit().await?;

        tracing::info!(
            %claims.sub,
            %effective_prefix,
            ?invite.capability,
            "redeemed invite link"
        );

        Ok(RedeemInviteLinkResult {
            catalog_prefix: models::Prefix::new(effective_prefix),
            capability: invite.capability,
        })
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
                            usesRemaining: 2
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

        // Verify Bob's grant was created.
        let grant = sqlx::query!(
            r#"
            SELECT object_role, capability AS "capability!: String"
            FROM user_grants
            WHERE user_id = '22222222-2222-2222-2222-222222222222'
            "#,
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(grant.len(), 1);
        assert_eq!(grant[0].object_role, "aliceCo/");
        assert_eq!(grant[0].capability, "write");
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_invite_link_with_requested_prefix(pool: sqlx::PgPool) {
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

        // Create an invite for aliceCo/ but Bob requests a more specific prefix.
        let create_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createInviteLink(
                            catalogPrefix: "aliceCo/"
                            capability: write
                        ) { token }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;

        let invite_token = create_response["data"]["createInviteLink"]["token"]
            .as_str()
            .unwrap();

        // Bob redeems with a more specific prefix.
        let redeem_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        redeemInviteLink(
                            token: $token
                            requestedPrefix: "aliceCo/data/"
                        ) {
                            catalogPrefix
                            capability
                        }
                    }"#,
                    "variables": { "token": invite_token }
                }),
                Some(&bob_token),
            )
            .await;

        insta::assert_json_snapshot!("redeem_with_requested_prefix", redeem_response);
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
                            usesRemaining: 1
                        ) { token }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;

        let invite_token = create_response["data"]["createInviteLink"]["token"]
            .as_str()
            .unwrap();

        // First redemption succeeds.
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

        // Second redemption fails (exhausted).
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

        // Requested prefix outside invite scope fails.
        let create_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createInviteLink(
                            catalogPrefix: "aliceCo/data/"
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

        let bad_prefix_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        redeemInviteLink(
                            token: $token
                            requestedPrefix: "somethingElse/"
                        ) { catalogPrefix capability }
                    }"#,
                    "variables": { "token": invite_token }
                }),
                Some(&bob_token),
            )
            .await;

        insta::assert_json_snapshot!("redeem_bad_prefix", bad_prefix_response);
    }
}
