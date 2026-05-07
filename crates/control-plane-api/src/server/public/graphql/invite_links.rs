use super::{TimestampCursor, filters};
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
    /// The SSO provider ID for the invite's tenant, if any.
    /// When present, the frontend should route the user directly into the SSO
    /// flow using this provider ID (e.g. via `supabase.auth.signInWithSSO`).
    pub sso_provider_id: Option<uuid::Uuid>,
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
    TimestampCursor,
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
                    il.token,
                    il.catalog_prefix AS "catalog_prefix!: String",
                    il.capability AS "capability!: models::Capability",
                    il.single_use AS "single_use!: bool",
                    il.detail,
                    il.created_at AS "created_at!: chrono::DateTime<chrono::Utc>",
                    t.sso_provider_id
                FROM internal.invite_links il
                LEFT JOIN tenants t ON il.catalog_prefix::text ^@ t.tenant
                WHERE il.catalog_prefix::text ^@ ANY($1)
                  AND ($5::text IS NULL OR il.catalog_prefix::text ^@ $5)
                  AND ($4::bool IS NULL OR il.single_use = $4)
                  AND ($2::timestamptz IS NULL OR il.created_at < $2)
                ORDER BY il.created_at DESC
                LIMIT $3 + 1
                "#,
                    &admin_prefixes,
                    after_created_at,
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
                        connection::Edge::new(
                            TimestampCursor(r.created_at),
                            InviteLink {
                                token: r.token,
                                catalog_prefix: models::Prefix::new(&r.catalog_prefix),
                                capability: r.capability,
                                single_use: r.single_use,
                                detail: r.detail,
                                created_at: r.created_at,
                                sso_provider_id: r.sso_provider_id,
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

        // Look up the tenant's SSO provider so the frontend can route the
        // invite recipient directly into the correct SSO flow.
        let tenant = models::tenant_from(catalog_prefix.as_str());

        let sso_provider_id = sqlx::query_scalar!(
            r#"
            SELECT t.sso_provider_id
            FROM tenants t
            WHERE t.tenant = $1
            "#,
            tenant,
        )
        .fetch_optional(&env.pg_pool)
        .await?
        .flatten();

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
            sso_provider_id,
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

        // If the invite's tenant enforces SSO, verify the redeeming user has
        // an identity linked to that tenant's SSO provider.
        //
        // We check auth.identities rather than session-level claims (e.g. amr)
        // because Supabase Auth excludes SAML SSO from identity linking — a user
        // with an SSO identity row can only have obtained it by authenticating
        // through SAML. If this assumption changes, we should check the JWT's
        // amr claim to verify the current session used SSO.
        let tenant = models::tenant_from(&invite.catalog_prefix);

        let sso_requirement_not_satisfied = sqlx::query_scalar!(
            r#"
            SELECT true AS "exists!"
            FROM tenants t
            WHERE t.tenant = $1
              AND t.enforce_sso IS TRUE
              AND NOT EXISTS (
                SELECT 1 FROM auth.identities ai
                WHERE ai.user_id = $2
                  AND ai.provider = 'sso:' || t.sso_provider_id::text
              )
            "#,
            tenant,
            claims.sub,
        )
        .fetch_optional(&mut *txn)
        .await?;

        if sso_requirement_not_satisfied.is_some() {
            return Err(async_graphql::Error::new(format!(
                "This organization requires SSO authentication. Please sign in via SSO to redeem this invite."
            )));
        }

        // Delete single-use invite links upon redemption.
        if invite.single_use {
            sqlx::query!("DELETE FROM internal.invite_links WHERE token = $1", token,)
                .execute(&mut *txn)
                .await?;
        }

        // Upsert the user grant (only upgrades capability, never downgrades).
        crate::grants::upsert_user_grant(
            claims.sub,
            &invite.catalog_prefix,
            invite.capability,
            Some("granted via invite link".to_string()),
            &mut txn,
        )
        .await?;

        // When the invite grants Admin, ensure the prefix has explicit read
        // grants to its tenant's private data plane and ops-tasks prefixes.
        // See `ensure_private_data_plane_grants` for why this workaround exists.
        if invite.capability == models::Capability::Admin {
            ensure_private_data_plane_grants(&mut *txn, &invite.catalog_prefix).await?;
        }

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

/// Ensure that `catalog_prefix` has read role_grants to its tenant's
/// private-data-plane and private-ops-tasks prefixes. Workaround for #2848.
///
/// # Why this is needed
///
/// The intended behavior of role_grants is that a grant on subject `acmeCo/`
/// propogates to *every* child of `acmeCo/`. Snapshot-based authorization
/// (`tables::RoleGrant::is_authorized`) implements this faithfully — it
/// walks both descendants and ancestors of the subject role.
///
/// `internal.user_roles()` (used by RLS, and therefore by every PostgREST
/// caller) only implements half: it walks *downward* from roles the user
/// holds, picking up grants whose `subject_role` is at-or-below one of those
/// roles. It does NOT walk upward to ancestor subjects.
///
/// So an admin of `acmeCo/qa/staffing-solutions/` cannot reach the grant
/// created with the private data plane `(acmeCo/, ops/dp/private/acmeCo/, read)`
/// via `user_roles()`, even though the snapshot would correctly resolve it. Every
/// RLS check against `ops/dp/private/acmeCo/...` or
/// `ops/tasks/private/acmeCo/...` therefore rejects the admin of the sub-prefix.
///
/// The workaround is to insert technically redundant grants whose
/// `subject_role` IS the sub-prefix, so `user_roles()`'s downward walk finds
/// them. We insert both the data plane prefix (direct cause of #2848) and
/// the ops-tasks prefix — the same gap applies to any RLS-gated
/// access to the data plane's logs/stats collections.
///
/// TODO(#2848): Remove this entire function and its call site once those
/// remaining `user_roles()`-based checks are migrated to snapshot-based
/// authorization. At that point sub-prefix admins will be authorized
/// correctly without the duplicate grants.
async fn ensure_private_data_plane_grants(
    txn: &mut sqlx::PgConnection,
    catalog_prefix: &str,
) -> Result<(), sqlx::Error> {
    let Some((tenant, _)) = catalog_prefix.split_once('/') else {
        return Ok(());
    };
    if tenant.is_empty() {
        return Ok(());
    }
    let grant_objects = vec![
        format!("ops/dp/private/{tenant}/"),
        format!("ops/tasks/private/{tenant}/"),
    ];

    // The object_roles here are the same two prefixes that
    // create_data_plane.rs installs at provisioning time with the tenant as
    // subject; here we install them with the (possibly sub-prefix) invite
    // prefix as subject. ON CONFLICT makes this a no-op when catalog_prefix
    // is the tenant itself (grants already exist) or when re-running.
    sqlx::query!(
        r#"
        INSERT INTO role_grants (subject_role, object_role, capability, detail)
        SELECT $1::text, object, 'read', 'sub-prefix access to private data plane'
        FROM UNNEST($2::text[]) AS t(object)
        ON CONFLICT DO NOTHING
        "#,
        catalog_prefix,
        &grant_objects,
    )
    .execute(&mut *txn)
    .await?;

    Ok(())
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

        // --- Capability upgrade/downgrade tests ---

        // Bob currently has write on aliceCo/ from the redemption above.
        // Redeeming a read invite should NOT downgrade Bob's capability.
        let read_invite: serde_json::Value = server
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
                        "prefix": "aliceCo/",
                        "capability": "read"
                    }
                }),
                Some(&alice_token),
            )
            .await;

        let read_token = read_invite["data"]["createInviteLink"]["token"]
            .as_str()
            .unwrap();

        let _: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        redeemInviteLink(token: $token) { catalogPrefix capability }
                    }"#,
                    "variables": { "token": read_token }
                }),
                Some(&bob_token),
            )
            .await;

        let bob_capability: String = sqlx::query_scalar(
            "SELECT capability::text FROM user_grants WHERE user_id = $1 AND object_role = $2",
        )
        .bind(uuid::Uuid::from_bytes([0x22; 16]))
        .bind("aliceCo/")
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            bob_capability, "write",
            "redeeming a read invite must not downgrade an existing write grant"
        );

        // Redeeming an admin invite SHOULD upgrade Bob's capability from write to admin.
        let admin_invite: serde_json::Value = server
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
                        "prefix": "aliceCo/",
                        "capability": "admin"
                    }
                }),
                Some(&alice_token),
            )
            .await;

        let admin_token = admin_invite["data"]["createInviteLink"]["token"]
            .as_str()
            .unwrap();

        let _: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        redeemInviteLink(token: $token) { catalogPrefix capability }
                    }"#,
                    "variables": { "token": admin_token }
                }),
                Some(&bob_token),
            )
            .await;

        let bob_capability: String = sqlx::query_scalar(
            "SELECT capability::text FROM user_grants WHERE user_id = $1 AND object_role = $2",
        )
        .bind(uuid::Uuid::from_bytes([0x22; 16]))
        .bind("aliceCo/")
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            bob_capability, "admin",
            "redeeming an admin invite must upgrade an existing write grant"
        );
    }

    // Regression test for #2848. Redeeming an admin invite for a sub-prefix
    // must install explicit read grants with the sub-prefix as the subject —
    // both to the tenant's private data plane prefix (for the publish-time
    // filter in publications/specs.rs) and to the ops-tasks prefix (for any
    // RLS-gated log/stats reads). Non-admin invites must not install grants.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_redeem_admin_invite_inserts_private_dp_grants(pool: sqlx::PgPool) {
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

        // A write invite for aliceCo/sub/ should NOT install private DP grants.
        let write_invite: serde_json::Value = server
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
                        "prefix": "aliceCo/sub/",
                        "capability": "write"
                    }
                }),
                Some(&alice_token),
            )
            .await;

        let write_token = write_invite["data"]["createInviteLink"]["token"]
            .as_str()
            .unwrap();

        let _: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        redeemInviteLink(token: $token) { catalogPrefix capability }
                    }"#,
                    "variables": { "token": write_token }
                }),
                Some(&bob_token),
            )
            .await;

        let count_after_write: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) FROM role_grants
            WHERE subject_role = 'aliceCo/sub/' AND capability = 'read'
            "#,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            count_after_write, 0,
            "non-admin invite must not install private DP grants"
        );

        // An admin invite for aliceCo/sub/ SHOULD install both grants.
        let admin_invite: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($prefix: Prefix!, $capability: Capability!) {
                        createInviteLink(
                            catalogPrefix: $prefix
                            capability: $capability
                            singleUse: false
                        ) { token }
                    }"#,
                    "variables": {
                        "prefix": "aliceCo/sub/",
                        "capability": "admin"
                    }
                }),
                Some(&alice_token),
            )
            .await;

        let admin_token = admin_invite["data"]["createInviteLink"]["token"]
            .as_str()
            .unwrap();

        let _: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        redeemInviteLink(token: $token) { catalogPrefix capability }
                    }"#,
                    "variables": { "token": admin_token }
                }),
                Some(&bob_token),
            )
            .await;

        let granted: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT object_role::text FROM role_grants
            WHERE subject_role = 'aliceCo/sub/' AND capability = 'read'
            ORDER BY object_role
            "#,
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            granted,
            vec![
                "ops/dp/private/aliceCo/".to_string(),
                "ops/tasks/private/aliceCo/".to_string(),
            ],
        );

        // Redeeming the admin invite again is idempotent: no duplicates.
        let _: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        redeemInviteLink(token: $token) { catalogPrefix capability }
                    }"#,
                    "variables": { "token": admin_token }
                }),
                Some(&bob_token),
            )
            .await;

        let count_after_second: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) FROM role_grants
            WHERE subject_role = 'aliceCo/sub/' AND capability = 'read'
            "#,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count_after_second, 2);
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

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "sso_tenant"))
    )]
    async fn test_redeem_invite_sso_enforcement(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let alice_token =
            server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), Some("alice@acme.co"));
        let bob_token =
            server.make_access_token(uuid::Uuid::from_bytes([0x22; 16]), Some("bob@other.co"));
        let carol_token = server.make_access_token(
            uuid::Uuid::from_bytes([0x33; 16]),
            Some("carol@example.com"),
        );

        // Alice (matching SSO) creates an invite link for acmeCo/.
        // The response should include ssoProviderId.
        let create_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($prefix: Prefix!, $capability: Capability!) {
                        createInviteLink(
                            catalogPrefix: $prefix
                            capability: $capability
                            singleUse: false
                        ) { token ssoProviderId }
                    }"#,
                    "variables": {
                        "prefix": "acmeCo/",
                        "capability": "write"
                    }
                }),
                Some(&alice_token),
            )
            .await;

        let invite_token = create_response["data"]["createInviteLink"]["token"]
            .as_str()
            .expect("should have token");

        // ssoProviderId should be the acme provider UUID.
        assert_eq!(
            create_response["data"]["createInviteLink"]["ssoProviderId"],
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "invite for SSO tenant should include ssoProviderId"
        );

        // Bob (SSO identity for a different provider) is rejected.
        let bob_redeem: serde_json::Value = server
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

        insta::assert_json_snapshot!("redeem_sso_wrong_provider", bob_redeem);

        // Carol (no SSO identity) is rejected.
        let carol_redeem: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        redeemInviteLink(token: $token) { catalogPrefix capability }
                    }"#,
                    "variables": { "token": invite_token }
                }),
                Some(&carol_token),
            )
            .await;

        insta::assert_json_snapshot!("redeem_sso_no_identity", carol_redeem);

        // Alice (matching SSO identity) succeeds.
        let alice_redeem: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        redeemInviteLink(token: $token) { catalogPrefix capability }
                    }"#,
                    "variables": { "token": invite_token }
                }),
                Some(&alice_token),
            )
            .await;

        insta::assert_json_snapshot!("redeem_sso_matching", alice_redeem);

        // Create an invite on openCo/ (no SSO) — Bob can redeem it.
        // Insert directly since Bob lacks admin on openCo.
        let open_token: uuid::Uuid = sqlx::query_scalar(
            "INSERT INTO internal.invite_links (catalog_prefix, capability, single_use) \
             VALUES ('openCo/', 'read', false) RETURNING token",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let bob_open_redeem: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        redeemInviteLink(token: $token) { catalogPrefix capability }
                    }"#,
                    "variables": { "token": open_token }
                }),
                Some(&bob_token),
            )
            .await;

        insta::assert_json_snapshot!("redeem_no_sso_enforcement", bob_open_redeem);

        // Sub-prefix invite: acmeCo/production/ should still be covered by
        // the SSO enforcement on tenant acmeCo/.
        let sub_prefix_token: uuid::Uuid = sqlx::query_scalar(
            "INSERT INTO internal.invite_links (catalog_prefix, capability, single_use) \
             VALUES ('acmeCo/production/', 'read', false) RETURNING token",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        // Bob (wrong SSO provider) is rejected for the sub-prefix too.
        let bob_sub_prefix: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        redeemInviteLink(token: $token) { catalogPrefix capability }
                    }"#,
                    "variables": { "token": sub_prefix_token }
                }),
                Some(&bob_token),
            )
            .await;

        insta::assert_json_snapshot!("redeem_sso_sub_prefix_rejected", bob_sub_prefix);

        // Alice (matching SSO) succeeds for the sub-prefix.
        let alice_sub_prefix: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        redeemInviteLink(token: $token) { catalogPrefix capability }
                    }"#,
                    "variables": { "token": sub_prefix_token }
                }),
                Some(&alice_token),
            )
            .await;

        insta::assert_json_snapshot!("redeem_sso_sub_prefix_allowed", alice_sub_prefix);

        // Verify createInviteLink for a non-SSO tenant returns null ssoProviderId.
        // Alice already has admin on openCo/ via fixture.
        let open_create: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($prefix: Prefix!, $capability: Capability!) {
                        createInviteLink(
                            catalogPrefix: $prefix
                            capability: $capability
                        ) { token ssoProviderId }
                    }"#,
                    "variables": {
                        "prefix": "openCo/",
                        "capability": "read"
                    }
                }),
                Some(&alice_token),
            )
            .await;

        assert!(
            open_create["data"]["createInviteLink"]["ssoProviderId"].is_null(),
            "invite for non-SSO tenant should have null ssoProviderId"
        );

        // createInviteLink for a sub-prefix under an SSO tenant should still
        // return the tenant's ssoProviderId (the tenant lookup strips to the
        // root prefix before the first '/').
        let sub_create: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($prefix: Prefix!, $capability: Capability!) {
                        createInviteLink(
                            catalogPrefix: $prefix
                            capability: $capability
                        ) { token ssoProviderId }
                    }"#,
                    "variables": {
                        "prefix": "acmeCo/production/",
                        "capability": "read"
                    }
                }),
                Some(&alice_token),
            )
            .await;

        assert_eq!(
            sub_create["data"]["createInviteLink"]["ssoProviderId"],
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "invite for sub-prefix under SSO tenant should include ssoProviderId"
        );

        // Single-use invite on SSO tenant: rejection should NOT consume the
        // invite. Create a single-use invite, have a non-SSO user attempt to
        // redeem it (rejected), then verify the matching SSO user can still
        // redeem it successfully.
        let single_use_create: serde_json::Value = server
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
                        "prefix": "acmeCo/",
                        "capability": "read"
                    }
                }),
                Some(&alice_token),
            )
            .await;

        let single_use_token = single_use_create["data"]["createInviteLink"]["token"]
            .as_str()
            .expect("should have token");

        // Carol (no SSO identity) is rejected — invite should survive.
        let carol_single_use: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        redeemInviteLink(token: $token) { catalogPrefix capability }
                    }"#,
                    "variables": { "token": single_use_token }
                }),
                Some(&carol_token),
            )
            .await;

        assert!(
            carol_single_use["errors"].is_array(),
            "non-SSO user should be rejected for SSO tenant single-use invite"
        );

        // Alice (matching SSO) can still redeem the single-use invite,
        // proving the earlier rejection did not consume it.
        let alice_single_use: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation($token: UUID!) {
                        redeemInviteLink(token: $token) { catalogPrefix capability }
                    }"#,
                    "variables": { "token": single_use_token }
                }),
                Some(&alice_token),
            )
            .await;

        assert!(
            alice_single_use["data"]["redeemInviteLink"]["catalogPrefix"]
                .as_str()
                .is_some(),
            "matching SSO user should redeem single-use invite after prior SSO rejection"
        );
    }
}
