use crate::directives::storage_mappings::{fetch_storage_mappings, upsert_storage_mapping};
use crate::publications::{
    DoNotRetry, DraftPublication, NoopInitialize, NoopWithCommit, PruneUnboundCollections,
};
use anyhow::Context;
use validator::Validate;

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Category {
    Managed,
    Manual(Manual),
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct Manual {
    /// Address of brokers in this data-plane.
    #[validate(url)]
    broker_address: String,
    /// Address of reactors in this data-plane.
    #[validate(url)]
    reactor_address: String,
    /// HMAC keys of the data-plane.
    hmac_keys: Vec<String>,
    /// Kafka-protocol address of Dekaf in this data-plane (tls:// URL).
    #[serde(default)]
    dekaf_address: Option<String>,
    /// Schema registry HTTP address of Dekaf in this data-plane (https:// URL).
    #[serde(default)]
    dekaf_registry_address: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    /// Base name of this data-plane, such as "gcp-us-central1-c1".
    #[validate(nested)]
    name: models::Token,

    /// Private tenant to which this data-plane is provisioned,
    /// or if None the data-plane is public.
    #[validate(nested)]
    #[serde(default)]
    private: Option<models::Prefix>,

    #[validate(nested)]
    category: Category,
}

#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Response {}

#[axum::debug_handler(state=std::sync::Arc<crate::App>)]
#[tracing::instrument(skip(app, env), ret, err(Debug, level = tracing::Level::WARN))]
pub async fn create_data_plane(
    axum::extract::State(app): axum::extract::State<std::sync::Arc<crate::App>>,
    env: crate::Envelope,
    super::Request(Request {
        name,
        private,
        category,
    }): super::Request<Request>,
) -> Result<axum::Json<Response>, crate::server::error::ApiError> {
    let claims = env.claims()?;
    let user_id = &claims.sub;
    let user_email = claims.email.as_deref().unwrap_or("user");

    // Authorize against the request's Snapshot (not a fresh watch token) so that
    // `authorization_outcome` can refresh-and-retry a denial that was decided
    // from a Snapshot older than the request.
    let policy_result = evaluate_ops_admin_authorization(env.snapshot(), *user_id, user_email);
    let (_expiry, ()) = env.authorization_outcome(policy_result).await?;

    let (data_plane_fqdn, base_name, pulumi_stack) = match &private {
        None => (
            format!("{name}.dp.estuary-data.com"), // 'aws-eu-west-1-c1.dp.estuary-data.com'
            format!("public/{name}"),              // 'public/aws-eu-west-1-c1'
            format!("public-{name}"),              // 'public-aws-eu-west-1-c1'
        ),
        Some(prefix) => {
            let base_name = format!("private/{prefix}{name}");
            (
                // '9e571ae54b74e18.dp.estuary-data.com'
                format!(
                    "{:x}.dp.estuary-data.com",
                    xxhash_rust::xxh3::xxh3_64(base_name.as_bytes()),
                ),
                // 'private/AcmeCo/aws-eu-west-1-c1'
                base_name,
                // 'private-AcmeCo-aws-eu-west-2-c3'
                format!("private-{}-{name}", prefix.trim_end_matches("/")),
            )
        }
    };
    std::mem::drop(name); // Use `base_name` only.

    let data_plane_name = format!("ops/dp/{base_name}");

    if super::public::graphql::parse_data_plane_name(&data_plane_name).is_none() {
        return Err(tonic::Status::invalid_argument(format!(
            "data plane name '{data_plane_name}' does not match the expected format (e.g., 'ops/dp/public/aws-us-east-1-c1')",
        ))
        .into());
    }

    let ops_l1_inferred_name = format!("ops/rollups/L1/{base_name}/inferred-schemas");
    let ops_l1_stats_name = format!("ops/rollups/L1/{base_name}/catalog-stats");
    let ops_l1_events_name = format!("ops/rollups/L1/{base_name}/events");
    let ops_l2_inferred_transform = format!("from.{data_plane_fqdn}");
    let ops_l2_stats_transform = format!("from.{data_plane_fqdn}");
    let ops_l2_events_transform = format!("from.{data_plane_fqdn}");
    let ops_logs_name = format!("ops/tasks/{base_name}/logs");
    let ops_stats_name = format!("ops/tasks/{base_name}/stats");

    let (broker_address, reactor_address, dekaf_address, dekaf_registry_address, hmac_keys) =
        match category {
            Category::Managed => (
                format!("https://gazette.{data_plane_fqdn}"),
                format!("https://reactor.{data_plane_fqdn}"),
                // dekaf_address and dekaf_registry_address are set by the
                // data-plane-controller when Dekaf is actually deployed.
                None,
                None,
                Vec::new(),
            ),
            Category::Manual(Manual {
                broker_address,
                reactor_address,
                hmac_keys,
                dekaf_address,
                dekaf_registry_address,
            }) => (
                broker_address,
                reactor_address,
                dekaf_address,
                dekaf_registry_address,
                hmac_keys,
            ),
        };

    // Grant a private tenant access to their data-plane and task logs & stats.
    // These grants are always safe to create for every tenant, but we only
    // bother to do it for tenants which are actively creating private data-planes.
    if let Some(prefix) = &private {
        // The `ops/dp/private/<tenant>/` grant delegates the
        // `ManageDataPlane` bundle. Legacy `read` stays in `capability`
        // for RLS / `user_roles()` access. The `ops/tasks/private/` grant
        // is strictly for log/stats visibility and stays plain `read`.
        sqlx::query!(
            r#"
            insert into role_grants (subject_role, object_role, capability, bundles, detail) values
                ($1::text, 'ops/dp/private/' || $1, 'read', $2::capability_bundle[], 'private data-plane'),
                ($1::text, 'ops/tasks/private/' || $1, 'read', $3::capability_bundle[], 'private data-plane')
            on conflict do nothing
            "#,
            &prefix as &str,
            &[models::authz::CapabilityBundle::ManageDataPlane]
                as &[models::authz::CapabilityBundle],
            &[] as &[models::authz::CapabilityBundle],
        )
        .execute(&env.pg_pool)
        .await?;
    }

    let insert = sqlx::query!(
        r#"
        insert into data_planes (
            data_plane_name,
            data_plane_fqdn,
            ops_logs_name,
            ops_stats_name,
            ops_l1_inferred_name,
            ops_l1_stats_name,
            ops_l1_events_name,
            ops_l2_inferred_transform,
            ops_l2_stats_transform,
            ops_l2_events_transform,
            broker_address,
            reactor_address,
            dekaf_address,
            dekaf_registry_address,
            hmac_keys,
            enable_l2,
            pulumi_stack
        ) values (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
        )
        on conflict (data_plane_name) do update set
            broker_address = $11,
            reactor_address = $12,
            dekaf_address = coalesce($13, data_planes.dekaf_address),
            dekaf_registry_address = coalesce($14, data_planes.dekaf_registry_address),
            -- Don't replace non-empty hmac_keys with empty ones.
            hmac_keys = case when array_length($15, 1) > 0 then $15
                        else data_planes.hmac_keys end
        returning logs_token
        ;
        "#,
        &data_plane_name as &String,
        data_plane_fqdn,
        &ops_logs_name as &String,
        &ops_stats_name as &String,
        &ops_l1_inferred_name as &String,
        &ops_l1_stats_name as &String,
        &ops_l1_events_name as &String,
        &ops_l2_inferred_transform,
        &ops_l2_stats_transform,
        &ops_l2_events_transform,
        broker_address,
        reactor_address,
        &dekaf_address as &Option<String>,
        &dekaf_registry_address as &Option<String>,
        &hmac_keys,
        !hmac_keys.is_empty(), // Enable L2 if HMAC keys are defined at creation.
        pulumi_stack,
    )
    .fetch_one(&env.pg_pool)
    .await?;

    // Install ops logs and stats collections, as well as L1 roll-ups.
    // These may fail to activate if the data-plane is still being provisioned.
    let draft_str = include_str!("../../../../ops-catalog/data-plane-template.bundle.json")
        .replace("BASE_NAME", &base_name);
    let draft: tables::DraftCatalog = serde_json::from_str::<models::Catalog>(&draft_str)
        .unwrap()
        .into();

    let publication = DraftPublication {
        user_id: *user_id,
        logs_token: insert.logs_token,
        draft,
        dry_run: false,
        detail: Some(format!("publication for data-plane {base_name}")),
        // We've already validated that the user can admin `ops/`,
        // so further authZ checks are unnecessary.
        verify_user_authz: false,
        default_data_plane_name: Some(data_plane_name.clone()),
        initialize: NoopInitialize,
        finalize: PruneUnboundCollections,
        retry: DoNotRetry,
        with_commit: NoopWithCommit,
    };
    let result = app
        .publisher
        .publish(publication)
        .await
        .context("publishing ops catalog")?;

    for err in result.draft_errors() {
        tracing::error!(error = ?err, "create-data-plane build error");
    }
    let _result = result.error_for_status()?;

    // Update storage mappings for private data planes to add the new data plane as the first option
    if let Some(tenant_prefix) = &private {
        let mut txn = app.pg_pool.begin().await?;
        let recovery_prefix = format!("recovery/{}", tenant_prefix);

        // Fetch existing storage mappings for this tenant
        let existing_mappings =
            fetch_storage_mappings(tenant_prefix, &recovery_prefix, &mut txn).await?;

        for mapping in existing_mappings {
            if mapping.catalog_prefix.starts_with("recovery/") {
                continue;
            }
            // Parse the existing spec
            let mut storage_spec: models::StorageDef = serde_json::from_str(mapping.spec.get())
                .context("deserializing existing storage mapping")?;

            // Add the new data plane to the front of the data_planes list
            // Remove it first if it already exists to avoid duplicates
            storage_spec.data_planes.retain(|dp| dp != &data_plane_name);
            storage_spec.data_planes.insert(0, data_plane_name.clone());

            // Update the storage mapping
            let detail = format!("updated by create-data-plane for {}", data_plane_name);
            upsert_storage_mapping(
                Some(&detail),
                &mapping.catalog_prefix,
                &storage_spec,
                &mut txn,
            )
            .await?;

            tracing::info!(
                tenant_prefix = %tenant_prefix,
                data_plane_name = %data_plane_name,
                "updated storage mapping to prioritize new data plane"
            );
        }

        txn.commit().await?;
    }

    tracing::info!(
        data_plane_fqdn,
        data_plane_name,
        ops_l1_inferred_name,
        ops_l1_stats_name,
        ops_logs_name,
        ops_stats_name,
        broker_address,
        reactor_address,
        ?dekaf_address,
        ?dekaf_registry_address,
        "data-plane created"
    );

    Ok(axum::Json(Response {}))
}

/// Builds a policy result (for `Envelope::authorization_outcome`) asserting that
/// `user_id` holds `admin` capability resolving to exactly the `ops/` tenant,
/// which is required to create a data-plane.
///
/// This replaces the prior
/// `internal.user_roles($user, 'admin') where role_prefix = 'ops/'` SQL check.
/// Because that check was an *exact* `ops/` match — a data-plane always lives
/// under `ops/` — this looks for `admin` at precisely the `ops/` prefix, and
/// deliberately does not accept a grant at an ancestor or a descendant of it.
///
/// It evaluates against a caller-provided `Snapshot` (the request's Snapshot via
/// `Envelope::snapshot`) rather than re-reading the watch, so that
/// `authorization_outcome` can refresh-and-retry a denial decided from a stale
/// Snapshot.
pub(super) fn evaluate_ops_admin_authorization(
    snapshot: &crate::Snapshot,
    user_id: uuid::Uuid,
    user_email: &str,
) -> crate::AuthZResult<()> {
    let is_ops_admin = snapshot
        .prefix_and_capabilities_per_user(user_id)
        .get("ops/")
        .is_some_and(|(_bits, legacy)| *legacy >= models::Capability::Admin);

    if is_ops_admin {
        Ok((None, ()))
    } else {
        Err(tonic::Status::permission_denied(format!(
            "{user_email} is not an admin of the 'ops/' tenant",
        )))
    }
}

impl Validate for Category {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        if let Self::Manual(manual) = &self {
            manual.validate()
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod test {
    use super::evaluate_ops_admin_authorization;

    // Inserts `user_id` along with the given `user_grants`
    // (as `(object_role, capability)`) and `role_grants` (as
    // `(subject_role, object_role, capability)`), builds an authorization
    // Snapshot from that database state, then evaluates the `ops/` admin check
    // for that user. Running through a real Snapshot built from real grants
    // proves the snapshot-based check preserves the prior SQL behavior across
    // every scenario below.
    async fn eval(
        pool: &sqlx::PgPool,
        user_id: uuid::Uuid,
        user_grants: &[(&str, &str)],
        role_grants: &[(&str, &str, &str)],
    ) -> bool {
        sqlx::query("insert into auth.users (id, email) values ($1, $2)")
            .bind(user_id)
            .bind(format!("{user_id}@example.com"))
            .execute(pool)
            .await
            .unwrap();

        for (object_role, capability) in user_grants {
            sqlx::query(
                "insert into user_grants (user_id, object_role, capability)
                 values ($1, $2, $3::grant_capability)",
            )
            .bind(user_id)
            .bind(object_role)
            .bind(capability)
            .execute(pool)
            .await
            .unwrap();
        }
        for (subject_role, object_role, capability) in role_grants {
            sqlx::query(
                "insert into role_grants (subject_role, object_role, capability)
                 values ($1, $2, $3::grant_capability)",
            )
            .bind(subject_role)
            .bind(object_role)
            .bind(capability)
            .execute(pool)
            .await
            .unwrap();
        }

        // `gate: false` so the first (and only) refresh serves the real
        // snapshot built from the grants inserted above, rather than the empty
        // snapshot used to exercise the server's retry flow.
        let snapshot_watch = crate::test_server::snapshot(pool.clone(), false).await;
        let refresh = snapshot_watch.token();
        let snapshot = refresh.result().unwrap();
        evaluate_ops_admin_authorization(snapshot, user_id, "test@example.com").is_ok()
    }

    // A user granted `admin` directly on `ops/` is authorized.
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn admin_directly_on_ops_is_authorized(pool: sqlx::PgPool) {
        let user_id = uuid::Uuid::from_bytes([1; 16]);
        assert!(eval(&pool, user_id, &[("ops/", "admin")], &[]).await);
    }

    // A user reaching `admin` on `ops/` transitively through a role grant
    // (e.g. an `estuary_support/` role that is itself granted admin over `ops/`)
    // is authorized. This exercises the recursive expansion in `user_roles`.
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn admin_on_ops_via_role_grant_is_authorized(pool: sqlx::PgPool) {
        let user_id = uuid::Uuid::from_bytes([2; 16]);
        assert!(
            eval(
                &pool,
                user_id,
                &[("estuary_support/", "admin")],
                &[("estuary_support/", "ops/", "admin")],
            )
            .await
        );
    }

    // Admin over a *sub-prefix* of `ops/` (e.g. `ops/dp/`) does NOT authorize:
    // the check is an exact `role_prefix = 'ops/'` match, not a prefix match.
    // This deliberately differs from the storage-mappings sub-prefix behavior,
    // and pins it so a later snapshot refactor cannot silently loosen it.
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn admin_on_sub_prefix_of_ops_is_denied(pool: sqlx::PgPool) {
        let user_id = uuid::Uuid::from_bytes([3; 16]);
        assert!(!eval(&pool, user_id, &[("ops/dp/", "admin")], &[]).await);
    }

    // A capability below `admin` on `ops/` is not enough.
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn write_on_ops_is_denied(pool: sqlx::PgPool) {
        let user_id = uuid::Uuid::from_bytes([4; 16]);
        assert!(!eval(&pool, user_id, &[("ops/", "write")], &[]).await);
    }

    // Admin over an unrelated tenant confers no authority over `ops/`.
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn admin_on_unrelated_tenant_is_denied(pool: sqlx::PgPool) {
        let user_id = uuid::Uuid::from_bytes([5; 16]);
        assert!(!eval(&pool, user_id, &[("aliceCo/", "admin")], &[]).await);
    }

    // A user with no grants at all is denied.
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn no_grants_is_denied(pool: sqlx::PgPool) {
        let user_id = uuid::Uuid::from_bytes([6; 16]);
        assert!(!eval(&pool, user_id, &[], &[]).await);
    }
}
