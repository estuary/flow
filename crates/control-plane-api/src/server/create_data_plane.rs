use crate::publications::{
    DoNotRetry, DraftPublication, NoopInitialize, NoopWithCommit, PruneUnboundCollections,
};
use crate::directives::storage_mappings::{fetch_storage_mappings, upsert_storage_mapping};
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
    let models::authorizations::ControlClaims { sub: user_id, .. } = env.claims()?;
    super::authorize_ops_admin(&env).await?;

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
        // A one-shot handler invocation, with no queued row to anchor on.
        started_at: None,
        snapshot: env.snapshot(),
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

impl Validate for Category {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        if let Self::Manual(manual) = &self {
            manual.validate()
        } else {
            Ok(())
        }
    }
}

/// The `ops/`-admin pre-check, evaluated against the request's pinned
/// Snapshot: a caller without `ops/` admin is rejected before any data-plane
/// state is touched — terminally (403) when the Snapshot postdates the
/// request, and with the platform-standard 307 retry when it doesn't.
#[cfg(test)]
mod test {
    use crate::test_server;

    // From `fixtures/alice.sql`: admin of `aliceCo/` and nothing else.
    const ALICE: uuid::Uuid = uuid::uuid!("11111111-1111-1111-1111-111111111111");

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_create_data_plane_denied_for_non_ops_admin(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let token = server.make_access_token(ALICE, Some("alice@example.com"));

        let response = server
            .rest_client()
            .post(
                "/admin/create-data-plane",
                &serde_json::json!({"name": "test-plane-c1", "category": "managed"}),
                Some(&token),
            )
            .send()
            .await
            .unwrap();

        assert_eq!(reqwest::StatusCode::FORBIDDEN, response.status());
    }

    /// A denial evaluated against a Snapshot which predates the request is
    /// provisional: the endpoint answers with the platform-standard 307
    /// `AuthZRetry` (Retry-After + `started`/`retryAfter` params) rather than
    /// a terminal 403, and a retry against the refreshed (authoritative)
    /// Snapshot then resolves the denial terminally.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_create_data_plane_stale_snapshot_retries_then_denies(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        // gate=true serves an empty epoch-taken Snapshot first: every denial
        // under it is provisional. The revoke cancelled by the first request
        // refreshes the watch to the real (+1h) Snapshot.
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;
        let token = server.make_access_token(ALICE, Some("alice@example.com"));

        let client = flow_client_next::rest::Client {
            base_url: server.base_url(),
            http_client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
        };
        let body = serde_json::json!({"name": "test-plane-c1", "category": "managed"});

        let response = client
            .post("/admin/create-data-plane", &body, Some(&token))
            .send()
            .await
            .unwrap();
        assert_eq!(
            reqwest::StatusCode::TEMPORARY_REDIRECT,
            response.status(),
            "a stale denial must be provisional"
        );
        assert!(
            response
                .headers()
                .contains_key(reqwest::header::RETRY_AFTER)
        );
        let location = response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|l| l.to_str().ok())
            .expect("redirect carries a Location");
        assert!(
            location.contains("started=") && location.contains("retryAfter="),
            "Location must carry retry bookkeeping: {location}"
        );

        // The cancelled revoke triggers a refresh to the authoritative
        // Snapshot; denials then become terminal. Bound the wait, since the
        // refresh races this retry loop.
        for attempt in 0..50 {
            let response = client
                .post("/admin/create-data-plane", &body, Some(&token))
                .send()
                .await
                .unwrap();
            match response.status() {
                reqwest::StatusCode::FORBIDDEN => return,
                reqwest::StatusCode::TEMPORARY_REDIRECT => {
                    tokio::time::sleep(std::time::Duration::from_millis(20 * attempt)).await;
                }
                other => panic!("unexpected interim status {other}"),
            }
        }
        panic!("denial never became terminal under the refreshed snapshot");
    }

    /// The seeded system user — these endpoints' routine caller — holds a
    /// direct `('ops/', 'admin')` row in `user_grants` (seed.sql); this pins
    /// that exactly that grant shape resolves through the Snapshot's grant
    /// walk, and that it doesn't leak into unrelated tenants.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_ops_admin_authorizes_via_snapshot(pool: sqlx::PgPool) {
        let ops_admin = uuid::uuid!("99999999-9999-9999-9999-999999999999");
        sqlx::query("insert into auth.users (id, email) values ($1, 'ops-admin@example.com')")
            .bind(ops_admin)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "insert into user_grants (user_id, object_role, capability) values ($1, 'ops/', 'admin')",
        )
        .bind(ops_admin)
        .execute(&pool)
        .await
        .unwrap();

        let mut decrypted_hmac_keys = std::collections::HashMap::new();
        let data = crate::snapshot::try_fetch(&pool, &mut decrypted_hmac_keys)
            .await
            .expect("failed to fetch snapshot");
        let snapshot = crate::Snapshot::new(tokens::now(), data);

        let claims = models::authorizations::ControlClaims {
            iat: 0,
            exp: u64::MAX,
            sub: ops_admin,
            role: "authenticated".to_string(),
            aud: "authenticated".to_string(),
            email: Some("ops-admin@example.com".to_string()),
        };
        assert!(
            crate::evaluate_names_authorization(
                &snapshot,
                &claims,
                models::Capability::Admin,
                ["ops/"],
            )
            .is_ok(),
            "an ops/ admin user_grant must satisfy the snapshot walk"
        );
        assert!(
            crate::evaluate_names_authorization(
                &snapshot,
                &claims,
                models::Capability::Admin,
                ["aliceCo/"],
            )
            .is_err(),
            "ops/ admin must not leak into unrelated tenants"
        );
    }
}
