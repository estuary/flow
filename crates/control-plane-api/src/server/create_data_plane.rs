use super::App;
use crate::directives::storage_mappings::{fetch_storage_mappings, upsert_storage_mapping};
use crate::publications::{
    DoNotRetry, DraftPublication, NoopInitialize, NoopWithCommit, PruneUnboundCollections,
};
use crate::server::error::ApiErrorExt;
use anyhow::Context;
use axum::http::StatusCode;
use std::sync::Arc;
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

#[axum::debug_handler]
#[tracing::instrument(
    skip(app),
    ret,
    err(level = tracing::Level::WARN),
)]
pub async fn create_data_plane(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Extension(super::ControlClaims { sub: user_id, .. }): axum::Extension<
        super::ControlClaims,
    >,
    super::Request(Request {
        name,
        private,
        category,
    }): super::Request<Request>,
) -> Result<axum::Json<Response>, crate::server::error::ApiError> {
    if let None = sqlx::query!(
        "select role_prefix from internal.user_roles($1, 'admin') where role_prefix = 'ops/'",
        user_id,
    )
    .fetch_optional(&app.pg_pool)
    .await?
    {
        return Err(
            anyhow::anyhow!("authenticated user is not an admin of the 'ops/' tenant")
                .with_status(StatusCode::FORBIDDEN),
        );
    }

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
    let ops_l1_inferred_name = format!("ops/rollups/L1/{base_name}/inferred-schemas");
    let ops_l1_stats_name = format!("ops/rollups/L1/{base_name}/catalog-stats");
    let ops_l1_events_name = format!("ops/rollups/L1/{base_name}/events");
    let ops_l2_inferred_transform = format!("from.{data_plane_fqdn}");
    let ops_l2_stats_transform = format!("from.{data_plane_fqdn}");
    let ops_l2_events_transform = format!("from.{data_plane_fqdn}");
    let ops_logs_name = format!("ops/tasks/{base_name}/logs");
    let ops_stats_name = format!("ops/tasks/{base_name}/stats");

    let (broker_address, reactor_address, hmac_keys) = match category {
        Category::Managed => (
            format!("https://gazette.{data_plane_fqdn}"),
            format!("https://reactor.{data_plane_fqdn}"),
            Vec::new(),
        ),
        Category::Manual(Manual {
            broker_address,
            reactor_address,
            hmac_keys,
        }) => (broker_address, reactor_address, hmac_keys),
    };

    // Grant a private tenant access to their data-plane and task logs & stats.
    // These grants are always safe to create for every tenant, but we only
    // bother to do it for tenants which are actively creating private data-planes.
    if let Some(prefix) = &private {
        sqlx::query!(
            r#"
            insert into role_grants (subject_role, object_role, capability, detail) values
                ($1::text, 'ops/dp/private/' || $1, 'read', 'private data-plane'),
                ($1::text, 'ops/tasks/private/' || $1, 'read', 'private data-plane')
            on conflict do nothing
            "#,
            &prefix as &str,
        )
        .execute(&app.pg_pool)
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
            hmac_keys,
            enable_l2,
            pulumi_stack
        ) values (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
        )
        on conflict (data_plane_name) do update set
            broker_address = $11,
            reactor_address = $12,
            -- Don't replace non-empty hmac_keys with empty ones.
            hmac_keys = case when array_length($13, 1) > 0 then $13
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
        &hmac_keys,
        !hmac_keys.is_empty(), // Enable L2 if HMAC keys are defined at creation.
        pulumi_stack,
    )
    .fetch_one(&app.pg_pool)
    .await?;

    // Install ops logs and stats collections, as well as L1 roll-ups.
    // These may fail to activate if the data-plane is still being provisioned.
    let draft_str = include_str!("../../../../ops-catalog/data-plane-template.bundle.json")
        .replace("BASE_NAME", &base_name);
    let draft: tables::DraftCatalog = serde_json::from_str::<models::Catalog>(&draft_str)
        .unwrap()
        .into();

    let publication = DraftPublication {
        user_id,
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
            upsert_storage_mapping(&detail, &mapping.catalog_prefix, &storage_spec, &mut txn)
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
