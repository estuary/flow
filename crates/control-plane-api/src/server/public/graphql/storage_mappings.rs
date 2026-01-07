use async_graphql::Context;
use proto_gazette::broker;
use std::sync::Arc;

use crate::server::{App, ControlClaims};

/// Result of testing storage health for a single data plane and store.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct StorageHealthResult {
    /// Name of the data plane that was tested.
    pub data_plane_name: String,
    /// The fragment store URL that was tested (e.g., "gs://bucket/prefix").
    pub fragment_store: String,
    /// Error message if the health check failed.
    pub error: Option<String>,
}

#[async_graphql::ComplexObject]
impl StorageHealthResult {
    /// Whether the health check succeeded.
    async fn success(&self) -> bool {
        self.error.is_none()
    }
}

/// Input for creating a storage mapping.
#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct CreateStorageMappingInput {
    /// The catalog prefix for which to create the storage mapping.
    pub catalog_prefix: String,
    /// Optional description of the storage mapping.
    pub detail: Option<String>,
    /// The storage definition containing stores and data planes.
    pub storage: async_graphql::Json<models::StorageDef>,
    /// If true, only run validation and health checks without saving.
    #[graphql(default)]
    pub dry_run: bool,
}

/// Validate a StorageDef for required fields.
fn validate_storage_def(storage: &models::StorageDef) -> async_graphql::Result<()> {
    if storage.data_planes.is_empty() {
        return Err(async_graphql::Error::new(
            "storage.data_planes must not be empty",
        ));
    }
    if storage.stores.is_empty() {
        return Err(async_graphql::Error::new(
            "storage.stores must not be empty",
        ));
    }
    Ok(())
}

/// Extract fragment store URLs from a StorageDef.
/// Custom (S3-compatible) stores are skipped as they're not supported for health checks.
fn extract_fragment_stores(storage: &models::StorageDef) -> async_graphql::Result<Vec<String>> {
    let fragment_stores: Vec<String> = storage
        .stores
        .iter()
        .filter_map(|store| match store {
            models::Store::Custom(_) => None,
            _ => Some(store.to_url("").to_string()),
        })
        .collect();

    if fragment_stores.is_empty() {
        return Err(async_graphql::Error::new(
            "No supported stores found (Custom stores are not supported)",
        ));
    }

    Ok(fragment_stores)
}

/// Resolved data planes and any that were not found or unauthorized.
struct ResolvedDataPlanes {
    found: Vec<tables::DataPlane>,
    missing: Vec<String>,
}

/// Resolve data plane names to data plane records, checking authorization.
async fn resolve_data_planes(
    app: &App,
    claims: &ControlClaims,
    data_plane_names: &[String],
) -> async_graphql::Result<ResolvedDataPlanes> {
    let all_data_planes = crate::data_plane::fetch_all_data_planes(&app.pg_pool).await?;

    let mut found = Vec::new();
    let mut missing = Vec::new();

    for name in data_plane_names {
        let cap = app
            .attach_user_capabilities(claims, std::iter::once(name.clone()), |n, c| Some((n, c)));

        let has_cap = cap.first().map(|(_, c)| c.is_some()).unwrap_or(false);

        if has_cap {
            if let Some(dp) = all_data_planes
                .iter()
                .find(|dp| &dp.data_plane_name == name)
            {
                found.push(dp.clone());
            } else {
                missing.push(name.clone());
            }
        } else {
            missing.push(name.clone());
        }
    }

    Ok(ResolvedDataPlanes { found, missing })
}

const HEALTH_CHECK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Check storage health for a single data plane + store combination.
async fn check_store_health(
    client: Result<gazette::journal::Client, String>,
    data_plane_name: String,
    fragment_store: String,
) -> StorageHealthResult {
    let error = match client {
        Ok(client) => {
            let fut = client.fragment_store_health(broker::FragmentStoreHealthRequest {
                fragment_store: fragment_store.clone(),
            });

            match tokio::time::timeout(HEALTH_CHECK_TIMEOUT, fut).await {
                Ok(Ok(resp)) if resp.store_health_error.is_empty() => None,
                Ok(Ok(resp)) => Some(resp.store_health_error),
                Ok(Err(err)) => Some(err.to_string()),
                Err(_) => Some("Health check timed out".to_string()),
            }
        }
        Err(err) => Some(format!("Failed to create client: {err}")),
    };

    StorageHealthResult {
        data_plane_name,
        fragment_store,
        error,
    }
}

/// Result of creating a storage mapping.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct CreateStorageMappingResult {
    /// Whether the storage mapping was created successfully.
    pub success: bool,
    /// The catalog prefix for which the storage mapping was created.
    pub catalog_prefix: String,
    /// Results of health checks for each data plane and store combination.
    pub health_checks: Vec<StorageHealthResult>,
}

/// Run storage health checks for each data plane + store combination.
async fn run_storage_health_checks(
    app: &App,
    data_planes: &[tables::DataPlane],
    fragment_stores: &[String],
) -> Vec<StorageHealthResult> {
    let handles: Vec<_> = data_planes
        .iter()
        .flat_map(|dp| {
            let client = crate::data_plane::build_journal_client(dp, app.hmac_keys())
                .map_err(|e| e.to_string());

            fragment_stores.iter().map(move |store| {
                let data_plane_name = dp.data_plane_name.clone();
                let fragment_store = store.clone();
                let client = client.clone();
                let handle = tokio::spawn({
                    let data_plane_name = data_plane_name.clone();
                    let fragment_store = fragment_store.clone();
                    async move {
                        check_store_health(client, data_plane_name, fragment_store).await
                    }
                });
                (data_plane_name, fragment_store, handle)
            })
        })
        .collect();

    let mut results = Vec::with_capacity(handles.len());
    for (data_plane_name, fragment_store, handle) in handles {
        results.push(handle.await.unwrap_or_else(|err| StorageHealthResult {
            data_plane_name,
            fragment_store,
            error: Some(format!("Health check task failed: {err}")),
        }));
    }

    results
}

#[derive(Debug, Default)]
pub struct StorageMappingsMutation;

#[async_graphql::Object]
impl StorageMappingsMutation {
    /// Create a storage mapping for the given catalog prefix.
    ///
    /// This validates that the user has admin access to the catalog prefix,
    /// runs health checks to verify that data planes can access the storage buckets,
    /// and then saves the storage mapping to the database.
    ///
    /// All health checks must pass before the storage mapping is created.
    /// Custom (S3-compatible) stores are not supported for health checks and will be skipped.
    pub async fn create_storage_mapping(
        &self,
        ctx: &Context<'_>,
        input: CreateStorageMappingInput,
    ) -> async_graphql::Result<CreateStorageMappingResult> {
        let claims = ctx.data::<ControlClaims>()?;
        let app = ctx.data::<Arc<App>>()?;

        // Verify user has admin capability to the catalog prefix
        app.verify_user_authorization_graphql(
            claims,
            None,
            vec![input.catalog_prefix.clone()],
            models::Capability::Admin,
        )
        .await?;

        let storage = &input.storage.0;
        validate_storage_def(storage)?;
        let fragment_stores = extract_fragment_stores(storage)?;

        // Resolve data planes, checking authorization
        let ResolvedDataPlanes {
            found: data_planes,
            missing,
        } = resolve_data_planes(app, claims, &storage.data_planes).await?;

        // Run health checks
        let mut health_checks =
            run_storage_health_checks(app, &data_planes, &fragment_stores).await;

        // Add error results for missing data planes
        for name in &missing {
            for store in &fragment_stores {
                health_checks.push(StorageHealthResult {
                    data_plane_name: name.clone(),
                    fragment_store: store.clone(),
                    error: Some("Data plane not found".to_string()),
                });
            }
        }

        // Check if any health checks failed
        let has_health_failures = health_checks.iter().any(|r| r.error.is_some());

        // Determine if we can save the mapping
        let can_save = !has_health_failures && !input.dry_run;

        if can_save {
            let mut txn = app.pg_pool.begin().await?;

            let detail = input.detail.as_deref().unwrap_or("created via GraphQL API");

            crate::directives::storage_mappings::upsert_storage_mapping(
                detail,
                &input.catalog_prefix,
                storage,
                &mut txn,
            )
            .await?;

            txn.commit().await?;

            tracing::info!(
                catalog_prefix = %input.catalog_prefix,
                data_planes = ?storage.data_planes,
                stores_count = storage.stores.len(),
                "created storage mapping"
            );
        }

        Ok(CreateStorageMappingResult {
            success: can_save,
            catalog_prefix: input.catalog_prefix,
            health_checks,
        })
    }
}
