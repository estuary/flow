use async_graphql::Context;
use proto_gazette::broker;
use std::collections::HashMap;
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

/// Input for testing storage health.
#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct TestStorageHealthInput {
    /// The storage definition to test, containing stores and data planes.
    pub storage: async_graphql::Json<models::StorageDef>,
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

/// Resolve data plane names to data plane records, checking authorization.
/// Returns a map from data plane name to the resolved data plane (or None if not found/unauthorized).
async fn resolve_data_planes(
    app: &App,
    claims: &ControlClaims,
    data_plane_names: &[String],
) -> async_graphql::Result<HashMap<String, Option<tables::DataPlane>>> {
    let all_data_planes = crate::data_plane::fetch_all_data_planes(&app.pg_pool).await?;

    let resolved: HashMap<String, Option<tables::DataPlane>> = app
        .attach_user_capabilities(
            claims,
            data_plane_names.iter().cloned(),
            |name, cap| {
                let data_plane = if cap.is_some() {
                    all_data_planes
                        .iter()
                        .find(|dp| dp.data_plane_name == name)
                        .cloned()
                } else {
                    None
                };
                Some((name, data_plane))
            },
        )
        .into_iter()
        .collect();

    Ok(resolved)
}

/// Check storage health for a single data plane + store combination.
async fn check_store_health(
    client: Result<gazette::journal::Client, String>,
    data_plane_name: String,
    fragment_store: String,
) -> StorageHealthResult {
    let error = match client {
        Ok(client) => {
            match client
                .fragment_store_health(broker::FragmentStoreHealthRequest {
                    fragment_store: fragment_store.clone(),
                })
                .await
            {
                Ok(resp) if resp.store_health_error.is_empty() => None,
                Ok(resp) => Some(resp.store_health_error),
                Err(err) => Some(err.to_string()),
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

#[derive(Debug, Default)]
pub struct StorageMappingsMutation;

#[async_graphql::Object]
impl StorageMappingsMutation {
    /// Test whether data planes can access the specified storage buckets.
    ///
    /// This sends FragmentStoreHealth RPCs to each specified data plane's broker
    /// to verify that the data plane's service account has access to the storage.
    ///
    /// Note: Custom (S3-compatible) stores are not supported and will be skipped.
    pub async fn test_storage_health(
        &self,
        ctx: &Context<'_>,
        input: TestStorageHealthInput,
    ) -> async_graphql::Result<Vec<StorageHealthResult>> {
        let claims = ctx.data::<ControlClaims>()?;
        let app = ctx.data::<Arc<App>>()?;

        let storage = &input.storage.0;
        validate_storage_def(storage)?;
        let fragment_stores = extract_fragment_stores(storage)?;

        // Resolve data planes, checking authorization
        let resolved = resolve_data_planes(app, claims, &storage.data_planes).await?;

        // Build clients and spawn health checks in parallel
        let handles: Vec<_> = resolved
            .iter()
            .flat_map(|(name, maybe_dp)| {
                let client = maybe_dp.as_ref().map(|dp| {
                    crate::data_plane::build_journal_client(dp, app.hmac_keys())
                        .map_err(|e| e.to_string())
                });

                fragment_stores.iter().map(move |store| {
                    let data_plane_name = name.clone();
                    let store = store.clone();
                    let client = client.clone();
                    tokio::spawn(async move {
                        match client {
                            Some(c) => check_store_health(c, data_plane_name, store).await,
                            None => StorageHealthResult {
                                data_plane_name,
                                fragment_store: store,
                                error: Some("Data plane not found".to_string()),
                            },
                        }
                    })
                })
            })
            .collect();

        // Collect results
        let mut results = Vec::with_capacity(handles.len());
        for result in futures::future::join_all(handles).await {
            results.push(result.unwrap_or_else(|err| StorageHealthResult {
                data_plane_name: String::new(),
                fragment_store: String::new(),
                error: Some(format!("Health check task failed: {err}")),
            }));
        }

        Ok(results)
    }
}
