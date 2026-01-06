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

/// Input for testing storage health.
#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct TestStorageHealthInput {
    /// The storage definition to test, containing stores and data planes.
    pub storage: async_graphql::Json<models::StorageDef>,
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
        let data_plane_names = &storage.data_planes;

        if data_plane_names.is_empty() {
            return Err(async_graphql::Error::new(
                "storage.data_planes must not be empty",
            ));
        }
        if storage.stores.is_empty() {
            return Err(async_graphql::Error::new(
                "storage.stores must not be empty",
            ));
        }

        // Convert stores to fragment store URLs, skipping Custom stores
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

        // Verify user has read access to the requested data planes
        app.verify_user_authorization_graphql(
            claims,
            None,
            data_plane_names.clone(),
            models::Capability::Read,
        )
        .await?;

        // Fetch data planes to test against
        let all_data_planes = crate::data_plane::fetch_all_data_planes(&app.pg_pool).await?;

        let data_planes: Vec<_> = all_data_planes
            .into_iter()
            .filter(|dp| data_plane_names.contains(&dp.data_plane_name))
            .collect();

        if data_planes.is_empty() {
            return Err(async_graphql::Error::new("No matching data planes found"));
        }

        // Build clients for each data plane, converting errors to strings for cloning
        let clients: Vec<_> = data_planes
            .iter()
            .map(|dp| {
                let client = crate::data_plane::build_journal_client(dp, app.hmac_keys())
                    .map_err(|e| e.to_string());
                (dp.data_plane_name.clone(), client)
            })
            .collect();

        // Build futures for all data plane + store combinations
        let futures: Vec<_> = clients
            .iter()
            .flat_map(|(data_plane_name, client_result)| {
                fragment_stores.iter().map(move |store| {
                    let data_plane_name = data_plane_name.clone();
                    let store = store.clone();
                    let client_result = client_result.clone();

                    async move {
                        match client_result {
                            Ok(client) => {
                                let result = client
                                    .fragment_store_health(broker::FragmentStoreHealthRequest {
                                        fragment_store: store.clone(),
                                    })
                                    .await;

                                let error = match result {
                                    Ok(resp) => {
                                        if resp.store_health_error.is_empty() {
                                            None
                                        } else {
                                            Some(resp.store_health_error)
                                        }
                                    }
                                    Err(err) => Some(format!("{err}")),
                                };

                                StorageHealthResult {
                                    data_plane_name,
                                    fragment_store: store,
                                    error,
                                }
                            }
                            Err(ref err) => StorageHealthResult {
                                data_plane_name,
                                fragment_store: store,
                                error: Some(format!("Failed to create client: {err}")),
                            },
                        }
                    }
                })
            })
            .collect();

        let results = futures::future::join_all(futures).await;

        Ok(results)
    }
}
