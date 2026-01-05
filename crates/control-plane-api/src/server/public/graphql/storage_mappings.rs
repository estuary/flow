use async_graphql::Context;
use proto_gazette::broker;
use std::sync::Arc;

use crate::server::{App, ControlClaims};

/// Result of testing storage health for a single data plane and store.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct StorageHealthResult {
    /// Name of the data plane that was tested.
    pub data_plane_name: String,
    /// The fragment store URL that was tested (e.g., "gs://bucket/prefix").
    pub fragment_store: String,
    /// Whether the health check succeeded.
    pub success: bool,
    /// Error message if the health check failed.
    pub error: Option<String>,
}

/// Input for testing storage health.
#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct TestStorageHealthInput {
    /// The fragment store URLs to test (e.g., ["gs://bucket/prefix", "s3://bucket/prefix"]).
    #[graphql(validator(min_items = 1))]
    pub fragment_stores: Vec<String>,
    /// Data plane names to test against.
    #[graphql(validator(min_items = 1))]
    pub data_plane_names: Vec<String>,
}

#[derive(Debug, Default)]
pub struct StorageMappingsMutation;

#[async_graphql::Object]
impl StorageMappingsMutation {
    /// Test whether data planes can access the specified storage buckets.
    ///
    /// This sends FragmentStoreHealth RPCs to each specified data plane's broker
    /// to verify that the data plane's service account has access to the storage.
    pub async fn test_storage_health(
        &self,
        ctx: &Context<'_>,
        input: TestStorageHealthInput,
    ) -> async_graphql::Result<Vec<StorageHealthResult>> {
        let claims = ctx.data::<ControlClaims>()?;
        let app = ctx.data::<Arc<App>>()?;

        // Verify user has read access to the requested data planes
        app.verify_user_authorization_graphql(
            claims,
            None,
            input.data_plane_names.clone(),
            models::Capability::Read,
        )
        .await?;

        // Fetch data planes to test against
        let all_data_planes = crate::data_plane::fetch_all_data_planes(&app.pg_pool).await?;

        let data_planes: Vec<_> = all_data_planes
            .into_iter()
            .filter(|dp| input.data_plane_names.contains(&dp.data_plane_name))
            .collect();

        if data_planes.is_empty() {
            return Err(async_graphql::Error::new("No data planes found"));
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
                input.fragment_stores.iter().map(move |store| {
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

                                let (success, error) = match result {
                                    Ok(resp) => {
                                        if resp.store_health_error.is_empty() {
                                            (true, None)
                                        } else {
                                            (false, Some(resp.store_health_error))
                                        }
                                    }
                                    Err(err) => (false, Some(format!("{err}"))),
                                };

                                StorageHealthResult {
                                    data_plane_name,
                                    fragment_store: store,
                                    success,
                                    error,
                                }
                            }
                            Err(ref err) => StorageHealthResult {
                                data_plane_name,
                                fragment_store: store,
                                success: false,
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
