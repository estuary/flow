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
    pub fragment_stores: Vec<String>,
    /// Data plane names to test against.
    pub data_plane_names: Vec<String>,
}

#[derive(Debug, Default)]
pub struct StorageHealthMutation;

#[async_graphql::Object]
impl StorageHealthMutation {
    /// Test whether data planes can access the specified storage buckets.
    ///
    /// This sends FragmentStoreHealth RPCs to each specified data plane's broker
    /// to verify that the data plane's service account has access to the storage.
    pub async fn test_storage_health(
        &self,
        ctx: &Context<'_>,
        input: TestStorageHealthInput,
    ) -> async_graphql::Result<Vec<StorageHealthResult>> {
        let _claims = ctx.data::<ControlClaims>()?;
        let app = ctx.data::<Arc<App>>()?;

        // Fetch data planes to test against
        let all_data_planes = crate::data_plane::fetch_all_data_planes(&app.pg_pool).await?;

        let data_planes: Vec<_> = all_data_planes
            .into_iter()
            .filter(|dp| input.data_plane_names.contains(&dp.data_plane_name))
            .collect();

        if data_planes.is_empty() {
            return Err(async_graphql::Error::new("No data planes found"));
        }

        let mut results = Vec::new();

        // For each data plane and store combination, test the health
        for data_plane in &data_planes {
            let client = match crate::data_plane::build_journal_client(data_plane, app.hmac_keys())
            {
                Ok(client) => client,
                Err(err) => {
                    // If we can't build a client for this data plane, report errors for all stores
                    for store in &input.fragment_stores {
                        results.push(StorageHealthResult {
                            data_plane_name: data_plane.data_plane_name.clone(),
                            fragment_store: store.clone(),
                            success: false,
                            error: Some(format!("Failed to create client: {err}")),
                        });
                    }
                    continue;
                }
            };

            for store in &input.fragment_stores {
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

                results.push(StorageHealthResult {
                    data_plane_name: data_plane.data_plane_name.clone(),
                    fragment_store: store.clone(),
                    success,
                    error,
                });
            }
        }

        Ok(results)
    }
}
