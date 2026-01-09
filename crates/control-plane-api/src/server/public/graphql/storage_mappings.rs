use async_graphql::{Context, ErrorExtensions};
use proto_gazette::broker;
use std::sync::Arc;

use crate::server::{App, ControlClaims, snapshot::Snapshot};

/// Result of testing storage health for a single data plane and store.
#[derive(Debug, Clone)]
struct StorageHealthResult {
    data_plane_name: String,
    fragment_store: String,
    error: Option<String>,
}

/// Input for creating a storage mapping.
#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct CreateStorageMappingInput {
    /// The catalog prefix for which to create the storage mapping (must end with '/').
    pub catalog_prefix: models::Prefix,
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

const HEALTH_CHECK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Check storage health for a single data plane + store combination.
async fn check_store_health(
    client: gazette::journal::Client,
    data_plane_name: String,
    fragment_store: String,
) -> StorageHealthResult {
    let fut = client.fragment_store_health(broker::FragmentStoreHealthRequest {
        fragment_store: fragment_store.clone(),
    });

    let error = match tokio::time::timeout(HEALTH_CHECK_TIMEOUT, fut).await {
        Ok(Ok(resp)) if resp.store_health_error.is_empty() => None,
        Ok(Ok(resp)) => Some(resp.store_health_error),
        Ok(Err(err)) => Some(err.to_string()),
        Err(_) => Some("Health check timed out".to_string()),
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
    /// Whether the storage mapping was created (false if dry_run was true).
    pub created: bool,
    /// The catalog prefix for which the storage mapping was created.
    pub catalog_prefix: String,
}

/// Run storage health checks for each data plane + store combination.
async fn run_storage_health_checks(
    app: &App,
    data_planes: &[tables::DataPlane],
    fragment_stores: &[String],
) -> Vec<StorageHealthResult> {
    let mut results = Vec::new();
    let mut handles = Vec::new();

    for dp in data_planes {
        let client = match crate::data_plane::build_journal_client(dp, app.hmac_keys()) {
            Ok(client) => client,
            Err(err) => {
                for store in fragment_stores {
                    results.push(StorageHealthResult {
                        data_plane_name: dp.data_plane_name.clone(),
                        fragment_store: store.clone(),
                        error: Some(format!("Failed to create client: {err}")),
                    });
                }
                continue;
            }
        };

        for store in fragment_stores {
            let data_plane_name = dp.data_plane_name.clone();
            let fragment_store = store.clone();
            let client = client.clone();
            let handle = tokio::spawn({
                let data_plane_name = data_plane_name.clone();
                let fragment_store = fragment_store.clone();
                async move { check_store_health(client, data_plane_name, fragment_store).await }
            });
            handles.push((data_plane_name, fragment_store, handle));
        }
    }

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
    pub async fn create_storage_mapping(
        &self,
        ctx: &Context<'_>,
        input: CreateStorageMappingInput,
    ) -> async_graphql::Result<CreateStorageMappingResult> {
        let claims = ctx.data::<ControlClaims>()?;
        let app = ctx.data::<Arc<App>>()?;

        // Validate the catalog prefix format
        use validator::Validate;
        if let Err(err) = input.catalog_prefix.validate() {
            return Err(async_graphql::Error::new(format!(
                "Invalid catalog prefix: {err}"
            )));
        }

        // Verify user has admin capability to the catalog prefix
        app.verify_user_authorization_graphql(
            claims,
            None,
            vec![input.catalog_prefix.to_string()],
            models::Capability::Admin,
        )
        .await?;

        // Check if a storage mapping already exists for this prefix
        let existing = sqlx::query_scalar!(
            r#"select true from storage_mappings where catalog_prefix = $1"#,
            &input.catalog_prefix
        )
        .fetch_optional(&app.pg_pool)
        .await?;

        if existing.is_some() {
            return Err(async_graphql::Error::new(format!(
                "A storage mapping already exists for catalog prefix '{}'",
                input.catalog_prefix
            )));
        }

        // Check if any existing tasks or collections would be affected by this new storage mapping.
        // We disallow creating storage mappings that would change the storage for existing specs.
        let prefix_str = input.catalog_prefix.as_str();
        let affected_specs: Vec<String> =
            Snapshot::evaluate(app.snapshot(), chrono::Utc::now(), |snapshot: &Snapshot| {
                let affected_tasks = snapshot
                    .tasks_by_prefix(prefix_str)
                    .map(|t| t.task_name.to_string());
                let affected_collections = snapshot
                    .collections_by_prefix(prefix_str)
                    .map(|c| c.collection_name.to_string());

                let mut all_affected: Vec<String> =
                    affected_tasks.chain(affected_collections).collect();
                all_affected.sort();
                Ok((None, all_affected))
            })
            .expect("evaluation cannot fail")
            .1;

        if !affected_specs.is_empty() {
            let sample: Vec<_> = affected_specs.iter().take(5).cloned().collect();
            let more = if affected_specs.len() > 5 {
                format!(" (and {} more)", affected_specs.len() - 5)
            } else {
                String::new()
            };
            return Err(async_graphql::Error::new(format!(
                "Cannot create storage mapping for '{}': existing specs would be affected: {}{}",
                input.catalog_prefix,
                sample.join(", "),
                more
            )));
        }

        let storage = &input.storage.0;
        validate_storage_def(storage)?;

        let fragment_stores: Vec<String> = storage
            .stores
            .iter()
            .map(|store| store.to_url(&input.catalog_prefix).to_string())
            .collect();

        // Verify user has authorization to all data planes
        app.verify_user_authorization_graphql(
            claims,
            None,
            storage.data_planes.clone(),
            models::Capability::Read,
        )
        .await?;

        // Fetch data plane records from the snapshot and identify any that don't exist
        let (data_planes, missing) =
            Snapshot::evaluate(app.snapshot(), chrono::Utc::now(), |snapshot: &Snapshot| {
                let mut data_planes = Vec::new();
                let mut missing = Vec::new();

                for name in &storage.data_planes {
                    if let Some(dp) = snapshot.data_plane_by_catalog_name(name) {
                        data_planes.push(dp.clone());
                    } else {
                        missing.push(name.clone());
                    }
                }

                Ok((None, (data_planes, missing)))
            })
            .expect("evaluation cannot fail")
            .1;

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

        // Collect only failing health checks
        let failed_checks: Vec<_> = health_checks
            .into_iter()
            .filter(|r| r.error.is_some())
            .collect();

        if !failed_checks.is_empty() {
            let health_check_errors: Vec<serde_json::Value> = failed_checks
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "dataPlane": r.data_plane_name,
                        "fragmentStore": r.fragment_store,
                        "error": r.error.as_deref().unwrap_or("unknown error"),
                    })
                })
                .collect();

            let errors_value: async_graphql::Value =
                serde_json::from_value(serde_json::Value::Array(health_check_errors))
                    .expect("valid JSON");

            return Err(
                async_graphql::Error::new("Storage health checks failed").extend_with(|_, ext| {
                    ext.set("healthCheckErrors", errors_value.clone());
                }),
            );
        }

        if input.dry_run {
            return Ok(CreateStorageMappingResult {
                created: false,
                catalog_prefix: input.catalog_prefix.to_string(),
            });
        }

        let mut txn = app.pg_pool.begin().await?;

        let detail = input.detail.as_deref().unwrap_or("created via GraphQL API");

        crate::directives::storage_mappings::insert_storage_mapping(
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

        Ok(CreateStorageMappingResult {
            created: true,
            catalog_prefix: input.catalog_prefix.to_string(),
        })
    }
}
