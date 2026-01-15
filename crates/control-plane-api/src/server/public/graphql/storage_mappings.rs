use async_graphql::{Context, ErrorExtensions};
use proto_gazette::broker;
use validator::Validate;

/// Result of testing storage health for a single data plane and store.
#[derive(Debug, Clone)]
struct StorageHealthResult {
    data_plane_name: String,
    fragment_store: url::Url,
    error: Option<String>,
}

/// Result of creating or updating a storage mapping.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct UpsertStorageMappingResult {
    /// Whether the storage mapping was created (false if dry_run was true, or it previously existed).
    pub created: bool,
    /// The catalog prefix for which the storage mapping was created or updated.
    pub catalog_prefix: models::Prefix,
}

const HEALTH_CHECK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Check storage health for a single data plane + store combination.
async fn check_store_health(
    client: gazette::journal::Client,
    data_plane_name: String,
    fragment_store: url::Url,
) -> StorageHealthResult {
    let fut = client.fragment_store_health(broker::FragmentStoreHealthRequest {
        fragment_store: fragment_store.to_string(),
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

/// Run storage health checks for each data plane + store combination.
async fn run_storage_health_checks(
    data_planes: &[&tables::DataPlane],
    fragment_stores: &[url::Url],
) -> Vec<StorageHealthResult> {
    let mut results = Vec::new();
    let mut handles = Vec::new();

    for dp in data_planes {
        let client = match crate::data_plane::build_journal_client(dp) {
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
    /// Create or update a storage mapping for the given catalog prefix.
    ///
    /// This validates that the user has admin access to the catalog prefix,
    /// runs health checks to verify that data planes can access the storage buckets,
    /// and then saves the storage mapping to the database if `dry_run` is false.
    ///
    /// All health checks must pass before the storage mapping is created.
    pub async fn upsert_storage_mapping(
        &self,
        ctx: &Context<'_>,
        catalog_prefix: models::Prefix,
        detail: Option<String>,
        storage: async_graphql::Json<models::StorageDef>,
        dry_run: bool,
    ) -> async_graphql::Result<UpsertStorageMappingResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;
        let snapshot = env.snapshot();

        // Do basic input validation checks first.
        if let Err(err) = catalog_prefix.validate() {
            return Err(async_graphql::Error::new(format!(
                "invalid catalog prefix: {err}"
            )));
        }

        let async_graphql::Json(storage) = storage;
        if let Err(err) = storage.validate() {
            return Err(async_graphql::Error::new(format!(
                "invalid storage definition: {err}"
            )));
        }
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

        // Verify user has admin capability to the catalog prefix and read capability to named data planes.
        let policy_result =
            evaluate_authorization(&snapshot, claims, &catalog_prefix, &storage.data_planes);

        let (_expiry, data_planes) = env.authorization_outcome(policy_result).await?;

        let fragment_stores: Vec<url::Url> = storage
            .stores
            .iter()
            .map(|store| store.to_url(&catalog_prefix))
            .collect();

        // Run health checks, then map to (only) failing ones.
        let health_checks = run_storage_health_checks(&data_planes, &fragment_stores).await;

        let health_check_errors: Vec<serde_json::Value> = health_checks
            .into_iter()
            .filter_map(|r| match r.error {
                Some(err) => Some(serde_json::json!({
                    "dataPlane": r.data_plane_name,
                    "fragmentStore": r.fragment_store,
                    "error": err,
                })),
                None => None,
            })
            .collect();

        // Bail now if any health checks failed.
        if !health_check_errors.is_empty() {
            let errors_value: async_graphql::Value =
                serde_json::from_value(serde_json::Value::Array(health_check_errors))
                    .expect("valid JSON");

            return Err(
                async_graphql::Error::new("Storage health checks failed").extend_with(|_, ext| {
                    ext.set("healthCheckErrors", errors_value.clone());
                }),
            );
        }

        // Begin a transaction to check for conflicts and upsert the storage mapping.
        let mut txn = env.pg_pool.begin().await?;

        let current = sqlx::query_scalar!(
            r#"
            SELECT spec AS "spec: crate::TextJson<models::StorageDef>"
            FROM storage_mappings
            WHERE catalog_prefix = $1
            FOR UPDATE OF storage_mappings
            "#,
            &catalog_prefix,
        )
        .fetch_optional(&mut *txn)
        .await?;

        // Check if any existing tasks or collections would be affected by this new storage mapping.
        // We disallow creating storage mappings that would change the storage for existing specs.
        if current.is_none() {
            let sampled_specs = sqlx::query_scalar!(
                r#"
                SELECT catalog_name
                FROM live_specs
                WHERE starts_with(catalog_name, $1)
                AND spec IS NOT NULL
                LIMIT 5
                "#,
                &catalog_prefix,
            )
            .fetch_all(&mut *txn)
            .await?;

            if !sampled_specs.is_empty() {
                return Err(async_graphql::Error::new(format!(
                    "Cannot create storage mapping for '{catalog_prefix}': existing specs would be affected: {}",
                    sampled_specs.join(", "),
                )));
            }
        };

        // A single conceptual "storage mapping" is (today) stored as two
        // distinct rows. They must align, and this alignment is enforced
        // by the `validations` crate.
        //
        // Build separate collection vs recovery StorageDefs from `storage`.
        let models::StorageDef {
            data_planes,
            stores,
        } = storage;

        let collection_storage = models::StorageDef {
            data_planes,
            stores: stores
                .iter()
                .cloned()
                .map(|mut store| {
                    let prefix = store.prefix_mut();
                    *prefix = models::Prefix::new(format!("{prefix}collection-data/"));
                    store
                })
                .collect(),
        };
        let recovery_storage = models::StorageDef {
            data_planes: Vec::new(),
            stores,
        };

        if !dry_run {
            sqlx::query!(
                r#"
                INSERT INTO storage_mappings (catalog_prefix, spec, detail)
                VALUES ($1, $2, $3)
                ON CONFLICT (catalog_prefix) DO UPDATE SET
                    spec = $2,
                    detail = $3,
                    updated_at = now()
                "#,
                &catalog_prefix as &str,
                crate::TextJson(&collection_storage) as crate::TextJson<&models::StorageDef>,
                detail,
            )
            .execute(&mut *txn)
            .await?;

            sqlx::query!(
                r#"
                INSERT INTO storage_mappings (catalog_prefix, spec, detail)
                VALUES ('recovery/' || $1, $2, $3)
                ON CONFLICT (catalog_prefix) DO UPDATE SET
                    spec = $2,
                    detail = $3,
                    updated_at = now()
                "#,
                &catalog_prefix as &str,
                crate::TextJson(&recovery_storage) as crate::TextJson<&models::StorageDef>,
                detail,
            )
            .execute(&mut *txn)
            .await?;

            tracing::info!(
                %catalog_prefix,
                data_planes = ?collection_storage.data_planes,
                stores_count = ?collection_storage.stores.len(),
                "created storage mapping"
            );
        }

        Ok(UpsertStorageMappingResult {
            created: current.is_none(),
            catalog_prefix,
        })
    }
}

fn evaluate_authorization<'s>(
    snapshot: &'s crate::Snapshot,
    claims: &crate::ControlClaims,
    catalog_prefix: &models::Prefix,
    storage_data_planes: &[String],
) -> crate::AuthZResult<Vec<&'s tables::DataPlane>> {
    let models::authorizations::ControlClaims {
        sub: user_id,
        email: user_email,
        ..
    } = claims;
    let user_email = user_email.as_ref().map(String::as_str).unwrap_or("user");

    // Verify the User admins `catalog_prefix`.
    if !tables::UserGrant::is_authorized(
        &snapshot.role_grants,
        &snapshot.user_grants,
        *user_id,
        catalog_prefix,
        models::Capability::Admin,
    ) {
        return Err(tonic::Status::permission_denied(format!(
            "{user_email} is not an authorized as an Admin of catalog prefix '{catalog_prefix}'",
        )));
    }

    let mut data_planes = Vec::with_capacity(storage_data_planes.len());

    for data_plane_name in storage_data_planes {
        // Verify `catalog_prefix` is authorized to access the data-plane for Read.
        if !tables::RoleGrant::is_authorized(
            &snapshot.role_grants,
            catalog_prefix,
            data_plane_name,
            models::Capability::Read,
        ) {
            return Err(tonic::Status::permission_denied(format!(
                "'{catalog_prefix}' is not an authorized to a data plane '{data_plane_name}' for Read",
            )));
        }

        let Some(dp) = snapshot.data_plane_by_catalog_name(data_plane_name) else {
            return Err(tonic::Status::not_found(format!(
                "data plane {data_plane_name} was not found"
            )));
        };
        data_planes.push(dp);
    }

    Ok((None, data_planes))
}
