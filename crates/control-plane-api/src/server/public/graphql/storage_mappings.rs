use crate::directives::storage_mappings::{
    insert_storage_mapping, split_collection_and_recovery_storage, strip_collection_data_suffix,
    update_storage_mapping, upsert_storage_mapping,
};
use async_graphql::{
    Context,
    types::connection::{self, Connection},
};
use proto_gazette::broker;
use validator::Validate;
/// Result of testing storage health for a single data plane and store.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct StorageHealthItem {
    /// Name of the data plane that was checked.
    data_plane_name: String,
    /// The fragment store that was checked.
    fragment_store: async_graphql::Json<models::Store>,
    /// Error message if the health check failed, or null if it passed.
    error: Option<String>,
}

/// Result of checking storage health for a catalog prefix.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct ConnectionHealthTestResult {
    /// The catalog prefix for which storage health was checked.
    pub catalog_prefix: models::Prefix,
    /// Individual health check results for each data plane and store combination.
    pub results: Vec<StorageHealthItem>,
}

#[async_graphql::ComplexObject]
impl ConnectionHealthTestResult {
    /// Whether all health checks passed.
    pub async fn all_passed(&self) -> bool {
        self.results.iter().all(|c| c.error.is_none())
    }
}

/// Result of creating a storage mapping.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct CreateStorageMappingResult {
    /// The catalog prefix for which the storage mapping was created.
    pub catalog_prefix: models::Prefix,
}

/// Result of updating a storage mapping.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct UpdateStorageMappingResult {
    /// The catalog prefix for which the storage mapping was updated.
    pub catalog_prefix: models::Prefix,
    /// Whether a republish is required because the primary storage bucket changed.
    pub republish: bool,
}

fn validate_inputs(
    catalog_prefix: &models::Prefix,
    storage: &models::StorageDef,
) -> async_graphql::Result<()> {
    if let Err(err) = catalog_prefix.validate() {
        return Err(async_graphql::Error::new(format!(
            "invalid catalog prefix: {err}"
        )));
    }

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
    Ok(())
}

const HEALTH_CHECK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const DEFAULT_PAGE_SIZE: usize = 50;

/// Check storage health for a single data plane + store combination.
async fn check_store_health(
    client: gazette::journal::Client,
    fragment_store: url::Url,
) -> Option<String> {
    let fut = client.fragment_store_health(broker::FragmentStoreHealthRequest {
        fragment_store: fragment_store.to_string(),
    });

    match tokio::time::timeout(HEALTH_CHECK_TIMEOUT, fut).await {
        Ok(Ok(resp)) if resp.store_health_error.is_empty() => None,
        Ok(Ok(resp)) => Some(resp.store_health_error),
        Ok(Err(err)) => Some(err.to_string()),
        Err(_) => Some("Health check timed out".to_string()),
    }
}

/// Run storage health checks for each data plane + store combination.
async fn run_all_health_checks(
    catalog_prefix: &models::Prefix,
    data_planes: &[&tables::DataPlane],
    fragment_stores: &[models::Store],
) -> Vec<StorageHealthItem> {
    let mut results = Vec::new();
    let mut handles = Vec::new();

    for dp in data_planes {
        let client = match crate::data_plane::build_journal_client(dp) {
            Ok(client) => client,
            Err(err) => {
                for store in fragment_stores {
                    results.push(StorageHealthItem {
                        data_plane_name: dp.data_plane_name.clone(),
                        fragment_store: async_graphql::Json(store.clone()),
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
            let catalog_prefix = catalog_prefix.clone();
            let handle = tokio::spawn({
                let fragment_store = fragment_store.clone();
                async move { check_store_health(client, fragment_store.to_url(&catalog_prefix)).await }
            });
            handles.push((handle, data_plane_name, fragment_store));
        }
    }

    for (handle, data_plane_name, fragment_store) in handles {
        let error = handle
            .await
            .unwrap_or_else(|err| Some(format!("Task join error: {}", err)));
        results.push(StorageHealthItem {
            data_plane_name,
            fragment_store: async_graphql::Json(fragment_store),
            error,
        });
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
        catalog_prefix: models::Prefix,
        detail: Option<String>,
        storage: async_graphql::Json<models::StorageDef>,
    ) -> async_graphql::Result<CreateStorageMappingResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;
        let snapshot = env.snapshot();
        let async_graphql::Json(storage) = storage;

        // Do basic input validation checks first.
        validate_inputs(&catalog_prefix, &storage)?;

        // Verify user has admin capability to the catalog prefix and read capability to named data planes.
        evaluate_authorization(env, claims, &catalog_prefix, &storage.data_planes).await?;

        let data_planes = resolve_data_planes(&snapshot, &storage.data_planes)?;

        // Run health checks.
        let health_checks =
            run_all_health_checks(&catalog_prefix, &data_planes, &storage.stores).await;
        let all_passed = health_checks.iter().all(|c| c.error.is_none());

        if !all_passed {
            return Err(async_graphql::Error::new("Storage health checks failed"));
        }

        let mut txn = env.pg_pool.begin().await?;

        // Check if any existing tasks or collections would be affected by this new storage mapping.
        // We disallow creating storage mappings that would change the storage for existing specs.
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

        // A single conceptual "storage mapping" is (today) stored as two
        // distinct rows. They must align, and this alignment is enforced
        // by the `validations` crate.
        let (collection_storage, recovery_storage) = split_collection_and_recovery_storage(storage);

        // Insert collection storage mapping (fails if already exists).
        let inserted = insert_storage_mapping(
            detail.as_deref(),
            catalog_prefix.as_str(),
            &collection_storage,
            &mut *txn,
        )
        .await?;

        if !inserted {
            return Err(async_graphql::Error::new(format!(
                "A storage mapping already exists for catalog prefix '{catalog_prefix}'"
            )));
        }

        // using upsert here to simplify recovery mapping update/insert
        // which we'll eventually remove when we stop storing recovery mappings separately
        upsert_storage_mapping(
            detail.as_deref(),
            &format!("recovery/{catalog_prefix}"),
            &recovery_storage,
            &mut txn,
        )
        .await?;

        txn.commit().await?;

        tracing::info!(
            %catalog_prefix,
            data_planes = ?collection_storage.data_planes,
            stores_count = ?collection_storage.stores.len(),
            "created storage mapping"
        );

        Ok(CreateStorageMappingResult { catalog_prefix })
    }

    /// Update an existing storage mapping for the given catalog prefix.
    ///
    /// This validates that the user has admin access to the catalog prefix,
    /// runs health checks to verify that data planes can access the storage buckets,
    /// and then updates the storage mapping in the database.
    ///
    /// Health checks for newly added stores or data planes must pass before the
    /// storage mapping is updated. Health check failures for existing stores/data planes
    /// are allowed (they were already validated when created).
    pub async fn update_storage_mapping(
        &self,
        ctx: &Context<'_>,
        catalog_prefix: models::Prefix,
        detail: Option<String>,
        storage: async_graphql::Json<models::StorageDef>,
    ) -> async_graphql::Result<UpdateStorageMappingResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;
        let snapshot = env.snapshot();
        let async_graphql::Json(storage) = storage;

        // Do basic input validation checks first.
        validate_inputs(&catalog_prefix, &storage)?;

        // Verify user has admin capability to the catalog prefix and read capability to named data planes.
        evaluate_authorization(env, claims, &catalog_prefix, &storage.data_planes).await?;

        let data_planes = resolve_data_planes(&snapshot, &storage.data_planes)?;

        // Run health checks outside of transaction so as not to keep rows locked too long.
        let health_checks =
            run_all_health_checks(&catalog_prefix, &data_planes, &storage.stores).await;

        // Begin a transaction to fetch existing mapping and update.
        let mut txn = env.pg_pool.begin().await?;

        // Fetch existing storage mapping to compare stores and verify it exists.
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

        let current = match current {
            Some(spec) => spec,
            None => {
                return Err(async_graphql::Error::new(format!(
                    "No storage mapping exists for catalog prefix '{catalog_prefix}'"
                )));
            }
        };

        // Determine if republish is needed: stores added or removed.
        let republish = storage.stores != current.0.stores;

        // Check if any health check failed for a newly added store or data plane.
        let has_new_failures = health_checks.iter().any(|c| {
            if c.error.is_none() {
                return false;
            }
            let is_new_store = !current.0.stores.contains(&c.fragment_store.0);
            let is_new_dp = !current.0.data_planes.contains(&c.data_plane_name);
            is_new_store || is_new_dp
        });

        if has_new_failures {
            // We only fail on health check errors for newly added stores or data planes.
            // Tasks under this storage mapping will still be broken if there are any failing
            // health checks, but we allow the update so long as the user isn't adding more
            // problems than there already were.
            return Err(async_graphql::Error::new(
                "Storage health checks failed for newly added stores or data planes",
            ));
        }

        // A single conceptual "storage mapping" is (today) stored as two
        // distinct rows. They must align, and this alignment is enforced
        // by the `validations` crate.
        let (collection_storage, recovery_storage) = split_collection_and_recovery_storage(storage);

        let updated = update_storage_mapping(
            detail.as_deref(),
            catalog_prefix.as_str(),
            &collection_storage,
            &mut *txn,
        )
        .await?;

        if !updated {
            return Err(async_graphql::Error::new(format!(
                "No storage mapping exists for catalog prefix '{catalog_prefix}'"
            )));
        }

        // using upsert here to simplify recovery mapping update/insert
        // which we'll eventually remove when we stop storing recovery mappings separately
        upsert_storage_mapping(
            detail.as_deref(),
            &format!("recovery/{catalog_prefix}"),
            &recovery_storage,
            &mut txn,
        )
        .await?;

        txn.commit().await?;

        tracing::info!(
            %catalog_prefix,
            data_planes = ?collection_storage.data_planes,
            stores_count = ?collection_storage.stores.len(),
            republish,
            "updated storage mapping"
        );

        Ok(UpdateStorageMappingResult {
            catalog_prefix,
            republish,
        })
    }

    /// Check storage health for a given catalog prefix and storage definition.
    ///
    /// This validates the inputs, verifies that the user has admin access to the catalog prefix,
    /// and runs health checks to verify that data planes can access the storage buckets.
    ///
    /// Unlike create/update mutations, this does not modify any data and always returns
    /// health check results (both successes and failures) rather than erroring on failures.
    pub async fn test_connection_health(
        &self,
        ctx: &Context<'_>,
        catalog_prefix: models::Prefix,
        storage: async_graphql::Json<models::StorageDef>,
    ) -> async_graphql::Result<ConnectionHealthTestResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;
        let snapshot = env.snapshot();
        let async_graphql::Json(storage) = storage;

        // Do basic input validation checks first.
        validate_inputs(&catalog_prefix, &storage)?;

        // Verify user has admin capability to the catalog prefix and read capability to named data planes.
        evaluate_authorization(env, claims, &catalog_prefix, &storage.data_planes).await?;

        let data_planes = resolve_data_planes(&snapshot, &storage.data_planes)?;

        // Run health checks and collect results.
        let results = run_all_health_checks(&catalog_prefix, &data_planes, &storage.stores).await;

        Ok(ConnectionHealthTestResult {
            catalog_prefix,
            results,
        })
    }
}

async fn evaluate_authorization(
    env: &crate::Envelope,
    claims: &crate::ControlClaims,
    catalog_prefix: &models::Prefix,
    data_plane_names: &[String],
) -> Result<(), crate::ApiError> {
    let policy_result =
        check_authorization(&env.snapshot(), claims, catalog_prefix, data_plane_names);
    env.authorization_outcome(policy_result).await?;
    Ok(())
}

fn check_authorization(
    snapshot: &crate::Snapshot,
    claims: &crate::ControlClaims,
    catalog_prefix: &models::Prefix,
    data_plane_names: &[String],
) -> crate::AuthZResult<()> {
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

    for data_plane_name in data_plane_names {
        // Verify `catalog_prefix` is authorized to access the data-plane for Read.
        if !tables::RoleGrant::is_authorized(
            &snapshot.role_grants,
            catalog_prefix,
            data_plane_name,
            models::Capability::Read,
        ) {
            return Err(tonic::Status::permission_denied(format!(
                "'{catalog_prefix}' is not authorized to data plane '{data_plane_name}' for Read",
            )));
        }
    }

    Ok((None, ()))
}

fn resolve_data_planes<'s>(
    snapshot: &'s crate::Snapshot,
    data_plane_names: &[String],
) -> Result<Vec<&'s tables::DataPlane>, async_graphql::Error> {
    data_plane_names
        .iter()
        .map(|name| {
            snapshot.data_plane_by_catalog_name(name).ok_or_else(|| {
                async_graphql::Error::new(format!("data plane {name} was not found"))
            })
        })
        .collect()
}

// ============================================================================
// Query types and implementation
// ============================================================================

/// Input type for querying storage mappings.
#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct StorageMappingsBy {
    /// Fetch storage mappings by exact catalog prefixes.
    /// At least one of `exactPrefixes` or `underPrefix` must be provided.
    pub exact_prefixes: Option<Vec<models::Prefix>>,
    /// Fetch all storage mappings under this prefix pattern.
    /// For example, "acmeCo/" returns mappings for "acmeCo/", "acmeCo/team-a/", etc.
    /// At least one of `exactPrefixes` or `underPrefix` must be provided.
    pub under_prefix: Option<models::Prefix>,
}

/// A storage mapping that defines where collection data is stored.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct StorageMapping {
    /// The catalog prefix this storage mapping applies to.
    pub catalog_prefix: models::Prefix,
    /// Optional description of this storage mapping.
    pub detail: Option<String>,
    /// The storage definition containing stores and data plane assignments.
    pub storage: async_graphql::Json<models::StorageDef>,
    /// The current user's capability to this storage mapping's prefix.
    pub user_capability: models::Capability,
}

pub type PaginatedStorageMappings = Connection<
    String,
    StorageMapping,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

#[derive(Debug, Default)]
pub struct StorageMappingsQuery;

#[async_graphql::Object]
impl StorageMappingsQuery {
    /// Returns storage mappings accessible to the current user.
    ///
    /// Requires at least read capability to the queried prefixes.
    /// Results are paginated and sorted by catalog_prefix.
    pub async fn storage_mappings(
        &self,
        ctx: &Context<'_>,
        by: StorageMappingsBy,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> async_graphql::Result<PaginatedStorageMappings> {
        let env = ctx.data::<crate::Envelope>()?;

        let StorageMappingsBy {
            exact_prefixes,
            under_prefix,
        } = by;
        let exact_prefixes = exact_prefixes.unwrap_or_default();

        // Validate that exactly one of the two input options is provided.
        match (exact_prefixes.is_empty(), under_prefix.is_none()) {
            (true, true) => {
                return Err("must provide exactly one of `exactPrefixes` or `underPrefix`".into());
            }
            (false, false) => {
                return Err(
                    "`exactPrefixes` and `underPrefix` are mutually exclusive; provide only one"
                        .into(),
                );
            }
            _ => {}
        }

        // Verify user has read capability to the queried prefixes.
        let claims = env.claims()?;
        let user_email = claims.email.as_deref().unwrap_or("user");

        let prefixes_to_check: Vec<&models::Prefix> = if let Some(ref prefix) = under_prefix {
            vec![prefix]
        } else {
            exact_prefixes.iter().collect()
        };

        for prefix in prefixes_to_check {
            let policy_result = if tables::UserGrant::is_authorized(
                &env.snapshot().role_grants,
                &env.snapshot().user_grants,
                claims.sub,
                prefix,
                models::Capability::Read,
            ) {
                Ok((None, ()))
            } else {
                Err(tonic::Status::permission_denied(format!(
                    "{user_email} is not authorized to read catalog prefix '{prefix}'",
                )))
            };
            env.authorization_outcome(policy_result).await?;
        }

        let (rows, has_prev, has_next) =
            connection::query_with::<String, _, _, _, async_graphql::Error>(
                after,
                before,
                first,
                last,
                |after, before, first, last| async move {
                    let limit = first.or(last).unwrap_or(DEFAULT_PAGE_SIZE);
                    if limit == 0 {
                        return Ok((Vec::new(), false, false));
                    }

                    let result = if before.is_some() || last.is_some() {
                        let rows = fetch_storage_mappings_before(
                            &env.pg_pool,
                            &exact_prefixes,
                            under_prefix.as_ref(),
                            before.as_deref(),
                            limit as i64,
                        )
                        .await
                        .map_err(async_graphql::Error::from)?;
                        let has_prev = rows.len() == limit;
                        (rows, has_prev, before.is_some())
                    } else {
                        let rows = fetch_storage_mappings_after(
                            &env.pg_pool,
                            &exact_prefixes,
                            under_prefix.as_ref(),
                            after.as_deref(),
                            limit as i64,
                        )
                        .await
                        .map_err(async_graphql::Error::from)?;
                        let has_next = rows.len() == limit;
                        (rows, after.is_some(), has_next)
                    };

                    async_graphql::Result::Ok(result)
                },
            )
            .await?;

        let snapshot = env.snapshot();
        let claims = env.claims()?;
        let edges = rows
            .into_iter()
            .map(|row| {
                let user_capability = tables::UserGrant::get_user_capability(
                    &snapshot.role_grants,
                    &snapshot.user_grants,
                    claims.sub,
                    &row.catalog_prefix,
                )
                .ok_or_else(|| {
                    async_graphql::Error::new(format!(
                        "missing capability for catalog prefix '{}'",
                        row.catalog_prefix
                    ))
                })?;

                // Strip "collection-data/" suffix from store prefixes before returning to user.
                let user_facing_storage = strip_collection_data_suffix(row.spec);

                Ok(connection::Edge::new(
                    row.catalog_prefix.clone(),
                    StorageMapping {
                        catalog_prefix: models::Prefix::new(row.catalog_prefix),
                        detail: row.detail,
                        storage: async_graphql::Json(user_facing_storage),
                        user_capability,
                    },
                ))
            })
            .collect::<Result<Vec<_>, async_graphql::Error>>()?;

        let mut conn = PaginatedStorageMappings::new(has_prev, has_next);
        conn.edges = edges;
        Ok(conn)
    }
}

struct StorageMappingRow {
    catalog_prefix: String,
    detail: Option<String>,
    spec: models::StorageDef,
}

async fn fetch_storage_mappings_after(
    db: &sqlx::PgPool,
    exact_prefixes: &[models::Prefix],
    under_prefix: Option<&models::Prefix>,
    after: Option<&str>,
    limit: i64,
) -> anyhow::Result<Vec<StorageMappingRow>> {
    // Convert to plain strings to avoid sqlx encoding as `_catalog_name` domain array,
    // which would trigger domain constraint validation on bind.
    let exact_prefixes: Vec<String> = exact_prefixes.iter().map(|p| p.to_string()).collect();
    let under_prefix = under_prefix.map(|p| p.as_str());

    let rows = sqlx::query!(
        r#"
        SELECT
            catalog_prefix as "catalog_prefix!: String",
            detail,
            spec as "spec!: crate::TextJson<models::StorageDef>"
        FROM storage_mappings
        WHERE NOT starts_with(catalog_prefix, 'recovery/')
        AND (
            catalog_prefix::text = any($1::text[])
            OR ($2::text IS NOT NULL AND starts_with(catalog_prefix, $2::text))
        )
        AND ($3::text IS NULL OR catalog_prefix > $3::text)
        ORDER BY catalog_prefix ASC
        LIMIT $4
        "#,
        &exact_prefixes,
        under_prefix,
        after,
        limit,
    )
    .fetch_all(db)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| StorageMappingRow {
            catalog_prefix: r.catalog_prefix,
            detail: r.detail,
            spec: r.spec.0,
        })
        .collect())
}

async fn fetch_storage_mappings_before(
    db: &sqlx::PgPool,
    exact_prefixes: &[models::Prefix],
    under_prefix: Option<&models::Prefix>,
    before: Option<&str>,
    limit: i64,
) -> anyhow::Result<Vec<StorageMappingRow>> {
    // TODO: Greg is skeptical about this _catalog_name workaround - investigate further.

    // Convert to plain strings to avoid sqlx encoding as `_catalog_name` domain array,
    // which would trigger domain constraint validation on bind.
    let exact_prefixes: Vec<String> = exact_prefixes.iter().map(|p| p.to_string()).collect();
    let under_prefix = under_prefix.map(|p| p.as_str());

    let mut rows = sqlx::query!(
        r#"
        SELECT
            catalog_prefix as "catalog_prefix!: String",
            detail,
            spec as "spec!: crate::TextJson<models::StorageDef>"
        FROM storage_mappings
        WHERE NOT starts_with(catalog_prefix, 'recovery/')
        AND (
            catalog_prefix::text = any($1::text[])
            OR ($2::text IS NOT NULL AND starts_with(catalog_prefix, $2::text))
        )
        AND ($3::text IS NULL OR catalog_prefix < $3::text)
        ORDER BY catalog_prefix DESC
        LIMIT $4
        "#,
        &exact_prefixes,
        under_prefix,
        before,
        limit,
    )
    .fetch_all(db)
    .await?;

    // Reverse to maintain ascending order per Relay spec.
    rows.reverse();

    Ok(rows
        .into_iter()
        .map(|r| StorageMappingRow {
            catalog_prefix: r.catalog_prefix,
            detail: r.detail,
            spec: r.spec.0,
        })
        .collect())
}
