use super::filters;
use crate::directives::storage_mappings::{
    collection_and_recovery_spec_from, insert_storage_mapping, update_storage_mapping,
    upsert_storage_mapping,
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
    #[graphql(deprecation = "Use storageMapping.catalogPrefix instead.")]
    pub catalog_prefix: models::Prefix,
    /// The newly created storage mapping.
    pub storage_mapping: StorageMapping,
}

/// Result of updating a storage mapping.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct UpdateStorageMappingResult {
    /// The catalog prefix for which the storage mapping was updated.
    #[graphql(deprecation = "Use storageMapping.catalogPrefix instead.")]
    pub catalog_prefix: models::Prefix,
    /// The updated storage mapping.
    pub storage_mapping: StorageMapping,
    /// Whether a republish is required because the primary storage bucket changed.
    pub republish: bool,
}

fn validate_inputs(
    catalog_prefix: &models::Prefix,
    spec: &models::StorageDef,
) -> async_graphql::Result<()> {
    if let Err(err) = catalog_prefix.validate() {
        return Err(async_graphql::Error::new(format!(
            "invalid catalog prefix: {err}"
        )));
    }

    if let Err(err) = spec.validate() {
        return Err(async_graphql::Error::new(format!(
            "invalid storage definition: {err}"
        )));
    }
    if spec.data_planes.is_empty() {
        return Err(async_graphql::Error::new(
            "spec.data_planes must not be empty",
        ));
    }
    if spec.stores.is_empty() {
        return Err(async_graphql::Error::new("spec.stores must not be empty"));
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
        check_delete: true,
        check_delete_prefix: "recovery/".to_string(),
    });

    match tokio::time::timeout(HEALTH_CHECK_TIMEOUT, fut).await {
        Ok(Ok(resp)) if resp.store_health_error.is_empty() => None,
        Ok(Ok(resp)) => Some(resp.store_health_error),
        // Surface the store's own diagnostic verbatim. The `Display` prefix
        // ("fragment store is unhealthy: ") is redundant in this context, where
        // the caller already knows it's reporting a failed store health check.
        Ok(Err(gazette::Error::FragmentStoreUnhealthy(err))) => Some(err),
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

/// Render failing health checks into a human-readable summary, one store per
/// line. `createStorageMapping`/`updateStorageMapping` return only a flat error
/// string, so — unlike `testConnectionHealth`, whose per-store results already
/// carry the store as a structured field — the store must be named in the text
/// for a caller to know which one failed.
fn describe_health_failures<'a>(
    catalog_prefix: &models::Prefix,
    failures: impl IntoIterator<Item = &'a StorageHealthItem>,
) -> String {
    failures
        .into_iter()
        .map(|item| {
            format!(
                "  - {} on data-plane {}: {}",
                item.fragment_store.0.to_url(catalog_prefix),
                item.data_plane_name,
                item.error.as_deref().unwrap_or("unknown error"),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
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
        spec: async_graphql::Json<models::StorageDef>,
    ) -> async_graphql::Result<CreateStorageMappingResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;
        let snapshot = env.snapshot();
        let async_graphql::Json(spec) = spec;

        // Do basic input validation checks first.
        validate_inputs(&catalog_prefix, &spec)?;

        // Verify user has admin capability to the catalog prefix and read capability to named data planes.
        evaluate_authorization(env, claims, &catalog_prefix, &spec.data_planes).await?;

        let data_planes = resolve_data_planes(&snapshot, &spec.data_planes)?;

        // Run health checks.
        let health_checks =
            run_all_health_checks(&catalog_prefix, &data_planes, &spec.stores).await;
        let failures: Vec<&StorageHealthItem> =
            health_checks.iter().filter(|c| c.error.is_some()).collect();

        if !failures.is_empty() {
            return Err(async_graphql::Error::new(format!(
                "Storage health checks failed:\n{}",
                describe_health_failures(&catalog_prefix, failures),
            )));
        }

        let mut txn = env.pg_pool.begin().await?;

        // Check if a more-specific storage mapping already exists under this prefix.
        let child_mappings = sqlx::query_scalar!(
            r#"
            SELECT catalog_prefix
            FROM storage_mappings
            WHERE catalog_prefix ^@ $1
            AND catalog_prefix != $1
            AND NOT catalog_prefix ^@ 'recovery/'
            "#,
            &catalog_prefix,
        )
        .fetch_all(&mut *txn)
        .await?;

        // Check if any existing tasks or collections would be affected by this new storage mapping.
        // We disallow creating storage mappings that would change the storage for existing specs.
        let sampled_specs = sqlx::query_scalar!(
            r#"
            SELECT catalog_name
            FROM live_specs
            WHERE catalog_name ^@ $1
            AND spec IS NOT NULL
            AND NOT catalog_name ^@ ANY($2::text[])
            LIMIT 5
            "#,
            &catalog_prefix,
            &child_mappings,
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
        let (collection_spec, recovery_spec) = collection_and_recovery_spec_from(spec);

        // Insert collection storage mapping (fails if already exists).
        let inserted = insert_storage_mapping(
            detail.as_deref(),
            catalog_prefix.as_str(),
            &collection_spec,
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
            &recovery_spec,
            &mut txn,
        )
        .await?;

        txn.commit().await?;

        tracing::info!(
            %catalog_prefix,
            data_planes = ?collection_spec.data_planes,
            stores_count = ?collection_spec.stores.len(),
            "created storage mapping"
        );

        Ok(CreateStorageMappingResult {
            catalog_prefix: catalog_prefix.clone(),
            storage_mapping: StorageMapping {
                catalog_prefix,
                detail,
                spec: async_graphql::Json(collection_spec),
                user_capability: models::Capability::Admin,
            },
        })
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
        spec: async_graphql::Json<models::StorageDef>,
    ) -> async_graphql::Result<UpdateStorageMappingResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;
        let snapshot = env.snapshot();
        let async_graphql::Json(spec) = spec;

        // Do basic input validation checks first.
        validate_inputs(&catalog_prefix, &spec)?;

        // Verify user has admin capability to the catalog prefix and read capability to named data planes.
        evaluate_authorization(env, claims, &catalog_prefix, &spec.data_planes).await?;

        let data_planes = resolve_data_planes(&snapshot, &spec.data_planes)?;

        // Run health checks outside of transaction so as not to keep rows locked too long.
        let health_checks =
            run_all_health_checks(&catalog_prefix, &data_planes, &spec.stores).await;

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
        let republish = spec.stores != current.0.stores;

        // Check if any health check failed for a newly added store or data plane.
        let new_failures: Vec<&StorageHealthItem> = health_checks
            .iter()
            .filter(|c| {
                if c.error.is_none() {
                    return false;
                }
                let is_new_store = !current.0.stores.contains(&c.fragment_store.0);
                let is_new_dp = !current.0.data_planes.contains(&c.data_plane_name);
                is_new_store || is_new_dp
            })
            .collect();

        if !new_failures.is_empty() {
            // We only fail on health check errors for newly added stores or data planes.
            // Tasks under this storage mapping will still be broken if there are any failing
            // health checks, but we allow the update so long as the user isn't adding more
            // problems than there already were.
            return Err(async_graphql::Error::new(format!(
                "Storage health checks failed for newly added stores or data planes:\n{}",
                describe_health_failures(&catalog_prefix, new_failures),
            )));
        }

        // A single conceptual "storage mapping" is (today) stored as two
        // distinct rows. They must align, and this alignment is enforced
        // by the `validations` crate.
        let (collection_spec, recovery_spec) = collection_and_recovery_spec_from(spec);

        let updated = update_storage_mapping(
            detail.as_deref(),
            catalog_prefix.as_str(),
            &collection_spec,
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
            &recovery_spec,
            &mut txn,
        )
        .await?;

        if republish {
            let message = crate::controllers::Message::Republish {
                reason: format!(
                    "storage mappings updated by user {} ({})",
                    claims.email.as_deref().unwrap_or_default(),
                    claims.sub
                ),
            };
            crate::controllers::broadcast_to_prefix(catalog_prefix.as_str(), message, &mut *txn)
                .await?;
        }

        txn.commit().await?;

        tracing::info!(
            %catalog_prefix,
            data_planes = ?collection_spec.data_planes,
            stores_count = ?collection_spec.stores.len(),
            republish,
            "updated storage mapping"
        );

        Ok(UpdateStorageMappingResult {
            catalog_prefix: catalog_prefix.clone(),
            storage_mapping: StorageMapping {
                catalog_prefix,
                detail,
                spec: async_graphql::Json(collection_spec),
                user_capability: models::Capability::Admin,
            },
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
        spec: async_graphql::Json<models::StorageDef>,
    ) -> async_graphql::Result<ConnectionHealthTestResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;
        let snapshot = env.snapshot();
        let async_graphql::Json(spec) = spec;

        // Do basic input validation checks first.
        validate_inputs(&catalog_prefix, &spec)?;

        // Verify user has admin capability to the catalog prefix and read capability to named data planes.
        evaluate_authorization(env, claims, &catalog_prefix, &spec.data_planes).await?;

        let data_planes = resolve_data_planes(&snapshot, &spec.data_planes)?;

        // Run health checks and collect results.
        let results = run_all_health_checks(&catalog_prefix, &data_planes, &spec.stores).await;

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
    /// Exactly one of `exactPrefixes` or `underPrefix` must be provided.
    pub exact_prefixes: Option<Vec<models::Prefix>>,
    /// Fetch all storage mappings under this prefix pattern.
    /// For example, "acmeCo/" returns mappings for "acmeCo/", "acmeCo/team-a/", etc.
    /// Exactly one of `exactPrefixes` or `underPrefix` must be provided.
    pub under_prefix: Option<models::Prefix>,
}

impl StorageMappingsBy {
    /// Maps the deprecated `by` selection onto the equivalent `PrefixFilter`:
    /// `underPrefix` is a `startsWith` subtree and `exactPrefixes` is an `in`
    /// exact set, so `by` and `filter` resolve through one shared path. Enforces
    /// that exactly one mode is set, naming `by`'s own fields in the error.
    fn into_prefix_filter(self) -> async_graphql::Result<filters::PrefixFilter> {
        let exact_prefixes = self.exact_prefixes.unwrap_or_default();
        match (exact_prefixes.is_empty(), self.under_prefix.is_none()) {
            (true, true) => Err(async_graphql::Error::new(
                "provide exactly one of `exactPrefixes` or `underPrefix`, or omit `by` entirely",
            )),
            (false, false) => Err(async_graphql::Error::new(
                "`exactPrefixes` and `underPrefix` are mutually exclusive; provide only one",
            )),
            (false, true) => Ok(filters::PrefixFilter {
                starts_with: None,
                r#in: Some(exact_prefixes.iter().map(|p| p.to_string()).collect()),
            }),
            (true, false) => Ok(filters::PrefixFilter {
                starts_with: self.under_prefix.map(|p| p.to_string()),
                r#in: None,
            }),
        }
    }
}

/// Cloud storage provider of a store. These are the `provider` discriminants
/// that `spec.stores` entries carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, async_graphql::Enum)]
pub enum StoreProvider {
    /// Amazon Simple Storage Service.
    #[graphql(name = "S3")]
    S3,
    /// Google Cloud Storage.
    #[graphql(name = "GCS")]
    Gcs,
    /// Azure object storage service.
    #[graphql(name = "AZURE")]
    Azure,
    /// An S3-compatible endpoint, such as Minio or R2.
    #[graphql(name = "CUSTOM")]
    Custom,
}

impl StoreProvider {
    /// The `provider` tag that the matching `models::Store` variant carries in
    /// a persisted `spec.stores` entry. `store_provider_tags_match_the_model`
    /// pins these against the model's own serialization.
    fn spec_tag(self) -> &'static str {
        match self {
            Self::S3 => "S3",
            Self::Gcs => "GCS",
            Self::Azure => "AZURE",
            Self::Custom => "CUSTOM",
        }
    }
}

/// Matches a single store within a storage mapping's `spec.stores`. Every
/// field is optional; the fields that are set must all hold on one store, and
/// a mapping matches when at least one of its stores satisfies them.
///
/// Each field belongs to the providers that carry it, so pairing one with a
/// `provider` that does not carry it matches nothing.
#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct StoreFilter {
    /// Match the store's provider.
    pub provider: Option<StoreProvider>,
    /// Match the bucket of an `S3`, `GCS`, or `CUSTOM` store. An `AZURE`
    /// store addresses a container instead; see `containerName`.
    pub bucket: Option<String>,
    /// Match the AWS region of an `S3` store, such as `us-east-1`.
    pub region: Option<String>,
    /// Match the S3-compatible endpoint of a `CUSTOM` store.
    pub endpoint: Option<String>,
    /// Match the tenant that owns an `AZURE` store's storage account.
    pub account_tenant_id: Option<String>,
    /// Match the storage account name of an `AZURE` store, which is that
    /// provider's equivalent of a bucket.
    pub storage_account_name: Option<String>,
    /// Match the container name within an `AZURE` store's storage account.
    pub container_name: Option<String>,
}

impl StoreFilter {
    /// Renders this filter as the right-hand operand of a jsonb containment
    /// test against `spec -> 'stores'`, returning `None` when no field is set
    /// so that an empty filter narrows nothing.
    ///
    /// The operand is a one-element array. Postgres' `@>` over two arrays
    /// holds when every element on the right is contained by some element on
    /// the left, and containment between two objects means a subset of
    /// key/value pairs — together, exactly "at least one store carries all of
    /// these fields".
    ///
    /// Keys are the field names that `models::Store` serializes, so the
    /// operand compares against stored specs verbatim.
    fn into_stores_pattern(self) -> Option<String> {
        let Self {
            provider,
            bucket,
            region,
            endpoint,
            account_tenant_id,
            storage_account_name,
            container_name,
        } = self;

        let mut store = serde_json::Map::new();

        if let Some(provider) = provider {
            store.insert(
                "provider".to_string(),
                serde_json::Value::from(provider.spec_tag()),
            );
        }
        for (key, value) in [
            ("bucket", bucket),
            ("region", region),
            ("endpoint", endpoint),
            ("account_tenant_id", account_tenant_id),
            ("storage_account_name", storage_account_name),
            ("container_name", container_name),
        ] {
            if let Some(value) = value {
                store.insert(key.to_string(), serde_json::Value::from(value));
            }
        }

        if store.is_empty() {
            return None;
        }
        Some(serde_json::Value::Array(vec![serde_json::Value::Object(store)]).to_string())
    }
}

/// Composable filter for the `storageMappings` query. Every field is optional
/// and only narrows the result set; the caller's catalog-read scope is enforced
/// independently, so a filter can never widen what a caller may see.
#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct StorageMappingsFilter {
    /// Narrow by catalog prefix. `startsWith` matches a whole subtree —
    /// mappings for `acmeCo/`, `acmeCo/team-a/`, etc. — like the deprecated
    /// `by: { underPrefix }`. `in` matches an exact set of prefixes, like
    /// `by: { exactPrefixes }`. The two are alternative query modes and are
    /// mutually exclusive. Either way, results compose with (never widen past)
    /// the caller's authorized read prefixes.
    pub catalog_prefix: Option<filters::PrefixFilter>,
    /// Narrow to mappings that write to a matching store: at least one entry
    /// of `spec.stores` must carry every field set here. This answers "which
    /// prefixes land in this bucket", within the caller's read scope.
    pub store: Option<StoreFilter>,
}

/// A storage mapping that defines where collection data is stored.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct StorageMapping {
    /// The catalog prefix this storage mapping applies to.
    pub catalog_prefix: models::Prefix,
    /// Optional description of this storage mapping.
    pub detail: Option<String>,
    /// The storage definition containing stores and data plane assignments.
    pub spec: async_graphql::Json<models::StorageDef>,
    /// The current user's capability to this storage mapping's prefix.
    pub user_capability: models::Capability,
}

#[async_graphql::ComplexObject]
impl StorageMapping {
    /// The data planes named by `spec.data_planes`, resolved to nodes. Order
    /// follows the spec, so the first entry is the plane used by default for
    /// tasks and collections under this prefix.
    ///
    /// A name is omitted when no data plane carries it or when the caller
    /// lacks read capability on it, so this list can be shorter than
    /// `spec.data_planes`.
    async fn data_planes(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Vec<super::data_planes::DataPlane>> {
        super::data_planes::data_planes_by_name(ctx, &self.spec.0.data_planes).await
    }
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

const MAX_PREFIXES: usize = 20;

#[async_graphql::Object]
impl StorageMappingsQuery {
    /// Returns storage mappings accessible to the current user.
    ///
    /// Returns mappings under every prefix where the caller has catalog-read
    /// capability. The optional `filter` narrows those authorized results by
    /// catalog prefix, by store, or by both. Results are paginated and sorted
    /// by catalog_prefix.
    pub async fn storage_mappings(
        &self,
        ctx: &Context<'_>,
        #[graphql(
            deprecation = "Prefer `filter: { catalogPrefix }`: `startsWith` replaces `underPrefix` \
                                 and `in` replaces `exactPrefixes`. `by` is retained only for \
                                 existing clients."
        )]
        by: Option<StorageMappingsBy>,
        filter: Option<StorageMappingsFilter>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> async_graphql::Result<PaginatedStorageMappings> {
        let env = ctx.data::<crate::Envelope>()?;

        let (filter_catalog_prefix, store_filter) = match filter {
            Some(StorageMappingsFilter {
                catalog_prefix,
                store,
            }) => (catalog_prefix, store),
            None => (None, None),
        };

        // A store filter is an independent predicate over `spec.stores`: it
        // narrows within the caller's prefix scope and never interacts with it,
        // so it composes with either prefix-scoping argument.
        let stores_pattern = store_filter.and_then(StoreFilter::into_stores_pattern);

        // `filter.catalogPrefix` is the going-forward replacement for `by`. Map
        // the deprecated `by` onto the same `PrefixFilter` shape — `underPrefix`
        // is a `startsWith` subtree, `exactPrefixes` is an `in` exact set — so
        // both resolve through the shared `filtered_authorized_prefixes`. The
        // two prefix-scoping modes are mutually exclusive.
        let prefix_filter = match (by, filter_catalog_prefix) {
            (Some(by), filter_catalog_prefix) => {
                if filter_catalog_prefix
                    .is_some_and(|cp| cp.starts_with.is_some() || cp.r#in.is_some())
                {
                    return Err(
                        "provide either `by` or `filter`, not both; `by` is deprecated".into(),
                    );
                }
                Some(by.into_prefix_filter()?)
            }
            (None, filter_catalog_prefix) => filter_catalog_prefix,
        };

        let snapshot = env.snapshot();
        let (read_prefixes, under_prefix, exact_prefixes) =
            super::authorized_prefixes::filtered_authorized_prefixes(
                &snapshot.role_grants,
                &snapshot.user_grants,
                env.claims()?.sub,
                models::authz::Capability::CatalogRead,
                prefix_filter,
                "filter.catalogPrefix",
            )
            .map(|(prefixes, starts_with, r#in)| {
                (prefixes, starts_with, r#in.unwrap_or_default())
            })?;

        if read_prefixes.is_empty() {
            return Ok(PaginatedStorageMappings::new(false, false));
        }
        if read_prefixes.len() > MAX_PREFIXES {
            return Err(async_graphql::Error::new(
                "Too many accessible prefixes; narrow results with a filter",
            ));
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
                            &read_prefixes,
                            &exact_prefixes,
                            under_prefix.as_deref(),
                            stores_pattern.as_deref(),
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
                            &read_prefixes,
                            &exact_prefixes,
                            under_prefix.as_deref(),
                            stores_pattern.as_deref(),
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

                Ok(connection::Edge::new(
                    row.catalog_prefix.clone(),
                    StorageMapping {
                        catalog_prefix: models::Prefix::new(row.catalog_prefix),
                        detail: row.detail,
                        spec: async_graphql::Json(row.spec),
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
    read_prefixes: &[String],
    exact_prefixes: &[String],
    under_prefix: Option<&str>,
    stores_pattern: Option<&str>,
    after: Option<&str>,
    limit: i64,
) -> anyhow::Result<Vec<StorageMappingRow>> {
    // Prefixes bind as `text[]`/`text` rather than the `_catalog_name` domain
    // array, so sqlx does not run domain-constraint validation on bind.
    let filter_all = exact_prefixes.is_empty() && under_prefix.is_none();

    let rows = sqlx::query!(
        r#"
        SELECT
            catalog_prefix as "catalog_prefix!: String",
            detail,
            spec as "spec!: crate::TextJson<models::StorageDef>"
        FROM storage_mappings
        WHERE NOT catalog_prefix ^@ 'recovery/'
        AND catalog_prefix::text ^@ ANY($1)
        AND (
            $2::bool
            OR catalog_prefix::text = any($3::text[])
            OR ($4::text IS NOT NULL AND catalog_prefix ^@ $4::text)
        )
        AND (
            $5::text IS NULL
            OR (spec::jsonb -> 'stores') @> $5::text::jsonb
        )
        AND ($6::text IS NULL OR catalog_prefix > $6::text)
        ORDER BY catalog_prefix ASC
        LIMIT $7
        "#,
        read_prefixes,
        filter_all,
        exact_prefixes,
        under_prefix,
        stores_pattern,
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
    read_prefixes: &[String],
    exact_prefixes: &[String],
    under_prefix: Option<&str>,
    stores_pattern: Option<&str>,
    before: Option<&str>,
    limit: i64,
) -> anyhow::Result<Vec<StorageMappingRow>> {
    // Prefixes bind as `text[]`/`text` rather than the `_catalog_name` domain
    // array, so sqlx does not run domain-constraint validation on bind.
    let filter_all = exact_prefixes.is_empty() && under_prefix.is_none();

    let mut rows = sqlx::query!(
        r#"
        SELECT
            catalog_prefix as "catalog_prefix!: String",
            detail,
            spec as "spec!: crate::TextJson<models::StorageDef>"
        FROM storage_mappings
        WHERE NOT catalog_prefix ^@ 'recovery/'
        AND catalog_prefix::text ^@ ANY($1)
        AND (
            $2::bool
            OR catalog_prefix::text = any($3::text[])
            OR ($4::text IS NOT NULL AND catalog_prefix ^@ $4::text)
        )
        AND (
            $5::text IS NULL
            OR (spec::jsonb -> 'stores') @> $5::text::jsonb
        )
        AND ($6::text IS NULL OR catalog_prefix < $6::text)
        ORDER BY catalog_prefix DESC
        LIMIT $7
        "#,
        read_prefixes,
        filter_all,
        exact_prefixes,
        under_prefix,
        stores_pattern,
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

#[cfg(test)]
mod test {
    use crate::test_server;

    #[test]
    fn by_under_prefix_maps_to_starts_with() {
        let filter = super::StorageMappingsBy {
            exact_prefixes: None,
            under_prefix: Some(models::Prefix::new("acmeCo/")),
        }
        .into_prefix_filter()
        .unwrap();
        assert_eq!(filter.starts_with.as_deref(), Some("acmeCo/"));
        assert_eq!(filter.r#in, None);
    }

    #[test]
    fn by_exact_prefixes_maps_to_in() {
        let filter = super::StorageMappingsBy {
            exact_prefixes: Some(vec![
                models::Prefix::new("acmeCo/"),
                models::Prefix::new("betaCo/"),
            ]),
            under_prefix: None,
        }
        .into_prefix_filter()
        .unwrap();
        assert_eq!(filter.starts_with, None);
        assert_eq!(
            filter.r#in,
            Some(vec!["acmeCo/".to_string(), "betaCo/".to_string()])
        );
    }

    #[test]
    fn by_rejects_both_modes() {
        let err = super::StorageMappingsBy {
            exact_prefixes: Some(vec![models::Prefix::new("acmeCo/")]),
            under_prefix: Some(models::Prefix::new("acmeCo/")),
        }
        .into_prefix_filter()
        .unwrap_err();
        assert_eq!(
            err.message,
            "`exactPrefixes` and `underPrefix` are mutually exclusive; provide only one"
        );
    }

    #[test]
    fn by_rejects_neither_mode() {
        // Empty `exactPrefixes` counts as absent, so this is the "neither" case
        // — legacy `by` errors rather than treating it as "match everything".
        let err = super::StorageMappingsBy {
            exact_prefixes: Some(vec![]),
            under_prefix: None,
        }
        .into_prefix_filter()
        .unwrap_err();
        assert_eq!(
            err.message,
            "provide exactly one of `exactPrefixes` or `underPrefix`, or omit `by` entirely"
        );
    }

    // `StoreProvider` exists so callers name a provider by enum rather than by
    // the string a spec happens to carry. Pin each tag against the model's own
    // serialization: a rename in `models::Store` must fail here rather than
    // silently reduce the filter to matching nothing.
    #[test]
    fn store_provider_tags_match_the_model() {
        let cases = [
            (
                super::StoreProvider::S3,
                models::Store::S3(models::S3StorageConfig::example()),
            ),
            (
                super::StoreProvider::Gcs,
                models::Store::Gcs(models::GcsBucketAndPrefix::example()),
            ),
            (
                super::StoreProvider::Azure,
                models::Store::Azure(models::AzureStorageConfig::example()),
            ),
            (
                super::StoreProvider::Custom,
                models::Store::Custom(models::CustomStore::example()),
            ),
        ];

        for (provider, store) in cases {
            let serialized = serde_json::to_value(&store).unwrap();
            assert_eq!(
                serialized["provider"].as_str(),
                Some(provider.spec_tag()),
                "provider tag for {provider:?}",
            );
        }
    }

    // The rendered operand is a one-element array of the set fields, under the
    // key names a spec carries. The `filter_by_store` test exercises what
    // Postgres does with it.
    #[test]
    fn store_filter_renders_a_containment_operand() {
        let pattern = super::StoreFilter {
            provider: Some(super::StoreProvider::S3),
            bucket: Some("acme-data".to_string()),
            ..Default::default()
        }
        .into_stores_pattern()
        .unwrap();
        assert_eq!(pattern, r#"[{"bucket":"acme-data","provider":"S3"}]"#);

        // Azure's fields keep their spec key names, which are snake_case.
        let pattern = super::StoreFilter {
            provider: Some(super::StoreProvider::Azure),
            storage_account_name: Some("acmestorage".to_string()),
            container_name: Some("acme-container".to_string()),
            ..Default::default()
        }
        .into_stores_pattern()
        .unwrap();
        assert_eq!(
            pattern,
            r#"[{"container_name":"acme-container","provider":"AZURE","storage_account_name":"acmestorage"}]"#
        );

        // A field may be given without a provider.
        let pattern = super::StoreFilter {
            bucket: Some("acme-data".to_string()),
            ..Default::default()
        }
        .into_stores_pattern()
        .unwrap();
        assert_eq!(pattern, r#"[{"bucket":"acme-data"}]"#);

        // An empty filter narrows nothing, so it renders no operand at all
        // rather than an always-true or always-false one.
        assert_eq!(super::StoreFilter::default().into_stores_pattern(), None);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn storage_mappings_are_scoped_to_readable_prefixes(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let mut store = models::Store::example();
        *store.prefix_mut() = models::Prefix::new("tenant/collection-data/");
        let spec = crate::TextJson(models::StorageDef {
            data_planes: Vec::new(),
            stores: vec![store],
        });
        for prefix in ["aliceCo/", "aliceCo/team/", "otherCo/"] {
            sqlx::query("INSERT INTO storage_mappings (catalog_prefix, spec) VALUES ($1, $2)")
                .bind(prefix)
                .bind(&spec)
                .execute(&pool)
                .await
                .unwrap();
        }

        let snapshot = test_server::snapshot(pool.clone(), false).await;
        let server = test_server::TestServer::start(pool.clone(), snapshot).await;
        let alice_token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);
        let bob_token = server.make_access_token(uuid::Uuid::from_bytes([0x22; 16]), None);

        let query = |by: Option<serde_json::Value>| {
            serde_json::json!({
                "query": r#"
                    query($by: StorageMappingsBy) {
                        storageMappings(by: $by) {
                            edges { node { catalogPrefix } }
                        }
                    }
                "#,
                "variables": { "by": by },
            })
        };

        let all_readable: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                        query {
                            storageMappings {
                                edges { node { catalogPrefix } }
                            }
                        }
                    "#,
                }),
                Some(&alice_token),
            )
            .await;
        insta::assert_json_snapshot!(all_readable, @r###"
        {
          "data": {
            "storageMappings": {
              "edges": [
                {
                  "node": {
                    "catalogPrefix": "aliceCo/"
                  }
                },
                {
                  "node": {
                    "catalogPrefix": "aliceCo/team/"
                  }
                }
              ]
            }
          }
        }
        "###);

        let narrowed: serde_json::Value = server
            .graphql(
                &query(Some(serde_json::json!({
                    "exactPrefixes": ["aliceCo/", "otherCo/"]
                }))),
                Some(&alice_token),
            )
            .await;
        insta::assert_json_snapshot!(narrowed, @r###"
        {
          "data": {
            "storageMappings": {
              "edges": [
                {
                  "node": {
                    "catalogPrefix": "aliceCo/"
                  }
                }
              ]
            }
          }
        }
        "###);

        let no_access: serde_json::Value = server.graphql(&query(None), Some(&bob_token)).await;
        insta::assert_json_snapshot!(no_access, @r###"
        {
          "data": {
            "storageMappings": {
              "edges": []
            }
          }
        }
        "###);
    }

    // The `filter` argument scopes by catalog-prefix subtree exactly like the
    // deprecated `by: { underPrefix }`, and composes with the caller's read
    // scope. Providing both `by` and `filter` is rejected.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn storage_mappings_filter_scopes_by_prefix(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let spec = crate::TextJson(models::StorageDef {
            data_planes: Vec::new(),
            stores: vec![models::Store::example()],
        });
        for prefix in ["aliceCo/", "aliceCo/team/", "otherCo/"] {
            sqlx::query("INSERT INTO storage_mappings (catalog_prefix, spec) VALUES ($1, $2)")
                .bind(prefix)
                .bind(&spec)
                .execute(&pool)
                .await
                .unwrap();
        }

        let snapshot = test_server::snapshot(pool.clone(), false).await;
        let server = test_server::TestServer::start(pool.clone(), snapshot).await;
        let alice_token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        // Helper: run the query with the given variables and return the sorted
        // list of returned catalog prefixes, asserting no GraphQL errors.
        async fn prefixes(
            server: &test_server::TestServer,
            token: &str,
            variables: serde_json::Value,
        ) -> Vec<String> {
            let response: serde_json::Value = server
                .graphql(
                    &serde_json::json!({
                        "query": r#"
                            query($by: StorageMappingsBy, $filter: StorageMappingsFilter) {
                                storageMappings(by: $by, filter: $filter) {
                                    edges { node { catalogPrefix } }
                                }
                            }
                        "#,
                        "variables": variables,
                    }),
                    Some(token),
                )
                .await;
            assert!(
                response.get("errors").is_none(),
                "unexpected errors: {response}"
            );
            response["data"]["storageMappings"]["edges"]
                .as_array()
                .expect("edges array")
                .iter()
                .map(|edge| edge["node"]["catalogPrefix"].as_str().unwrap().to_string())
                .collect()
        }

        // A prefix filter narrows to the matching subtree, the same result the
        // deprecated `by: { underPrefix }` produces.
        let narrowed = prefixes(
            &server,
            &alice_token,
            serde_json::json!({ "filter": { "catalogPrefix": { "startsWith": "aliceCo/team/" } } }),
        )
        .await;
        assert_eq!(narrowed, vec!["aliceCo/team/"]);

        // A broader prefix returns the whole authorized subtree.
        let subtree = prefixes(
            &server,
            &alice_token,
            serde_json::json!({ "filter": { "catalogPrefix": { "startsWith": "aliceCo/" } } }),
        )
        .await;
        assert_eq!(subtree, vec!["aliceCo/", "aliceCo/team/"]);

        // The filter can never widen scope past the caller's grants.
        let cross_tenant = prefixes(
            &server,
            &alice_token,
            serde_json::json!({ "filter": { "catalogPrefix": { "startsWith": "otherCo/" } } }),
        )
        .await;
        assert!(cross_tenant.is_empty());

        // Omitting the filter returns every accessible mapping. This is the
        // baseline the present-but-empty filter forms must match.
        let no_filter = prefixes(&server, &alice_token, serde_json::json!({})).await;
        assert_eq!(no_filter, vec!["aliceCo/", "aliceCo/team/"]);

        // An empty filter — and an empty `catalogPrefix` within it — behave
        // like omitting the filter: neither narrows anything.
        let empty_filter =
            prefixes(&server, &alice_token, serde_json::json!({ "filter": {} })).await;
        assert_eq!(empty_filter, no_filter);
        let empty_catalog_prefix = prefixes(
            &server,
            &alice_token,
            serde_json::json!({ "filter": { "catalogPrefix": {} } }),
        )
        .await;
        assert_eq!(empty_catalog_prefix, no_filter);

        // `in` matches an exact set of prefixes (unlike `startsWith`, it does
        // not descend into the subtree), the same result `by: { exactPrefixes }`
        // produces.
        let exact_one = prefixes(
            &server,
            &alice_token,
            serde_json::json!({ "filter": { "catalogPrefix": { "in": ["aliceCo/"] } } }),
        )
        .await;
        assert_eq!(exact_one, vec!["aliceCo/"]);

        // A cross-tenant `in` entry is dropped rather than widening scope; an
        // unknown prefix simply matches nothing. This also confirms several
        // `in` entries return every authorized exact match (`filters::test`
        // covers the multi-entry narrowing logic directly).
        let exact_cross_tenant = prefixes(
            &server,
            &alice_token,
            serde_json::json!({
                "filter": { "catalogPrefix": { "in": ["aliceCo/", "otherCo/", "ghostCo/"] } }
            }),
        )
        .await;
        assert_eq!(exact_cross_tenant, vec!["aliceCo/"]);

        // Helper: assert a set of variables is rejected with a GraphQL error.
        // When `expected_message` is given it must appear in the first error, so
        // that distinct rejection branches are told apart rather than any error
        // counting as a pass.
        async fn expect_error(
            server: &test_server::TestServer,
            token: &str,
            variables: serde_json::Value,
            expected_message: Option<&str>,
        ) {
            let response: serde_json::Value = server
                .graphql(
                    &serde_json::json!({
                        "query": r#"
                            query($by: StorageMappingsBy, $filter: StorageMappingsFilter) {
                                storageMappings(by: $by, filter: $filter) {
                                    edges { node { catalogPrefix } }
                                }
                            }
                        "#,
                        "variables": variables,
                    }),
                    Some(token),
                )
                .await;
            assert!(
                response["errors"]
                    .as_array()
                    .is_some_and(|errors| !errors.is_empty()),
                "expected an error for variables {variables}: {response}"
            );
            if let Some(expected) = expected_message {
                let message = response["errors"][0]["message"]
                    .as_str()
                    .unwrap_or_default();
                assert!(
                    message.contains(expected),
                    "expected error containing {expected:?}, got {message:?} for variables {variables}"
                );
            }
        }

        // `by` and `filter` are mutually exclusive.
        expect_error(
            &server,
            &alice_token,
            serde_json::json!({
                "by": { "underPrefix": "aliceCo/" },
                "filter": { "catalogPrefix": { "startsWith": "aliceCo/" } },
            }),
            Some("provide either `by` or `filter`"),
        )
        .await;

        // Within a filter, `startsWith` and `in` are mutually exclusive.
        expect_error(
            &server,
            &alice_token,
            serde_json::json!({
                "filter": { "catalogPrefix": { "startsWith": "aliceCo/", "in": ["aliceCo/"] } },
            }),
            Some("mutually exclusive; provide only one"),
        )
        .await;

        // An empty `in` set is rejected at input validation, rather than
        // ambiguously meaning "match nothing" or "match everything".
        expect_error(
            &server,
            &alice_token,
            serde_json::json!({
                "filter": { "catalogPrefix": { "in": [] } },
            }),
            None,
        )
        .await;
    }

    // `filter.store` narrows to mappings that write to a matching store. The
    // fields set on the filter must all hold on one store of the mapping, and
    // the filter composes with (never widens past) the caller's read scope.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn storage_mappings_filter_by_store(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        fn s3(bucket: &str, region: &str) -> models::Store {
            models::Store::S3(models::S3StorageConfig {
                bucket: bucket.to_string(),
                prefix: None,
                region: Some(region.to_string()),
            })
        }
        fn gcs(bucket: &str) -> models::Store {
            models::Store::Gcs(models::GcsBucketAndPrefix {
                bucket: bucket.to_string(),
                prefix: None,
            })
        }

        // `aliceCo/team/` writes to two stores, so it is the case that tells
        // "one store carries every field" apart from "the fields are spread
        // across two stores": no single store is GCS in `us-west-2`.
        let mappings: [(&str, Vec<models::Store>); 3] = [
            ("aliceCo/", vec![s3("acme-data", "us-east-1")]),
            (
                "aliceCo/team/",
                vec![gcs("acme-archive"), s3("team-data", "us-west-2")],
            ),
            ("otherCo/", vec![s3("acme-data", "us-east-1")]),
        ];
        for (prefix, stores) in mappings {
            let spec = crate::TextJson(models::StorageDef {
                data_planes: Vec::new(),
                stores,
            });
            sqlx::query("INSERT INTO storage_mappings (catalog_prefix, spec) VALUES ($1, $2)")
                .bind(prefix)
                .bind(&spec)
                .execute(&pool)
                .await
                .unwrap();
        }

        let snapshot = test_server::snapshot(pool.clone(), false).await;
        let server = test_server::TestServer::start(pool.clone(), snapshot).await;
        let alice_token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        async fn prefixes(
            server: &test_server::TestServer,
            token: &str,
            filter: serde_json::Value,
        ) -> Vec<String> {
            let response: serde_json::Value = server
                .graphql(
                    &serde_json::json!({
                        "query": r#"
                            query($filter: StorageMappingsFilter) {
                                storageMappings(filter: $filter) {
                                    edges { node { catalogPrefix } }
                                }
                            }
                        "#,
                        "variables": { "filter": filter },
                    }),
                    Some(token),
                )
                .await;
            assert!(
                response.get("errors").is_none(),
                "unexpected errors: {response}"
            );
            response["data"]["storageMappings"]["edges"]
                .as_array()
                .expect("edges array")
                .iter()
                .map(|edge| edge["node"]["catalogPrefix"].as_str().unwrap().to_string())
                .collect()
        }

        // Provider plus bucket is the motivating query: which of my prefixes
        // land in this bucket. `otherCo/` also uses it but is out of scope.
        let by_bucket = prefixes(
            &server,
            &alice_token,
            serde_json::json!({ "store": { "provider": "S3", "bucket": "acme-data" } }),
        )
        .await;
        assert_eq!(by_bucket, vec!["aliceCo/"]);

        // A bucket alone matches without naming a provider.
        let bucket_only = prefixes(
            &server,
            &alice_token,
            serde_json::json!({ "store": { "bucket": "acme-archive" } }),
        )
        .await;
        assert_eq!(bucket_only, vec!["aliceCo/team/"]);

        // A provider alone matches every mapping holding a store of it, which
        // for GCS is only the mapping whose second store is GCS.
        let by_provider = prefixes(
            &server,
            &alice_token,
            serde_json::json!({ "store": { "provider": "GCS" } }),
        )
        .await;
        assert_eq!(by_provider, vec!["aliceCo/team/"]);
        let s3_provider = prefixes(
            &server,
            &alice_token,
            serde_json::json!({ "store": { "provider": "S3" } }),
        )
        .await;
        assert_eq!(s3_provider, vec!["aliceCo/", "aliceCo/team/"]);

        // Region is an S3 field and narrows within the provider.
        let by_region = prefixes(
            &server,
            &alice_token,
            serde_json::json!({ "store": { "provider": "S3", "region": "us-west-2" } }),
        )
        .await;
        assert_eq!(by_region, vec!["aliceCo/team/"]);

        // Every field must hold on the SAME store. `aliceCo/team/` holds a GCS
        // store and a `us-west-2` store, but no GCS store in `us-west-2`.
        let cross_store = prefixes(
            &server,
            &alice_token,
            serde_json::json!({ "store": { "provider": "GCS", "region": "us-west-2" } }),
        )
        .await;
        assert!(
            cross_store.is_empty(),
            "matched across stores: {cross_store:?}"
        );

        // A field paired with a provider that does not carry it matches
        // nothing, rather than being ignored.
        let mismatched_field = prefixes(
            &server,
            &alice_token,
            serde_json::json!({
                "store": { "provider": "S3", "storageAccountName": "acmestorage" }
            }),
        )
        .await;
        assert!(mismatched_field.is_empty());

        // An unknown bucket matches nothing.
        let unknown = prefixes(
            &server,
            &alice_token,
            serde_json::json!({ "store": { "bucket": "ghost-bucket" } }),
        )
        .await;
        assert!(unknown.is_empty());

        // A store filter composes with a catalog-prefix filter.
        let composed = prefixes(
            &server,
            &alice_token,
            serde_json::json!({
                "catalogPrefix": { "startsWith": "aliceCo/team/" },
                "store": { "provider": "S3" },
            }),
        )
        .await;
        assert_eq!(composed, vec!["aliceCo/team/"]);

        // An empty store filter behaves like omitting it: it narrows nothing.
        let all = prefixes(&server, &alice_token, serde_json::json!({})).await;
        assert_eq!(all, vec!["aliceCo/", "aliceCo/team/"]);
        let empty_store = prefixes(&server, &alice_token, serde_json::json!({ "store": {} })).await;
        assert_eq!(empty_store, all);
    }

    // `StorageMapping.dataPlanes` expands the names in `spec.data_planes` into
    // `DataPlane` nodes, in spec order, dropping names the caller cannot see.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn storage_mapping_data_planes_resolve_spec_names(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        // A private plane alice cannot read. Non-empty `hmac_keys` is what puts
        // a plane in the authorization snapshot at all, so without it this row
        // would be absent for a reason other than the one under test.
        sqlx::query(
            r#"
            INSERT INTO data_planes (
                id, data_plane_name, data_plane_fqdn, hmac_keys,
                broker_address, reactor_address,
                ops_logs_name, ops_stats_name,
                ops_l1_events_name, ops_l1_inferred_name, ops_l1_stats_name,
                ops_l2_events_transform, ops_l2_inferred_transform, ops_l2_stats_transform,
                enable_l2
            ) VALUES (
                '444444444444', 'ops/dp/private/otherCo/aws-us-east-1-c1', 'dp.other', '{c2VjcmV0}',
                'broker.dp.other', 'reactor.dp.other',
                'ops/tasks/private/other/logs', 'ops/tasks/private/other/stats',
                'ops/rollups/L1/private/other/events',
                'ops/rollups/L1/private/other/inferred',
                'ops/rollups/L1/private/other/stats',
                'from.dp.other', 'from.dp.other', 'from.dp.other',
                true
            )
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        // The spec lists the second public plane first, so the assertion below
        // shows the resolver preserves spec order rather than sorting by name.
        // It also names a plane alice cannot read and one that does not exist.
        let spec = crate::TextJson(models::StorageDef {
            data_planes: vec![
                "ops/dp/public/gcp-us-central1-c2".to_string(),
                "ops/dp/public/aws-us-west-2-c1".to_string(),
                "ops/dp/private/otherCo/aws-us-east-1-c1".to_string(),
                "ops/dp/public/aws-eu-west-1-c9".to_string(),
            ],
            stores: vec![models::Store::example()],
        });
        sqlx::query("INSERT INTO storage_mappings (catalog_prefix, spec) VALUES ($1, $2)")
            .bind("aliceCo/")
            .bind(&spec)
            .execute(&pool)
            .await
            .unwrap();

        let snapshot = test_server::snapshot(pool.clone(), false).await;
        let server = test_server::TestServer::start(pool.clone(), snapshot).await;
        let alice_token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                        query {
                            storageMappings {
                                edges {
                                    node {
                                        catalogPrefix
                                        spec
                                        dataPlanes {
                                            name
                                            cloudProvider
                                            region
                                            isPublic
                                            userCapability
                                            cidrBlocks
                                            awsIamUserArn
                                        }
                                    }
                                }
                            }
                        }
                    "#,
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response}"
        );

        // The two readable public planes resolve, in spec order, with detail
        // fields loaded from the database. The unreadable and unknown names are
        // dropped, so `dataPlanes` is shorter than `spec.data_planes`. Keys
        // within each node are alphabetical because the snapshot renders a
        // `serde_json::Value`; `name` is what carries the spec's ordering.
        insta::assert_json_snapshot!(response["data"]["storageMappings"]["edges"][0]["node"]["dataPlanes"], @r###"
        [
          {
            "awsIamUserArn": "arn:aws:iam::987654321:user/test",
            "cidrBlocks": [
              "172.16.0.0/12"
            ],
            "cloudProvider": "GCP",
            "isPublic": true,
            "name": "ops/dp/public/gcp-us-central1-c2",
            "region": "us-central1",
            "userCapability": "read"
          },
          {
            "awsIamUserArn": "arn:aws:iam::123456789:user/test",
            "cidrBlocks": [
              "10.0.0.0/16",
              "192.168.1.0/24"
            ],
            "cloudProvider": "AWS",
            "isPublic": true,
            "name": "ops/dp/public/aws-us-west-2-c1",
            "region": "us-west-2",
            "userCapability": "read"
          }
        ]
        "###);

        // `spec.data_planes` still reports every configured name, including the
        // ones `dataPlanes` withholds.
        let spec_names =
            &response["data"]["storageMappings"]["edges"][0]["node"]["spec"]["data_planes"];
        assert_eq!(
            spec_names.as_array().map(Vec::len),
            Some(4),
            "spec should be unfiltered: {spec_names}"
        );
    }
}
