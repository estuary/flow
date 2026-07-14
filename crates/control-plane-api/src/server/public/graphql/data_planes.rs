use async_graphql::{
    ComplexObject, Context, SimpleObject,
    types::connection::{self, Connection},
};
use std::collections::HashMap;

const DEFAULT_PAGE_SIZE: usize = 50;

/// Cloud provider where the data plane is hosted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, async_graphql::Enum)]
pub enum DataPlaneCloudProvider {
    Aws,
    Azure,
    Gcp,
    Local,
}

/// Controller-observed provisioning status of a configured private link.
#[derive(Debug, Clone, Copy, PartialEq, Eq, async_graphql::Enum)]
pub enum PrivateLinkProvisioningStatus {
    /// Not yet provisioned for the current configuration.
    Pending,
    /// Provisioned; `details` describes the endpoint.
    Provisioned,
    /// Provisioning failed; see `error`.
    Failed,
}

impl PrivateLinkProvisioningStatus {
    fn from_db(s: &str) -> async_graphql::Result<Self> {
        match s {
            "pending" => Ok(Self::Pending),
            "provisioned" => Ok(Self::Provisioned),
            "failed" => Ok(Self::Failed),
            other => Err(async_graphql::Error::new(format!(
                "unknown private link status {other:?}"
            ))),
        }
    }
}

/// A configured private link and its controller-observed provisioning status.
#[derive(Debug, Clone, SimpleObject)]
#[graphql(name = "PrivateLink")]
pub struct DataPlanePrivateLink {
    /// Stable identifier of this private link.
    pub id: models::Id,
    /// The link configuration (AWS PrivateLink, Azure Private Link, or GCP PSC).
    /// Its variant (`AWSPrivateLink`/`AzurePrivateLink`/`GCPPrivateServiceConnect`)
    /// is the link's cloud provider.
    pub config: models::PrivateLink,
    /// Controller-observed provisioning status.
    pub status: PrivateLinkProvisioningStatus,
    /// Provider-specific provisioning details (DNS entries, IPs) once
    /// provisioned; opaque JSON exported by the data-plane controller.
    pub details: Option<async_graphql::Json<serde_json::Value>>,
    /// Failure detail when `status` is `failed`.
    pub error: Option<String>,
    /// When the controller last observed this link's status.
    pub observed_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Keys the request-scoped loader by data plane so sibling `privateLinks`
/// resolvers collapse into one batch query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct DataPlanePrivateLinksKey(models::Id);

impl async_graphql::dataloader::Loader<DataPlanePrivateLinksKey> for super::PgDataLoader {
    type Value = Vec<DataPlanePrivateLink>;
    type Error = String;

    async fn load(
        &self,
        keys: &[DataPlanePrivateLinksKey],
    ) -> Result<HashMap<DataPlanePrivateLinksKey, Self::Value>, Self::Error> {
        let data_plane_ids: Vec<models::Id> = keys.iter().map(|key| key.0).collect();
        let rows = sqlx::query!(
            r#"
            SELECT
                data_plane_id as "data_plane_id: models::Id",
                id as "id: models::Id",
                config as "config!: sqlx::types::Json<models::PrivateLink>",
                status,
                details as "details: sqlx::types::Json<serde_json::Value>",
                error,
                observed_at as "observed_at: chrono::DateTime<chrono::Utc>"
            FROM internal.data_plane_private_links
            WHERE data_plane_id = ANY($1::flowid[])
            ORDER BY data_plane_id, created_at, id
            "#,
            &data_plane_ids as &[models::Id],
        )
        .fetch_all(&self.0)
        .await
        .map_err(|err| format!("failed to fetch data plane private links: {err}"))?;

        let mut result: HashMap<DataPlanePrivateLinksKey, Vec<DataPlanePrivateLink>> =
            HashMap::new();
        for row in rows {
            let status =
                PrivateLinkProvisioningStatus::from_db(&row.status).map_err(|err| err.message)?;
            result
                .entry(DataPlanePrivateLinksKey(row.data_plane_id))
                .or_default()
                .push(DataPlanePrivateLink {
                    id: row.id,
                    config: row.config.0,
                    status,
                    details: row.details.map(|details| async_graphql::Json(details.0)),
                    error: row.error,
                    observed_at: row.observed_at,
                });
        }
        Ok(result)
    }
}

/// A data plane where tasks execute and collections are stored.
#[derive(Debug, Clone, SimpleObject)]
#[graphql(complex)]
pub struct DataPlane {
    /// Name of this data-plane under the catalog namespace.
    pub name: String,
    /// Fully-qualified domain name of this data-plane.
    pub fqdn: String,
    /// Address of reactors within the data-plane.
    pub reactor_address: String,
    /// The current user's capability to this data plane's name prefix.
    pub user_capability: models::Capability,
    /// Cloud provider where this data-plane is hosted.
    pub cloud_provider: DataPlaneCloudProvider,
    /// Cloud region where this data-plane is hosted.
    /// For example: "us-east-1" (AWS), "us-central1" (GCP), "eastus" (Azure).
    pub region: String,
    /// Tag (cluster) identifier within the region.
    pub tag: String,
    /// Whether this is a public data-plane.
    pub is_public: bool,
    /// CIDR blocks for this data-plane.
    pub cidr_blocks: Vec<String>,
    /// GCP service account email for this data-plane.
    pub gcp_service_account_email: Option<String>,
    /// AWS IAM user ARN for this data-plane.
    pub aws_iam_user_arn: Option<String>,
    /// Azure application name for this data-plane.
    pub azure_application_name: Option<String>,
    /// Azure application client ID for this data-plane.
    pub azure_application_client_id: Option<String>,
    // The private-networking fields below are gated behind
    // `ViewDataPlanePrivateNetworking` and resolved by `ComplexObject` methods,
    // so the capability check lives with the field rather than the construction
    // site; see the resolvers below. `control_id` lets the `private_links`
    // resolver query the `data_plane_private_links` table; the endpoint arrays
    // are raw JSON exported by the controller.
    #[graphql(skip)]
    control_id: models::Id,
    #[graphql(skip)]
    raw_aws_link_endpoints: Vec<serde_json::Value>,
    #[graphql(skip)]
    raw_azure_link_endpoints: Vec<serde_json::Value>,
    #[graphql(skip)]
    raw_gcp_psc_endpoints: Vec<serde_json::Value>,
}

#[ComplexObject]
impl DataPlane {
    /// Configured private links for this data-plane, each with its
    /// controller-observed provisioning status. Mutating links (via
    /// `addDataPlanePrivateLink` and friends) triggers reconvergence by the
    /// data-plane controller. Returns an empty list to callers that lack the
    /// `ViewDataPlanePrivateNetworking` capability on this data plane.
    async fn private_links(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Vec<DataPlanePrivateLink>> {
        if !super::may_access(
            ctx,
            &self.name,
            models::authz::Capability::ViewDataPlanePrivateNetworking,
        )? {
            return Ok(Vec::new());
        }
        let loader = ctx.data::<async_graphql::dataloader::DataLoader<super::PgDataLoader>>()?;
        Ok(loader
            .load_one(DataPlanePrivateLinksKey(self.control_id))
            .await?
            .unwrap_or_default())
    }

    /// AWS PrivateLink endpoint provisioning results, opaque JSON exported by
    /// the data-plane controller. Empty when no AWS endpoints are provisioned,
    /// or when the caller lacks `ViewDataPlanePrivateNetworking`.
    async fn aws_link_endpoints(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Vec<async_graphql::Json<serde_json::Value>>> {
        if !super::may_access(
            ctx,
            &self.name,
            models::authz::Capability::ViewDataPlanePrivateNetworking,
        )? {
            return Ok(Vec::new());
        }
        Ok(self
            .raw_aws_link_endpoints
            .iter()
            .cloned()
            .map(async_graphql::Json)
            .collect())
    }

    /// Azure Private Link endpoint provisioning results, opaque JSON. Empty when
    /// the caller lacks `ViewDataPlanePrivateNetworking`.
    async fn azure_link_endpoints(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Vec<async_graphql::Json<serde_json::Value>>> {
        if !super::may_access(
            ctx,
            &self.name,
            models::authz::Capability::ViewDataPlanePrivateNetworking,
        )? {
            return Ok(Vec::new());
        }
        Ok(self
            .raw_azure_link_endpoints
            .iter()
            .cloned()
            .map(async_graphql::Json)
            .collect())
    }

    /// GCP Private Service Connect endpoint provisioning results, opaque JSON.
    /// Empty when the caller lacks `ViewDataPlanePrivateNetworking`.
    async fn gcp_psc_endpoints(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Vec<async_graphql::Json<serde_json::Value>>> {
        if !super::may_access(
            ctx,
            &self.name,
            models::authz::Capability::ViewDataPlanePrivateNetworking,
        )? {
            return Ok(Vec::new());
        }
        Ok(self
            .raw_gcp_psc_endpoints
            .iter()
            .cloned()
            .map(async_graphql::Json)
            .collect())
    }
}

/// Fetches detail fields for the given data plane names from the database.
/// Returns a map from data_plane_name to its detail fields.
async fn fetch_data_plane_details(
    pg_pool: &sqlx::PgPool,
    names: &[String],
) -> async_graphql::Result<HashMap<String, DataPlaneDetails>> {
    tracing::debug!(count = names.len(), "loading data_plane details");

    let names_ref: Vec<&str> = names.iter().map(String::as_str).collect();

    let rows = sqlx::query!(
        r#"select
            dp.data_plane_name,
            dp.cidr_blocks::text[] as "cidr_blocks!: Vec<String>",
            dp.gcp_service_account_email,
            dp.aws_iam_user_arn,
            dp.azure_application_name,
            dp.azure_application_client_id,
            dp.aws_link_endpoints as "aws_link_endpoints: Vec<serde_json::Value>",
            dp.azure_link_endpoints as "azure_link_endpoints: Vec<serde_json::Value>",
            dp.gcp_psc_endpoints as "gcp_psc_endpoints: Vec<serde_json::Value>"
        from unnest($1::text[]) as input(name)
        join data_planes dp on dp.data_plane_name = input.name
        "#,
        &names_ref as &[&str],
    )
    .fetch_all(pg_pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            (
                row.data_plane_name,
                DataPlaneDetails {
                    cidr_blocks: row.cidr_blocks,
                    gcp_service_account_email: row.gcp_service_account_email,
                    aws_iam_user_arn: row.aws_iam_user_arn,
                    azure_application_name: row.azure_application_name,
                    azure_application_client_id: row.azure_application_client_id,
                    aws_link_endpoints: row.aws_link_endpoints.unwrap_or_default(),
                    azure_link_endpoints: row.azure_link_endpoints.unwrap_or_default(),
                    gcp_psc_endpoints: row.gcp_psc_endpoints.unwrap_or_default(),
                },
            )
        })
        .collect())
}

struct DataPlaneDetails {
    cidr_blocks: Vec<String>,
    gcp_service_account_email: Option<String>,
    aws_iam_user_arn: Option<String>,
    azure_application_name: Option<String>,
    azure_application_client_id: Option<String>,
    aws_link_endpoints: Vec<serde_json::Value>,
    azure_link_endpoints: Vec<serde_json::Value>,
    gcp_psc_endpoints: Vec<serde_json::Value>,
}

/// Parses a data plane name into its component parts.
/// Returns None if the name format is invalid.
///
/// Expected formats:
/// - Cloud: "ops/dp/public/aws-us-east-1-c1" or "ops/dp/private/gcp-us-central1-c2"
/// - Local: "ops/dp/local/local-foo" (any suffix after "local-")
pub(crate) fn parse_data_plane_name(
    name: &str,
) -> Option<(DataPlaneCloudProvider, String, String, bool)> {
    let last_segment = name.rsplit('/').next()?;
    let (provider_str, after_provider) = last_segment.split_once('-')?;

    match provider_str {
        "local" => Some((
            DataPlaneCloudProvider::Local,
            "local".to_string(),
            "c1".to_string(),
            true,
        )),
        "aws" | "az" | "azure" | "gcp" => {
            // Must have privacy indicator in path.
            if !name.contains("ops/dp/private/") && !name.starts_with("ops/dp/public/") {
                return None;
            }

            // Parse tag (cluster) suffix (e.g., "-c1", "-c5").
            let idx = after_provider.rfind("-c")?;
            let tag = &after_provider[idx + 1..];
            if tag.len() < 2 || !tag[1..].chars().all(|c| c.is_ascii_digit()) {
                return None;
            }

            let region = &after_provider[..idx];
            if region.is_empty() {
                return None;
            }

            let cloud_provider = match provider_str {
                "aws" => DataPlaneCloudProvider::Aws,
                "az" | "azure" => DataPlaneCloudProvider::Azure,
                "gcp" => DataPlaneCloudProvider::Gcp,
                _ => unreachable!(),
            };

            let is_public = name.starts_with("ops/dp/public/");
            Some((
                cloud_provider,
                region.to_string(),
                tag.to_string(),
                is_public,
            ))
        }
        _ => None,
    }
}

pub type PaginatedDataPlanes = Connection<
    String,
    DataPlane,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

#[derive(Debug, Default)]
pub struct DataPlanesQuery;

#[async_graphql::Object]
impl DataPlanesQuery {
    /// Returns data planes accessible to the current user.
    ///
    /// Results are paginated and sorted by data_plane_name.
    /// Only data planes the user has at least read capability to are returned.
    pub async fn data_planes(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> async_graphql::Result<PaginatedDataPlanes> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;
        let snapshot = env.snapshot();

        // Filter to only data planes the user can read and that have valid names.
        let accessible_data_planes: Vec<&tables::DataPlane> = snapshot
            .data_planes
            .iter()
            .filter(|dp| {
                if parse_data_plane_name(&dp.data_plane_name).is_none() {
                    tracing::warn!(data_plane_name = %dp.data_plane_name, "skipping data plane with unparseable name");
                    return false;
                }
                tables::UserGrant::is_authorized(
                        &snapshot.role_grants,
                        &snapshot.user_grants,
                        claims.sub,
                        &dp.data_plane_name,
                        models::Capability::Read,
                    )
            })
            .collect();

        // Apply cursor-based pagination.
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

                    // Sort by data_plane_name for consistent pagination.
                    let mut sorted: Vec<&tables::DataPlane> = accessible_data_planes.clone();
                    sorted.sort_by(|a, b| a.data_plane_name.cmp(&b.data_plane_name));

                    let result = if before.is_some() || last.is_some() {
                        // Backward pagination
                        let filtered: Vec<_> = sorted
                            .into_iter()
                            .filter(|dp| {
                                before
                                    .as_ref()
                                    .map(|b| dp.data_plane_name.as_str() < b.as_str())
                                    .unwrap_or(true)
                            })
                            .collect();

                        let total = filtered.len();
                        let skip = total.saturating_sub(limit);
                        let rows: Vec<_> = filtered.into_iter().skip(skip).collect();
                        let has_prev = skip > 0;
                        (rows, has_prev, before.is_some())
                    } else {
                        // Forward pagination
                        let rows: Vec<_> = sorted
                            .into_iter()
                            .filter(|dp| {
                                after
                                    .as_ref()
                                    .map(|a| dp.data_plane_name.as_str() > a.as_str())
                                    .unwrap_or(true)
                            })
                            .take(limit)
                            .collect();

                        let has_next = rows.len() == limit;
                        (rows, after.is_some(), has_next)
                    };

                    async_graphql::Result::Ok(result)
                },
            )
            .await?;

        // Preserve sorted order from pagination before moving into HashMap.
        let names: Vec<String> = rows.iter().map(|dp| dp.data_plane_name.clone()).collect();

        // Build row data map for attach_user_capabilities.
        let row_data: HashMap<String, &tables::DataPlane> = rows
            .into_iter()
            .map(|dp| (dp.data_plane_name.clone(), dp))
            .collect();

        // Fetch detail fields from the database for all data planes in this page.
        let details_map = fetch_data_plane_details(&env.pg_pool, &names).await?;

        let edges = crate::server::attach_user_capabilities(
            env.snapshot(),
            env.claims()?,
            names.into_iter(),
            |data_plane_name, user_capability| {
                let dp = row_data.get(&data_plane_name)?;
                let details = details_map.get(&data_plane_name);
                let (cloud_provider, region, tag, is_public) =
                    parse_data_plane_name(&data_plane_name).expect("name validated by pre-filter");
                let node = DataPlane {
                    name: data_plane_name.clone(),
                    fqdn: dp.data_plane_fqdn.clone(),
                    reactor_address: dp.reactor_address.clone(),
                    user_capability: user_capability.expect("capability guaranteed by pre-filter"),
                    cloud_provider,
                    region,
                    tag,
                    is_public,
                    cidr_blocks: details.map(|d| d.cidr_blocks.clone()).unwrap_or_default(),
                    gcp_service_account_email: details
                        .and_then(|d| d.gcp_service_account_email.clone()),
                    aws_iam_user_arn: details.and_then(|d| d.aws_iam_user_arn.clone()),
                    azure_application_name: details.and_then(|d| d.azure_application_name.clone()),
                    azure_application_client_id: details
                        .and_then(|d| d.azure_application_client_id.clone()),
                    control_id: dp.control_id,
                    raw_aws_link_endpoints: details
                        .map(|d| d.aws_link_endpoints.clone())
                        .unwrap_or_default(),
                    raw_azure_link_endpoints: details
                        .map(|d| d.azure_link_endpoints.clone())
                        .unwrap_or_default(),
                    raw_gcp_psc_endpoints: details
                        .map(|d| d.gcp_psc_endpoints.clone())
                        .unwrap_or_default(),
                };
                Some(connection::Edge::new(data_plane_name, node))
            },
        );

        let mut conn = PaginatedDataPlanes::new(has_prev, has_next);
        conn.edges = edges;
        Ok(conn)
    }
}

#[derive(Debug, Default)]
pub struct DataPlanesMutation;

/// Structural check: the name must sit under `ops/dp/private/` with at least
/// one path segment beyond it. Anything more specific (cluster suffix shape,
/// owning prefix shape) is the data plane's problem; an unknown but well-formed
/// name falls out as "not found" when no `data_planes` row matches.
fn require_private_dp_name(name: &str) -> async_graphql::Result<()> {
    if name
        .strip_prefix("ops/dp/private/")
        .is_none_or(|rest| !rest.contains('/') || rest.starts_with('/'))
    {
        return Err(async_graphql::Error::new(format!(
            "{name} is not a private data-plane name"
        )));
    }
    Ok(())
}

/// Maps a unique-violation on `(data_plane_id, service_identity)` to a clear
/// message; other database errors propagate unchanged.
fn map_link_db_error(err: sqlx::Error) -> async_graphql::Error {
    if let sqlx::Error::Database(db) = &err {
        if db.is_unique_violation() {
            return async_graphql::Error::new(
                "a private link with this service identity already exists on this data plane",
            );
        }
    }
    async_graphql::Error::new(err.to_string())
}

/// Resolves an id-addressed private link, authorizes the caller to modify it,
/// and returns the owning data-plane name. A link that does not exist and a link
/// the caller may not modify both return the same "not found" error, so an
/// unauthorized caller cannot probe which link ids exist. This deliberately uses
/// the visibility gate ([`super::may_access`]) rather than the hard gate
/// ([`super::verify_authorization`]) so a denial is hidden as not-found instead
/// of surfacing as a distinguishable permission-denied that names the data plane.
async fn resolve_modifiable_link(
    ctx: &Context<'_>,
    id: models::Id,
) -> async_graphql::Result<String> {
    let env = ctx.data::<crate::Envelope>()?;
    let not_found = || async_graphql::Error::new(format!("private link '{id}' not found"));

    let Some(row) = sqlx::query!(
        r#"
        SELECT dp.data_plane_name
        FROM internal.data_plane_private_links l
        JOIN data_planes dp ON dp.id = l.data_plane_id
        WHERE l.id = $1
        "#,
        id as models::Id,
    )
    .fetch_optional(&env.pg_pool)
    .await?
    else {
        return Err(not_found());
    };

    if !super::may_access(
        ctx,
        &row.data_plane_name,
        models::authz::Capability::ModifyDataPlanePrivateNetworking,
    )? {
        return Err(not_found());
    }

    Ok(row.data_plane_name)
}

#[async_graphql::Object]
impl DataPlanesMutation {
    /// Adds a private link to a private data plane. The data-plane controller
    /// converges to provision it on its next poll; the returned link starts
    /// `pending`. Requires `ModifyDataPlanePrivateNetworking` on the data plane.
    pub async fn add_data_plane_private_link(
        &self,
        ctx: &Context<'_>,
        data_plane_name: String,
        config: models::PrivateLink,
    ) -> async_graphql::Result<DataPlanePrivateLink> {
        let env = ctx.data::<crate::Envelope>()?;
        require_private_dp_name(&data_plane_name)?;
        super::verify_authorization(
            env,
            &data_plane_name,
            models::authz::Capability::ModifyDataPlanePrivateNetworking,
        )
        .await?;

        let row = sqlx::query!(
            r#"
            INSERT INTO internal.data_plane_private_links (data_plane_id, provider, config)
            SELECT dp.id, $2, $3
            FROM data_planes dp WHERE dp.data_plane_name = $1
            RETURNING
                id as "id: models::Id",
                status,
                details as "details: sqlx::types::Json<serde_json::Value>",
                error,
                observed_at as "observed_at: chrono::DateTime<chrono::Utc>"
            "#,
            data_plane_name,
            config.provider_str(),
            sqlx::types::Json(&config) as sqlx::types::Json<&models::PrivateLink>,
        )
        .fetch_optional(&env.pg_pool)
        .await
        .map_err(map_link_db_error)?;

        let Some(row) = row else {
            return Err(async_graphql::Error::new(format!(
                "data plane '{data_plane_name}' not found"
            )));
        };

        tracing::info!(%data_plane_name, link_id = %row.id, "added data plane private link");

        Ok(DataPlanePrivateLink {
            id: row.id,
            config,
            status: PrivateLinkProvisioningStatus::from_db(&row.status)?,
            details: row.details.map(|d| async_graphql::Json(d.0)),
            error: row.error,
            observed_at: row.observed_at,
        })
    }

    /// Replaces the configuration of an existing private link by id. Any edit
    /// resets the observed status to `pending` and re-triggers convergence: the
    /// desired-edit trigger clears the observation columns and bumps the link's
    /// internal generation, so a converge already in flight against the previous
    /// configuration cannot later stamp this link with a stale status. Requires
    /// `ModifyDataPlanePrivateNetworking` on the owning data plane.
    pub async fn update_data_plane_private_link(
        &self,
        ctx: &Context<'_>,
        id: models::Id,
        config: models::PrivateLink,
    ) -> async_graphql::Result<DataPlanePrivateLink> {
        let env = ctx.data::<crate::Envelope>()?;
        resolve_modifiable_link(ctx, id).await?;

        // Only the desired columns are set here; the desired-edit trigger resets
        // status/details/error/observed_at and bumps generation in the same
        // write, and `RETURNING` reflects those trigger-applied values.
        let row = sqlx::query!(
            r#"
            UPDATE internal.data_plane_private_links SET
                provider = $2,
                config = $3
            WHERE id = $1
            RETURNING
                status,
                details as "details: sqlx::types::Json<serde_json::Value>",
                error,
                observed_at as "observed_at: chrono::DateTime<chrono::Utc>"
            "#,
            id as models::Id,
            config.provider_str(),
            sqlx::types::Json(&config) as sqlx::types::Json<&models::PrivateLink>,
        )
        .fetch_optional(&env.pg_pool)
        .await
        .map_err(map_link_db_error)?
        // The row was authorized by `resolve_modifiable_link` above, but a
        // concurrent remove (or a cascading data-plane teardown) can delete it
        // before this UPDATE runs. Report the same existence-hiding not-found
        // rather than leaking a raw "no rows returned" sqlx error.
        .ok_or_else(|| async_graphql::Error::new(format!("private link '{id}' not found")))?;

        Ok(DataPlanePrivateLink {
            id,
            config,
            status: PrivateLinkProvisioningStatus::from_db(&row.status)?,
            details: row.details.map(|d| async_graphql::Json(d.0)),
            error: row.error,
            observed_at: row.observed_at,
        })
    }

    /// Removes a private link by id. The controller tears down its endpoint on
    /// the next converge. Requires `ModifyDataPlanePrivateNetworking` on the
    /// owning data plane. Returns the removed link id.
    pub async fn remove_data_plane_private_link(
        &self,
        ctx: &Context<'_>,
        id: models::Id,
    ) -> async_graphql::Result<models::Id> {
        let env = ctx.data::<crate::Envelope>()?;
        let data_plane_name = resolve_modifiable_link(ctx, id).await?;

        _ = sqlx::query!(
            "DELETE FROM internal.data_plane_private_links WHERE id = $1",
            id as models::Id,
        )
        .execute(&env.pg_pool)
        .await?;

        tracing::info!(link_id = %id, %data_plane_name, "removed data plane private link");
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_server;

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_graphql_data_planes(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server =
            test_server::TestServer::start(pool.clone(), test_server::snapshot(pool, false).await)
                .await;

        let token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        dataPlanes {
                            edges {
                                node {
                                    name
                                    fqdn
                                    reactorAddress
                                    cloudProvider
                                    region
                                    tag
                                    isPublic
                                    userCapability
                                    cidrBlocks
                                    gcpServiceAccountEmail
                                    awsIamUserArn
                                    azureApplicationName
                                    azureApplicationClientId
                                }
                            }
                        }
                    }
                "#
                }),
                Some(&token),
            )
            .await;

        insta::assert_json_snapshot!(response);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../fixtures",
            scripts("data_planes", "alice", "private_links")
        )
    )]
    async fn test_graphql_data_planes_with_private_links(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server =
            test_server::TestServer::start(pool.clone(), test_server::snapshot(pool, false).await)
                .await;

        let token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        // The private fixture grants Alice `read` on
        // ops/dp/private/aliceCo/aws-us-east-1-c1 and populates one entry of
        // each private-link variant plus a single AWS provisioning result.
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        dataPlanes {
                            edges {
                                node {
                                    name
                                    awsLinkEndpoints
                                    azureLinkEndpoints
                                    gcpPscEndpoints
                                    privateLinks {
                                        id
                                        status
                                        details
                                        config {
                                            __typename
                                            ... on AWSPrivateLink { region azIds serviceName }
                                            ... on AzurePrivateLink { serviceName location dnsName resourceType }
                                            ... on GCPPrivateServiceConnect { serviceAttachment region dnsZoneName dnsRecordNames allPorts }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    "#
                }),
                Some(&token),
            )
            .await;

        insta::assert_json_snapshot!("data_planes_with_private_links", response);
    }

    // A malformed config must cause the `privateLinks` resolver to return a
    // GraphQL error, rather than silently omit a row the caller may view.
    // The migration validates rows present at backfill time, but support
    // and other internal writers can edit this unvalidated JSONB column later.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../fixtures",
            scripts("data_planes", "alice", "private_links")
        )
    )]
    async fn test_graphql_data_planes_malformed_private_link(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        // Retain the service identity while removing the other required AWS
        // fields, producing a row that matches no `models::PrivateLink` variant.
        sqlx::query(
            r#"UPDATE internal.data_plane_private_links
               SET config = '{"service_name":"com.amazonaws.vpce.us-east-1.vpce-svc-malformed"}'::jsonb
               WHERE id = '00:00:00:00:00:00:0a:01'"#,
        )
        .execute(&pool)
        .await
        .unwrap();

        let server =
            test_server::TestServer::start(pool.clone(), test_server::snapshot(pool, false).await)
                .await;
        let token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        dataPlanes {
                            edges {
                                node {
                                    name
                                    privateLinks { id }
                                }
                            }
                        }
                    }
                    "#
                }),
                Some(&token),
            )
            .await;

        assert!(
            response["data"].is_null(),
            "expected fail-closed data: {response}"
        );
        insta::assert_json_snapshot!("data_planes_malformed_private_link", response);
    }

    // Existing tenants can still view their private data plane's private links
    // even before the `manage_data_plane` backfill runs, which is what later
    // adds the ability to modify them. Clearing the bundle reproduces that
    // pre-backfill state: the links stay readable, the update mutation is denied.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../fixtures",
            scripts("data_planes", "alice", "private_links")
        )
    )]
    async fn test_modify_denied_when_role_grant_lacks_manage_data_plane(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        // Strip the `manage_data_plane` bundle from the only edge carrying
        // Alice to the private dp, leaving its legacy `read` untouched.
        sqlx::query(
            r#"UPDATE role_grants
               SET bundles = '{}'
               WHERE subject_role = 'aliceCo/'
                 AND object_role = 'ops/dp/private/aliceCo/'"#,
        )
        .execute(&pool)
        .await
        .unwrap();

        let server =
            test_server::TestServer::start(pool.clone(), test_server::snapshot(pool, false).await)
                .await;
        let alice_token = server.make_access_token(
            uuid::Uuid::from_bytes([0x11; 16]),
            Some("alice@example.test"),
        );

        let dp = "ops/dp/private/aliceCo/aws-us-east-1-c1";

        // View still resolves: `read` -> Viewer -> ViewDataPlanePrivateNetworking
        // does not depend on the cleared bundle.
        let view: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        dataPlanes {
                            edges { node { name privateLinks { __typename } } }
                        }
                    }
                    "#
                }),
                Some(&alice_token),
            )
            .await;
        let edges = view["data"]["dataPlanes"]["edges"]
            .as_array()
            .expect("should have edges");
        let private_dp = edges
            .iter()
            .find(|e| e["node"]["name"] == dp)
            .expect("alice should still see the private dp via her read edge");
        assert_eq!(
            private_dp["node"]["privateLinks"].as_array().unwrap().len(),
            3,
            "read must still grant view after the manage_data_plane bundle is cleared: {private_dp}",
        );

        // Modify is denied: ModifyDataPlanePrivateNetworking flowed only
        // through the now-cleared `manage_data_plane` bundle on the edge.
        let denied: serde_json::Value = server
            .graphql(&add_mutation(dp, VALID_AWS_INPUT), Some(&alice_token))
            .await;
        assert_eq!(
            first_error_message(&denied),
            "PermissionDenied: alice@example.test is not authorized to access prefix or name 'ops/dp/private/aliceCo/aws-us-east-1-c1' with required capability ModifyDataPlanePrivateNetworking",
        );
    }

    // ===== per-link CRUD mutation tests =====

    // The `*_INPUT` constants are `PrivateLinkConfigInput` @oneOf values. The
    // AWS one matches the fixture's existing AWS link (used to exercise the
    // duplicate-identity guard); `NEW_AWS_INPUT` is a distinct link to add.
    const VALID_AWS_INPUT: &str = r#"{
        "aws": {
            "region": "us-east-1",
            "azIds": ["use1-az1", "use1-az2"],
            "serviceName": "com.amazonaws.vpce.us-east-1.vpce-svc-abc123"
        }
    }"#;
    const NEW_AWS_INPUT: &str = r#"{
        "aws": {
            "region": "us-east-1",
            "azIds": ["use1-az1"],
            "serviceName": "com.amazonaws.vpce.us-east-1.vpce-svc-new999"
        }
    }"#;

    fn add_mutation(name: &str, config_json: &str) -> serde_json::Value {
        serde_json::json!({
            "query": r#"
            mutation($name: String!, $config: PrivateLinkConfigInput!) {
                addDataPlanePrivateLink(dataPlaneName: $name, config: $config) {
                    id
                    status
                    config {
                        __typename
                        ... on AWSPrivateLink { serviceName }
                        ... on AzurePrivateLink { serviceName }
                        ... on GCPPrivateServiceConnect { serviceAttachment }
                    }
                }
            }"#,
            "variables": {
                "name": name,
                "config": serde_json::from_str::<serde_json::Value>(config_json).unwrap(),
            }
        })
    }

    fn update_link_mutation(id: &str, config_json: &str) -> serde_json::Value {
        serde_json::json!({
            "query": r#"
            mutation($id: Id!, $config: PrivateLinkConfigInput!) {
                updateDataPlanePrivateLink(id: $id, config: $config) {
                    id status config { __typename ... on AWSPrivateLink { serviceName } }
                }
            }"#,
            "variables": {
                "id": id,
                "config": serde_json::from_str::<serde_json::Value>(config_json).unwrap(),
            }
        })
    }

    fn remove_link_mutation(id: &str) -> serde_json::Value {
        serde_json::json!({
            "query": r#"
            mutation($id: Id!) { removeDataPlanePrivateLink(id: $id) }"#,
            "variables": { "id": id }
        })
    }

    /// Extracts the first error message from a GraphQL response, or panics
    /// if the response did not return an error.
    fn first_error_message(response: &serde_json::Value) -> &str {
        response["errors"][0]["message"]
            .as_str()
            .unwrap_or_else(|| panic!("expected an error, got: {response}"))
    }

    async fn count_links(pool: &sqlx::PgPool, dp: &str) -> i64 {
        sqlx::query_scalar(
            r#"SELECT count(*) FROM internal.data_plane_private_links l
               JOIN data_planes dp ON dp.id = l.data_plane_id
               WHERE dp.data_plane_name = $1"#,
        )
        .bind(dp)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../fixtures",
            scripts("data_planes", "alice", "private_links")
        )
    )]
    async fn test_add_private_link(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let alice_token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);
        let dp = "ops/dp/private/aliceCo/aws-us-east-1-c1";

        // A new link is created `pending` (no endpoint provisioned yet) as a
        // fourth row alongside the three from the fixture.
        let added: serde_json::Value = server
            .graphql(&add_mutation(dp, NEW_AWS_INPUT), Some(&alice_token))
            .await;
        let link = &added["data"]["addDataPlanePrivateLink"];
        assert_eq!(
            link["config"]["__typename"], "AWSPrivateLink",
            "got: {added}"
        );
        assert_eq!(link["status"], "PENDING");
        assert_eq!(
            link["config"]["serviceName"],
            "com.amazonaws.vpce.us-east-1.vpce-svc-new999"
        );
        assert!(link["id"].is_string());
        assert_eq!(count_links(&pool, dp).await, 4);

        // Adding a link whose service identity already exists on the data plane
        // is rejected by the unique constraint.
        let dup: serde_json::Value = server
            .graphql(&add_mutation(dp, VALID_AWS_INPUT), Some(&alice_token))
            .await;
        assert_eq!(
            first_error_message(&dup),
            "a private link with this service identity already exists on this data plane",
        );
        assert_eq!(count_links(&pool, dp).await, 4);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../fixtures",
            scripts("data_planes", "alice", "private_links")
        )
    )]
    async fn test_update_and_remove_private_link(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let alice_token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);
        let dp = "ops/dp/private/aliceCo/aws-us-east-1-c1";

        // The fixture's AWS link id; it starts `provisioned`. Replacing its
        // config resets the observed status to `pending`.
        let aws_id = "0000000000000a01";
        let updated: serde_json::Value = server
            .graphql(
                &update_link_mutation(aws_id, NEW_AWS_INPUT),
                Some(&alice_token),
            )
            .await;
        let link = &updated["data"]["updateDataPlanePrivateLink"];
        assert_eq!(link["id"], aws_id, "got: {updated}");
        assert_eq!(link["status"], "PENDING");
        assert_eq!(
            link["config"]["serviceName"],
            "com.amazonaws.vpce.us-east-1.vpce-svc-new999"
        );

        // Editing a link that is already `pending` is allowed: it replaces the
        // config and stays `pending` for the next converge. The desired-edit
        // trigger bumps the link's generation on this write, which is what keeps
        // a converge racing the earlier edit from stamping a stale status.
        let reupdated: serde_json::Value = server
            .graphql(
                &update_link_mutation(aws_id, VALID_AWS_INPUT),
                Some(&alice_token),
            )
            .await;
        let link = &reupdated["data"]["updateDataPlanePrivateLink"];
        assert_eq!(link["id"], aws_id, "got: {reupdated}");
        assert_eq!(link["status"], "PENDING");
        assert_eq!(
            link["config"]["serviceName"],
            "com.amazonaws.vpce.us-east-1.vpce-svc-abc123"
        );

        // Removing a link is allowed in any status, `pending` included; it
        // returns the removed id and drops the row.
        let removed: serde_json::Value = server
            .graphql(&remove_link_mutation(aws_id), Some(&alice_token))
            .await;
        assert_eq!(
            removed["data"]["removeDataPlanePrivateLink"], aws_id,
            "got: {removed}"
        );
        assert_eq!(count_links(&pool, dp).await, 2);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../fixtures",
            scripts("data_planes", "alice", "private_links")
        )
    )]
    async fn test_private_link_mutation_authorization(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        // bob has no grants on the private dp.
        sqlx::query(
            "INSERT INTO auth.users (id, email) VALUES \
             ('22222222-2222-2222-2222-222222222222', 'bob@example.test')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let alice_token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);
        let bob_token =
            server.make_access_token(uuid::Uuid::from_bytes([0x22; 16]), Some("bob@example.test"));
        let dp = "ops/dp/private/aliceCo/aws-us-east-1-c1";

        // Alice (read + manage_data_plane bundle) can add.
        let alice_ok: serde_json::Value = server
            .graphql(&add_mutation(dp, NEW_AWS_INPUT), Some(&alice_token))
            .await;
        assert_eq!(
            alice_ok["data"]["addDataPlanePrivateLink"]["config"]["__typename"], "AWSPrivateLink",
            "got: {alice_ok}"
        );

        // Bob is rejected for lacking ModifyDataPlanePrivateNetworking. The
        // name-addressed `add` openly names the prefix, because the caller
        // supplied the name and so reveals nothing they did not already know.
        let bob_denied: serde_json::Value = server
            .graphql(&add_mutation(dp, NEW_AWS_INPUT), Some(&bob_token))
            .await;
        assert_eq!(
            first_error_message(&bob_denied),
            "PermissionDenied: bob@example.test is not authorized to access prefix or name 'ops/dp/private/aliceCo/aws-us-east-1-c1' with required capability ModifyDataPlanePrivateNetworking",
        );

        // An id-addressed mutation on a link Bob may not modify must return the
        // same "not found" as a missing id, never a permission error that would
        // confirm the link (or its data plane) exists. `0000000000000a01` is the
        // fixture's existing AWS link.
        let aws_id = "0000000000000a01";
        for probe in [
            update_link_mutation(aws_id, NEW_AWS_INPUT),
            remove_link_mutation(aws_id),
        ] {
            let response: serde_json::Value = server.graphql(&probe, Some(&bob_token)).await;
            let message = first_error_message(&response);
            assert!(
                message.contains("not found") && !message.contains("PermissionDenied"),
                "expected an existence-hiding not-found error, got: {response}"
            );
        }

        // Bob's denied remove did not actually delete: Alice's added link plus
        // the three from the fixture remain.
        assert_eq!(count_links(&pool, dp).await, 4);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../fixtures",
            scripts("data_planes", "alice", "private_links")
        )
    )]
    async fn test_add_private_link_name_validation(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let server =
            test_server::TestServer::start(pool.clone(), test_server::snapshot(pool, false).await)
                .await;
        let alice_token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        // Names outside `ops/dp/private/<tenant>/...` are rejected before any
        // auth or DB work.
        let cases: &[&str] = &[
            "ops/dp/public/aws-us-west-2-c1",
            "ops/dp/private/aws-us-east-1-c1",
        ];
        for name in cases {
            let response: serde_json::Value = server
                .graphql(&add_mutation(name, NEW_AWS_INPUT), Some(&alice_token))
                .await;
            assert_eq!(
                first_error_message(&response),
                format!("{name} is not a private data-plane name"),
                "case: {name}",
            );
        }

        // A well-formed name alice is authorized for but with no matching
        // data_planes row reports not-found.
        let response: serde_json::Value = server
            .graphql(
                &add_mutation("ops/dp/private/aliceCo/aws-us-east-2-c9", NEW_AWS_INPUT),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            first_error_message(&response),
            "data plane 'ops/dp/private/aliceCo/aws-us-east-2-c9' not found",
        );
    }

    #[test]
    fn parses_aws_public() {
        let (provider, region, tag, is_public) =
            parse_data_plane_name("ops/dp/public/aws-us-east-1-c1").unwrap();
        assert_eq!(provider, DataPlaneCloudProvider::Aws);
        assert_eq!(region, "us-east-1");
        assert_eq!(tag, "c1");
        assert!(is_public);
    }

    #[test]
    fn parses_gcp_private() {
        let (provider, region, tag, is_public) =
            parse_data_plane_name("ops/dp/private/estuary/gcp-us-central1-c5").unwrap();
        assert_eq!(provider, DataPlaneCloudProvider::Gcp);
        assert_eq!(region, "us-central1");
        assert_eq!(tag, "c5");
        assert!(!is_public);
    }

    #[test]
    fn parses_azure_variants() {
        // "az" prefix
        let (provider, region, tag, _) =
            parse_data_plane_name("ops/dp/private/EastPack/az-australiaeast-c1").unwrap();
        assert_eq!(provider, DataPlaneCloudProvider::Azure);
        assert_eq!(region, "australiaeast");
        assert_eq!(tag, "c1");

        // "azure" prefix
        let (provider, region, tag, _) =
            parse_data_plane_name("ops/dp/private/AccumTech/azure-eastus-c1").unwrap();
        assert_eq!(provider, DataPlaneCloudProvider::Azure);
        assert_eq!(region, "eastus");
        assert_eq!(tag, "c1");
    }

    #[test]
    fn parses_local() {
        let (provider, region, tag, is_public) =
            parse_data_plane_name("ops/dp/local/local-foo").unwrap();
        assert_eq!(provider, DataPlaneCloudProvider::Local);
        assert_eq!(region, "local");
        assert_eq!(tag, "c1");
        assert!(is_public);
    }

    #[test]
    fn rejects_invalid_names() {
        // Missing privacy indicator
        assert!(parse_data_plane_name("ops/dp/aws-us-east-1-c1").is_none());
        // Unknown provider
        assert!(parse_data_plane_name("ops/dp/public/unknown-us-east-1-c1").is_none());
        // Missing cluster suffix
        assert!(parse_data_plane_name("ops/dp/public/aws-us-east-1").is_none());
        // Non-numeric cluster
        assert!(parse_data_plane_name("ops/dp/public/aws-us-east-1-ca").is_none());
    }
}
