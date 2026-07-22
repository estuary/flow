use super::filters;
use async_graphql::{
    ComplexObject, Context, SimpleObject,
    types::connection::{self, Connection},
};
use std::collections::HashMap;

const DEFAULT_PAGE_SIZE: usize = 50;

/// Optional filter for the `dataPlanes` query. When omitted, all accessible
/// data planes are returned.
#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct DataPlanesFilter {
    /// Filter on the `closed` flag.
    pub closed: Option<filters::BoolFilter>,
}

/// Cloud provider where the data plane is hosted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, async_graphql::Enum)]
pub enum DataPlaneCloudProvider {
    Aws,
    Azure,
    Gcp,
    Local,
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
    /// Whether this data plane is closed to new selection.
    pub closed: bool,
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
    // The four private-networking fields below are gated behind
    // `ViewDataPlanePrivateNetworking` and resolved by `ComplexObject` methods.
    // They are stored as raw JSON and skipped from the derived object so the
    // capability check lives with the field rather than the construction site;
    // see the resolvers below.
    #[graphql(skip)]
    raw_private_links: Vec<serde_json::Value>,
    #[graphql(skip)]
    raw_aws_link_endpoints: Vec<serde_json::Value>,
    #[graphql(skip)]
    raw_azure_link_endpoints: Vec<serde_json::Value>,
    #[graphql(skip)]
    raw_gcp_psc_endpoints: Vec<serde_json::Value>,
}

#[ComplexObject]
impl DataPlane {
    /// Configured private link endpoints for this data-plane. Replacing this
    /// list (via `updateDataPlanePrivateLinks`) triggers reconvergence by the
    /// data-plane controller on its next poll. Returns an empty list to
    /// callers that lack the `ViewDataPlanePrivateNetworking` capability on
    /// this data plane.
    async fn private_links(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Vec<models::PrivateLink>> {
        if !super::may_access(
            ctx,
            &self.name,
            models::authz::Capability::ViewDataPlanePrivateNetworking,
        )? {
            return Ok(Vec::new());
        }
        self.raw_private_links
            .iter()
            .enumerate()
            .map(|(idx, raw)| {
                serde_json::from_value::<models::PrivateLink>(raw.clone()).map_err(|err| {
                    async_graphql::Error::new(format!(
                        "failed to parse private_links[{idx}] for data plane {}: {err}",
                        self.name,
                    ))
                })
            })
            .collect()
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
            dp.private_links as "private_links!: Vec<serde_json::Value>",
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
                    private_links: row.private_links,
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
    private_links: Vec<serde_json::Value>,
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

/// A public data plane, as visible to unauthenticated callers.
#[derive(Debug, Clone, SimpleObject)]
pub struct PublicDataPlane {
    /// Name of this data-plane under the catalog namespace.
    pub name: String,
    /// Cloud provider where this data-plane is hosted.
    pub cloud_provider: DataPlaneCloudProvider,
    /// Cloud region where this data-plane is hosted.
    /// For example: "us-east-1" (AWS), "us-central1" (GCP), "eastus" (Azure).
    pub region: String,
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

pub type PaginatedPublicDataPlanes = Connection<
    String,
    PublicDataPlane,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

/// Applies cursor pagination over `sorted`, which must already be ordered by
/// the unique `name` key that also serves as the cursor. Returns the page of
/// rows plus has-previous-page / has-next-page indicators.
fn paginate_by_name<T>(
    sorted: Vec<T>,
    name: impl Fn(&T) -> &str,
    after: Option<String>,
    before: Option<String>,
    first: Option<usize>,
    last: Option<usize>,
) -> (Vec<T>, bool, bool) {
    let limit = first.or(last).unwrap_or(DEFAULT_PAGE_SIZE);
    if limit == 0 {
        return (Vec::new(), false, false);
    }

    if before.is_some() || last.is_some() {
        // Backward pagination
        let filtered: Vec<T> = sorted
            .into_iter()
            .filter(|t| {
                before
                    .as_ref()
                    .map(|b| name(t) < b.as_str())
                    .unwrap_or(true)
            })
            .collect();

        let total = filtered.len();
        let skip = total.saturating_sub(limit);
        let rows: Vec<T> = filtered.into_iter().skip(skip).collect();
        (rows, skip > 0, before.is_some())
    } else {
        // Forward pagination
        let rows: Vec<T> = sorted
            .into_iter()
            .filter(|t| after.as_ref().map(|a| name(t) > a.as_str()).unwrap_or(true))
            .take(limit)
            .collect();

        let has_next = rows.len() == limit;
        (rows, after.is_some(), has_next)
    }
}

#[derive(Debug, Default)]
pub struct DataPlanesQuery;

#[async_graphql::Object]
impl DataPlanesQuery {
    /// Returns data planes accessible to the current user.
    ///
    /// Results are paginated and sorted by data_plane_name.
    /// Only data planes the user has at least read capability to are returned.
    ///
    /// `filter.closed.eq` restricts results to data planes whose `closed`
    /// flag matches it; omitting it returns both open and closed planes.
    pub async fn data_planes(
        &self,
        ctx: &Context<'_>,
        filter: Option<DataPlanesFilter>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> async_graphql::Result<PaginatedDataPlanes> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;
        let snapshot = env.snapshot();

        let closed_eq = filter
            .as_ref()
            .and_then(|f| f.closed.as_ref())
            .and_then(|f| f.eq);

        // Filter to only data planes the user can read and that have valid
        // names, sorted by data_plane_name for consistent pagination.
        let mut accessible_data_planes: Vec<&tables::DataPlane> = snapshot
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
        accessible_data_planes.sort_by(|a, b| a.data_plane_name.cmp(&b.data_plane_name));

        // Narrow to the requested `closed` value before pagination, so page
        // sizes and cursors stay correct. The flag rides along in the authz
        // snapshot, so this needs no database round-trip.
        if let Some(want_closed) = closed_eq {
            accessible_data_planes.retain(|dp| dp.closed == want_closed);
        }

        // Apply cursor-based pagination.
        let (rows, has_prev, has_next) =
            connection::query_with::<String, _, _, _, async_graphql::Error>(
                after,
                before,
                first,
                last,
                |after, before, first, last| async move {
                    Ok(paginate_by_name(
                        accessible_data_planes,
                        |dp| dp.data_plane_name.as_str(),
                        after,
                        before,
                        first,
                        last,
                    ))
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
                    closed: dp.closed,
                    cidr_blocks: details.map(|d| d.cidr_blocks.clone()).unwrap_or_default(),
                    gcp_service_account_email: details
                        .and_then(|d| d.gcp_service_account_email.clone()),
                    aws_iam_user_arn: details.and_then(|d| d.aws_iam_user_arn.clone()),
                    azure_application_name: details.and_then(|d| d.azure_application_name.clone()),
                    azure_application_client_id: details
                        .and_then(|d| d.azure_application_client_id.clone()),
                    raw_private_links: details.map(|d| d.private_links.clone()).unwrap_or_default(),
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

    /// Returns all public data planes.
    ///
    /// This query requires no authentication. It exposes only the name, cloud
    /// provider, and region of public data planes, so that account-creation
    /// flows can offer a data-plane selection before the user has signed up.
    /// Closed planes are always excluded: this query drives new selection,
    /// which is exactly what closing a plane retires it from.
    ///
    /// Results are paginated and sorted by name.
    pub async fn public_data_planes(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> async_graphql::Result<PaginatedPublicDataPlanes> {
        let env = ctx.data::<crate::Envelope>()?;

        let mut planes: Vec<PublicDataPlane> = env
            .snapshot()
            .data_planes
            .iter()
            .filter_map(|dp| {
                let (cloud_provider, region, _tag, is_public) =
                    parse_data_plane_name(&dp.data_plane_name)?;
                (is_public && !dp.closed).then(|| PublicDataPlane {
                    name: dp.data_plane_name.clone(),
                    cloud_provider,
                    region,
                })
            })
            .collect();
        planes.sort_by(|a, b| a.name.cmp(&b.name));

        let (rows, has_prev, has_next) =
            connection::query_with::<String, _, _, _, async_graphql::Error>(
                after,
                before,
                first,
                last,
                |after, before, first, last| async move {
                    Ok(paginate_by_name(
                        planes,
                        |p| p.name.as_str(),
                        after,
                        before,
                        first,
                        last,
                    ))
                },
            )
            .await?;

        let mut conn = PaginatedPublicDataPlanes::new(has_prev, has_next);
        conn.edges = rows
            .into_iter()
            .map(|p| connection::Edge::new(p.name.clone(), p))
            .collect();
        Ok(conn)
    }
}

#[derive(Debug, Default)]
pub struct DataPlanesMutation;

#[async_graphql::Object]
impl DataPlanesMutation {
    /// Replaces the configured private link endpoints on a private data plane.
    ///
    /// The provided list overwrites the entire `private_links` column; partial
    /// updates are intentionally not supported. The data-plane controller
    /// converges to the new configuration on its next poll. Returns the desired
    /// private links state. The `*LinkEndpoints` provisioning results are not echoed here:
    /// they lag this write until the controller converges, so callers needing them re-query `dataPlanes`.
    ///
    /// Requires the `ModifyDataPlanePrivateNetworking` capability on the
    /// private data-plane name.
    pub async fn update_data_plane_private_links(
        &self,
        ctx: &Context<'_>,
        data_plane_name: String,
        private_links: Vec<models::PrivateLink>,
    ) -> async_graphql::Result<Vec<models::PrivateLink>> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        // Structural check only: the name must sit under `ops/dp/private/` and
        // have at least one path segment beyond it. Anything more specific
        // (cluster suffix shape, owning prefix shape) is the data plane's
        // problem, not the mutation's; an unknown name falls out as "not
        // found" when the UPDATE matches zero rows.
        if data_plane_name
            .strip_prefix("ops/dp/private/")
            .is_none_or(|rest| !rest.contains('/') || rest.starts_with('/'))
        {
            return Err(async_graphql::Error::new(format!(
                "{data_plane_name} is not a private data-plane name"
            )));
        }

        super::verify_authorization(
            env,
            &data_plane_name,
            models::authz::Capability::ModifyDataPlanePrivateNetworking,
        )
        .await?;

        let bound: Vec<sqlx::types::Json<&models::PrivateLink>> =
            private_links.iter().map(sqlx::types::Json).collect();
        let row = sqlx::query!(
            r#"UPDATE data_planes
               SET private_links = $2, updated_at = now()
               WHERE data_plane_name = $1
               RETURNING private_links as "private_links!: Vec<serde_json::Value>"
            "#,
            data_plane_name,
            &bound as &[sqlx::types::Json<&models::PrivateLink>],
        )
        .fetch_optional(&env.pg_pool)
        .await?;

        let Some(row) = row else {
            return Err(async_graphql::Error::new(format!(
                "data plane '{data_plane_name}' not found"
            )));
        };

        tracing::info!(
            %data_plane_name,
            link_count = row.private_links.len(),
            %claims.sub,
            "updated data plane private links",
        );

        row.private_links
            .into_iter()
            .map(serde_json::from_value::<models::PrivateLink>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| {
                async_graphql::Error::new(format!(
                    "stored private_links for {data_plane_name} did not round-trip: {err}"
                ))
            })
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

    // `publicDataPlanes` serves the pre-signup account-creation flow, so it
    // must answer unauthenticated requests with public data planes only, and
    // must not loosen authentication of the full `dataPlanes` query.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../fixtures",
            scripts("data_planes", "alice", "private_links")
        )
    )]
    async fn test_graphql_public_data_planes_unauthenticated(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server =
            test_server::TestServer::start(pool.clone(), test_server::snapshot(pool, false).await)
                .await;

        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        publicDataPlanes {
                            pageInfo { hasPreviousPage hasNextPage }
                            edges {
                                cursor
                                node {
                                    name
                                    cloudProvider
                                    region
                                }
                            }
                        }
                    }
                    "#
                }),
                None,
            )
            .await;

        insta::assert_json_snapshot!("public_data_planes_unauthenticated", response);

        // Paginate through the two fixture planes one at a time.
        let paged_query = r#"
            query($after: String) {
                publicDataPlanes(first: 1, after: $after) {
                    pageInfo { hasNextPage endCursor }
                    edges { node { name } }
                }
            }
        "#;
        let page_one: serde_json::Value = server
            .graphql(
                &serde_json::json!({"query": paged_query, "variables": {"after": null}}),
                None,
            )
            .await;
        insta::assert_json_snapshot!("public_data_planes_page_one", page_one);

        let end_cursor = page_one["data"]["publicDataPlanes"]["pageInfo"]["endCursor"]
            .as_str()
            .expect("page one must have an end cursor");
        let page_two: serde_json::Value = server
            .graphql(
                &serde_json::json!({"query": paged_query, "variables": {"after": end_cursor}}),
                None,
            )
            .await;
        insta::assert_json_snapshot!("public_data_planes_page_two", page_two);

        // The full `dataPlanes` query remains authenticated.
        let denied: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        dataPlanes {
                            edges { node { name } }
                        }
                    }
                    "#
                }),
                None,
            )
            .await;
        assert!(
            first_error_message(&denied)
                .contains("the request is missing a required Authorization: Bearer token"),
            "expected an unauthenticated error, got: {denied}",
        );
    }

    // `publicDataPlanes` drives pre-signup selection, so a closed plane must
    // never appear. Closing one of the two public fixture planes must drop it
    // while the other remains.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_graphql_public_data_planes_excludes_closed(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let closed_dp = "ops/dp/public/gcp-us-central1-c2";
        let open_dp = "ops/dp/public/aws-us-west-2-c1";
        sqlx::query("UPDATE data_planes SET closed = true WHERE data_plane_name = $1")
            .bind(closed_dp)
            .execute(&pool)
            .await
            .unwrap();

        // The snapshot is built after the update, so it captures closed = true.
        let server =
            test_server::TestServer::start(pool.clone(), test_server::snapshot(pool, false).await)
                .await;

        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        publicDataPlanes {
                            edges { node { name } }
                        }
                    }
                    "#
                }),
                None,
            )
            .await;

        let names: Vec<&str> = response["data"]["publicDataPlanes"]["edges"]
            .as_array()
            .unwrap_or_else(|| panic!("expected edges, got: {response}"))
            .iter()
            .map(|e| e["node"]["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec![open_dp], "closed public plane must be excluded");
    }

    // The `filter.closed.eq` argument narrows the listing to matching planes,
    // while omitting the filter returns both open and closed planes. The
    // `data_planes` fixture leaves both public planes open by default; closing
    // one exercises all three cases.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_graphql_data_planes_closed_filter(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let closed_dp = "ops/dp/public/gcp-us-central1-c2";
        let open_dp = "ops/dp/public/aws-us-west-2-c1";
        sqlx::query("UPDATE data_planes SET closed = true WHERE data_plane_name = $1")
            .bind(closed_dp)
            .execute(&pool)
            .await
            .unwrap();

        let server =
            test_server::TestServer::start(pool.clone(), test_server::snapshot(pool, false).await)
                .await;
        let token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        // Collects the sorted set of returned data-plane names for the given
        // `filter` argument (JSON null when omitted).
        async fn names(
            server: &test_server::TestServer,
            token: &str,
            filter: serde_json::Value,
        ) -> Vec<String> {
            let response: serde_json::Value = server
                .graphql(
                    &serde_json::json!({
                        "query": r#"
                        query($filter: DataPlanesFilter) {
                            dataPlanes(filter: $filter) {
                                edges { node { name closed } }
                            }
                        }
                        "#,
                        "variables": { "filter": filter },
                    }),
                    Some(token),
                )
                .await;
            let mut names: Vec<String> = response["data"]["dataPlanes"]["edges"]
                .as_array()
                .unwrap_or_else(|| panic!("expected edges, got: {response}"))
                .iter()
                .map(|e| e["node"]["name"].as_str().unwrap().to_string())
                .collect();
            names.sort();
            names
        }

        // No filter: both planes, regardless of `closed`.
        assert_eq!(
            names(&server, &token, serde_json::Value::Null).await,
            vec![open_dp.to_string(), closed_dp.to_string()],
        );
        // closed eq false -> only the open plane.
        assert_eq!(
            names(
                &server,
                &token,
                serde_json::json!({ "closed": { "eq": false } })
            )
            .await,
            vec![open_dp.to_string()],
        );
        // closed eq true -> only the closed plane.
        assert_eq!(
            names(
                &server,
                &token,
                serde_json::json!({ "closed": { "eq": true } })
            )
            .await,
            vec![closed_dp.to_string()],
        );
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
                                        __typename
                                        ... on AWSPrivateLink {
                                            region
                                            azIds
                                            serviceName
                                        }
                                        ... on AzurePrivateLink {
                                            serviceName
                                            location
                                            dnsName
                                            resourceType
                                        }
                                        ... on GCPPrivateServiceConnect {
                                            serviceAttachment
                                            region
                                            dnsZoneName
                                            dnsRecordNames
                                            allPorts
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

    // A caller with only legacy `read` on the DP prefix can view the
    // private-networking fields (the `Viewer` bundle carries
    // `ViewDataPlanePrivateNetworking`, because `read` on a data-plane
    // prefix already conveys deploy-level trust) but cannot mutate them:
    // `ModifyDataPlanePrivateNetworking` only comes via the separately
    // granted `ManageDataPlane` bundle.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../fixtures",
            scripts("data_planes", "alice", "private_links")
        )
    )]
    async fn test_read_grants_view_but_not_modify(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        sqlx::query(
            "INSERT INTO auth.users (id, email) VALUES \
             ('22222222-2222-2222-2222-222222222222', 'bob@example.test')",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO user_grants (user_id, object_role, capability) VALUES \
             ($1, 'ops/dp/private/aliceCo/', 'read')",
        )
        .bind(uuid::Uuid::from_bytes([0x22; 16]))
        .execute(&pool)
        .await
        .unwrap();

        let server =
            test_server::TestServer::start(pool.clone(), test_server::snapshot(pool, false).await)
                .await;
        let bob_token =
            server.make_access_token(uuid::Uuid::from_bytes([0x22; 16]), Some("bob@example.test"));

        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        dataPlanes {
                            edges {
                                node {
                                    name
                                    privateLinks { __typename }
                                    awsLinkEndpoints
                                    azureLinkEndpoints
                                    gcpPscEndpoints
                                }
                            }
                        }
                    }
                    "#
                }),
                Some(&bob_token),
            )
            .await;

        let edges = response["data"]["dataPlanes"]["edges"]
            .as_array()
            .expect("should have edges");
        let private_dp = edges
            .iter()
            .find(|e| e["node"]["name"] == "ops/dp/private/aliceCo/aws-us-east-1-c1")
            .expect("bob should see the private dp via his read grant");
        // The fixture populates three private links and one AWS provisioning
        // result; bob's `read` is enough to view all of them.
        assert_eq!(
            private_dp["node"]["privateLinks"].as_array().unwrap().len(),
            3,
            "read must grant view of private links: {private_dp}",
        );
        assert_eq!(
            private_dp["node"]["awsLinkEndpoints"]
                .as_array()
                .unwrap()
                .len(),
            1,
            "read must grant view of endpoint provisioning results: {private_dp}",
        );

        // Mutating requires `ModifyDataPlanePrivateNetworking`, which `read`
        // does not carry.
        let bob_denied: serde_json::Value = server
            .graphql(
                &update_mutation("ops/dp/private/aliceCo/aws-us-east-1-c1", VALID_AWS_INPUT),
                Some(&bob_token),
            )
            .await;
        assert_eq!(
            first_error_message(&bob_denied),
            "PermissionDenied: bob@example.test is not authorized to access prefix or name 'ops/dp/private/aliceCo/aws-us-east-1-c1' with required capability ModifyDataPlanePrivateNetworking",
        );
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
            .graphql(&update_mutation(dp, VALID_AWS_INPUT), Some(&alice_token))
            .await;
        assert_eq!(
            first_error_message(&denied),
            "PermissionDenied: alice@example.test is not authorized to access prefix or name 'ops/dp/private/aliceCo/aws-us-east-1-c1' with required capability ModifyDataPlanePrivateNetworking",
        );
    }

    // A malformed `private_links` row produces a field-level error that names
    // the data plane and the failing index. Because `privateLinks` is declared
    // `[PrivateLink!]!` (non-null), the error null-propagates up to the
    // nullable root and the whole `data` field comes back as null; the error
    // path locates the offending edge.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../fixtures",
            scripts("data_planes", "alice", "private_links")
        )
    )]
    async fn test_graphql_data_planes_malformed_private_link(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        // Corrupt the private_links column for the private dp before snapshot.
        sqlx::query(
            r#"UPDATE data_planes
               SET private_links = array['{"not":"a private link"}'::json]
               WHERE data_plane_name = 'ops/dp/private/aliceCo/aws-us-east-1-c1'"#,
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
                                    privateLinks { __typename }
                                }
                            }
                        }
                    }
                    "#
                }),
                Some(&token),
            )
            .await;

        insta::assert_json_snapshot!("data_planes_malformed_private_link", response);
    }

    // ===== updateDataPlanePrivateLinks mutation tests =====

    const VALID_AWS_INPUT: &str = r#"{
        "aws": {
            "region": "us-east-1",
            "azIds": ["use1-az1", "use1-az2"],
            "serviceName": "com.amazonaws.vpce.us-east-1.vpce-svc-abc123"
        }
    }"#;
    const VALID_AZURE_INPUT: &str = r#"{
        "azure": {
            "serviceName": "/subscriptions/x/resourceGroups/rg/providers/Microsoft.Network/privateLinkServices/svc",
            "location": "eastus",
            "dnsName": "privatelink.database.windows.net",
            "resourceType": ""
        }
    }"#;
    const VALID_GCP_INPUT: &str = r#"{
        "gcp": {
            "serviceAttachment": "projects/p/regions/us-central1/serviceAttachments/sa",
            "region": "us-central1",
            "dnsZoneName": "z",
            "dnsRecordNames": ["r1"],
            "allPorts": true
        }
    }"#;

    fn update_mutation(name: &str, links_json: &str) -> serde_json::Value {
        // The mutation echoes the stored links as the `PrivateLink` union, so
        // the selection set spreads each variant's discriminating fields.
        serde_json::json!({
            "query": r#"
            mutation($name: String!, $links: [PrivateLinkInput!]!) {
                updateDataPlanePrivateLinks(dataPlaneName: $name, privateLinks: $links) {
                    __typename
                    ... on AWSPrivateLink { region serviceName }
                    ... on AzurePrivateLink { serviceName location }
                    ... on GCPPrivateServiceConnect { serviceAttachment region }
                }
            }"#,
            "variables": {
                "name": name,
                "links": serde_json::from_str::<serde_json::Value>(&format!("[{links_json}]")).unwrap(),
            }
        })
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../fixtures",
            scripts("data_planes", "alice", "private_links")
        )
    )]
    async fn test_update_private_links_happy_path(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let alice_token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        let dp = "ops/dp/private/aliceCo/aws-us-east-1-c1";
        let links = format!("{VALID_AWS_INPUT},{VALID_AZURE_INPUT},{VALID_GCP_INPUT}");

        let updated_at_before: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT updated_at FROM data_planes WHERE data_plane_name = $1")
                .bind(dp)
                .fetch_one(&pool)
                .await
                .unwrap();

        let response: serde_json::Value = server
            .graphql(&update_mutation(dp, &links), Some(&alice_token))
            .await;
        // The mutation echoes the three submitted links in order, one per
        // union variant.
        let echoed = response["data"]["updateDataPlanePrivateLinks"]
            .as_array()
            .unwrap_or_else(|| panic!("expected echoed links, got: {response}"));
        let typenames: Vec<&str> = echoed
            .iter()
            .map(|l| l["__typename"].as_str().unwrap())
            .collect();
        assert_eq!(
            typenames,
            [
                "AWSPrivateLink",
                "AzurePrivateLink",
                "GCPPrivateServiceConnect"
            ],
        );
        assert_eq!(echoed[0]["region"], "us-east-1");

        // Postgres `now()` is `transaction_timestamp()` at microsecond
        // precision, so two distinct transactions return distinct values.
        let updated_at_after: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT updated_at FROM data_planes WHERE data_plane_name = $1")
                .bind(dp)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(
            updated_at_after > updated_at_before,
            "updated_at must advance on a successful mutation"
        );

        // Calling again with a single AWS link replaces the entire array
        // rather than merging.
        let response: serde_json::Value = server
            .graphql(&update_mutation(dp, VALID_AWS_INPUT), Some(&alice_token))
            .await;
        let echoed = response["data"]["updateDataPlanePrivateLinks"]
            .as_array()
            .unwrap_or_else(|| panic!("expected echoed links, got: {response}"));
        assert_eq!(echoed.len(), 1);
        assert_eq!(echoed[0]["__typename"], "AWSPrivateLink");

        // Confirm the second call replaced (rather than merged) the array.
        let stored_count: i64 = sqlx::query_scalar(
            "SELECT array_length(private_links, 1)::bigint FROM data_planes WHERE data_plane_name = $1",
        )
        .bind(dp)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(stored_count, 1);
    }

    /// Extracts the first error message from a GraphQL response, or panics
    /// if the response did not return an error.
    fn first_error_message(response: &serde_json::Value) -> &str {
        response["errors"][0]["message"]
            .as_str()
            .unwrap_or_else(|| panic!("expected an error, got: {response}"))
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../fixtures",
            scripts("data_planes", "alice", "private_links")
        )
    )]
    async fn test_update_private_links_authorization(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        // Create a bob who has no grants on the private dp.
        sqlx::query(
            "INSERT INTO auth.users (id, email) VALUES \
             ('22222222-2222-2222-2222-222222222222', 'bob@example.test')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let server =
            test_server::TestServer::start(pool.clone(), test_server::snapshot(pool, false).await)
                .await;
        let alice_token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);
        let bob_token =
            server.make_access_token(uuid::Uuid::from_bytes([0x22; 16]), Some("bob@example.test"));

        let dp = "ops/dp/private/aliceCo/aws-us-east-1-c1";

        // Alice has read on the private dp via the aliceCo/ -> ops/dp/private/aliceCo/
        // role grant installed by the private_links fixture.
        let alice_ok: serde_json::Value = server
            .graphql(&update_mutation(dp, VALID_AWS_INPUT), Some(&alice_token))
            .await;
        let echoed = alice_ok["data"]["updateDataPlanePrivateLinks"]
            .as_array()
            .unwrap_or_else(|| panic!("alice with `read` should succeed: {alice_ok}"));
        assert_eq!(echoed.len(), 1);
        assert_eq!(echoed[0]["__typename"], "AWSPrivateLink");

        // Bob has no grants and should be rejected.
        let bob_denied: serde_json::Value = server
            .graphql(&update_mutation(dp, VALID_AWS_INPUT), Some(&bob_token))
            .await;
        assert_eq!(
            first_error_message(&bob_denied),
            "PermissionDenied: bob@example.test is not authorized to access prefix or name 'ops/dp/private/aliceCo/aws-us-east-1-c1' with required capability ModifyDataPlanePrivateNetworking",
        );
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../fixtures",
            scripts("data_planes", "alice", "private_links")
        )
    )]
    async fn test_update_private_links_name_validation(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let server =
            test_server::TestServer::start(pool.clone(), test_server::snapshot(pool, false).await)
                .await;
        let alice_token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        // Names outside `ops/dp/private/<tenant>/...` are rejected by the
        // structural check before any auth or DB work.
        let cases: &[&str] = &[
            "ops/dp/public/aws-us-west-2-c1",
            "ops/dp/private/aws-us-east-1-c1",
        ];
        for name in cases {
            let response: serde_json::Value = server
                .graphql(&update_mutation(name, VALID_AWS_INPUT), Some(&alice_token))
                .await;
            assert_eq!(
                first_error_message(&response),
                format!("{name} is not a private data-plane name"),
                "case: {name}",
            );
        }

        // A structurally-valid name that alice is authorized for (the
        // fixture's aliceCo/ -> ops/dp/private/aliceCo/ role grant covers any
        // sub-prefix) but which matches no data_planes row: the UPDATE
        // affects zero rows and reports not-found.
        let response: serde_json::Value = server
            .graphql(
                &update_mutation("ops/dp/private/aliceCo/aws-us-east-2-c9", VALID_AWS_INPUT),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            first_error_message(&response),
            "data plane 'ops/dp/private/aliceCo/aws-us-east-2-c9' not found",
        );
    }

    // Covers both directions of the shared cursor helper, including the
    // forward over-report of has-next when a page ends exactly at the last
    // row (no +1 lookahead; the follow-up fetch comes back empty).
    #[test]
    fn paginates_by_name() {
        fn page(
            after: Option<&str>,
            before: Option<&str>,
            first: Option<usize>,
            last: Option<usize>,
        ) -> (Vec<&'static str>, bool, bool) {
            paginate_by_name(
                vec!["a", "b", "c", "d"],
                |n| *n,
                after.map(String::from),
                before.map(String::from),
                first,
                last,
            )
        }

        // Forward pagination: (rows, has_previous, has_next).
        assert_eq!(
            page(None, None, Some(2), None),
            (vec!["a", "b"], false, true)
        );
        assert_eq!(
            page(Some("b"), None, Some(2), None),
            (vec!["c", "d"], true, true)
        );
        assert_eq!(
            page(Some("b"), None, Some(10), None),
            (vec!["c", "d"], true, false)
        );
        assert_eq!(page(Some("d"), None, Some(2), None), (vec![], true, false));

        // Backward pagination.
        assert_eq!(
            page(None, None, None, Some(2)),
            (vec!["c", "d"], true, false)
        );
        assert_eq!(
            page(None, Some("d"), None, Some(2)),
            (vec!["b", "c"], true, true)
        );
        assert_eq!(
            page(None, Some("c"), None, Some(10)),
            (vec!["a", "b"], false, true)
        );

        // Zero and default limits.
        assert_eq!(page(None, None, Some(0), None), (vec![], false, false));
        assert_eq!(
            page(None, None, None, None),
            (vec!["a", "b", "c", "d"], false, false)
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
