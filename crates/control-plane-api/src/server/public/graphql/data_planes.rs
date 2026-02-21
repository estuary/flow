use async_graphql::{
    ComplexObject, Context, SimpleObject, dataloader,
    types::connection::{self, Connection},
};
use std::collections::HashMap;

const DEFAULT_PAGE_SIZE: usize = 50;

/// Cloud provider where the data plane is hosted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, async_graphql::Enum)]
pub enum CloudProvider {
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
    pub data_plane_name: String,
    /// Fully-qualified domain name of this data-plane.
    pub data_plane_fqdn: String,
    /// Address of reactors within the data-plane.
    pub reactor_address: String,
    /// The current user's capability to this data plane's name prefix.
    pub user_capability: models::Capability,
    /// Cloud provider where this data-plane is hosted.
    pub cloud_provider: CloudProvider,
    /// Cloud region where this data-plane is hosted.
    /// For example: "us-east-1" (AWS), "us-central1" (GCP), "eastus" (Azure).
    pub region: String,
    /// Cluster identifier within the region.
    pub tag: String,
    /// Whether this is a public data-plane.
    pub is_public: bool,
}

/// Typed key for loading cloud-provider details of data planes from the database.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataPlaneDetailsKey(pub String);

#[derive(Clone)]
pub struct DataPlaneDetails {
    cidr_blocks: Vec<String>,
    gcp_service_account_email: Option<String>,
    aws_iam_user_arn: Option<String>,
    azure_application_name: Option<String>,
    azure_application_client_id: Option<String>,
}

impl dataloader::Loader<DataPlaneDetailsKey> for super::PgDataLoader {
    type Value = DataPlaneDetails;
    type Error = async_graphql::Error;

    async fn load(
        &self,
        keys: &[DataPlaneDetailsKey],
    ) -> Result<HashMap<DataPlaneDetailsKey, Self::Value>, Self::Error> {
        let names = keys.iter().map(|k| k.0.as_str()).collect::<Vec<_>>();

        tracing::debug!(count = names.len(), "loading data_plane details");

        sqlx::query!(
            r#"select
                dp.data_plane_name,
                dp.cidr_blocks::text[] as "cidr_blocks!: Vec<String>",
                dp.gcp_service_account_email,
                dp.aws_iam_user_arn,
                dp.azure_application_name,
                dp.azure_application_client_id
            from unnest($1::text[]) as input(name)
            join data_planes dp on dp.data_plane_name = input.name
            "#,
            &names as &[&str],
        )
        .fetch_all(&self.0)
        .await
        .map_err(async_graphql::Error::from)
        .map(|rows| {
            rows.into_iter()
                .map(|row| {
                    let key = DataPlaneDetailsKey(row.data_plane_name);
                    let details = DataPlaneDetails {
                        cidr_blocks: row.cidr_blocks,
                        gcp_service_account_email: row.gcp_service_account_email,
                        aws_iam_user_arn: row.aws_iam_user_arn,
                        azure_application_name: row.azure_application_name,
                        azure_application_client_id: row.azure_application_client_id,
                    };
                    (key, details)
                })
                .collect()
        })
    }
}

#[ComplexObject]
impl DataPlane {
    /// CIDR blocks for this data-plane.
    async fn cidr_blocks(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<String>> {
        let loader = ctx.data::<dataloader::DataLoader<super::PgDataLoader>>()?;
        let details = loader
            .load_one(DataPlaneDetailsKey(self.data_plane_name.clone()))
            .await?;
        Ok(details.map(|d| d.cidr_blocks).unwrap_or_default())
    }

    /// GCP service account email for this data-plane.
    async fn gcp_service_account_email(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Option<String>> {
        let loader = ctx.data::<dataloader::DataLoader<super::PgDataLoader>>()?;
        let details = loader
            .load_one(DataPlaneDetailsKey(self.data_plane_name.clone()))
            .await?;
        Ok(details.and_then(|d| d.gcp_service_account_email))
    }

    /// AWS IAM user ARN for this data-plane.
    async fn aws_iam_user_arn(&self, ctx: &Context<'_>) -> async_graphql::Result<Option<String>> {
        let loader = ctx.data::<dataloader::DataLoader<super::PgDataLoader>>()?;
        let details = loader
            .load_one(DataPlaneDetailsKey(self.data_plane_name.clone()))
            .await?;
        Ok(details.and_then(|d| d.aws_iam_user_arn))
    }

    /// Azure application name for this data-plane.
    async fn azure_application_name(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Option<String>> {
        let loader = ctx.data::<dataloader::DataLoader<super::PgDataLoader>>()?;
        let details = loader
            .load_one(DataPlaneDetailsKey(self.data_plane_name.clone()))
            .await?;
        Ok(details.and_then(|d| d.azure_application_name))
    }

    /// Azure application client ID for this data-plane.
    async fn azure_application_client_id(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Option<String>> {
        let loader = ctx.data::<dataloader::DataLoader<super::PgDataLoader>>()?;
        let details = loader
            .load_one(DataPlaneDetailsKey(self.data_plane_name.clone()))
            .await?;
        Ok(details.and_then(|d| d.azure_application_client_id))
    }
}

/// Parses a data plane name into its component parts.
/// Returns None if the name format is invalid.
///
/// Expected formats:
/// - Cloud: "ops/dp/public/aws-us-east-1-c1" or "ops/dp/private/gcp-us-central1-c2"
/// - Local: "ops/dp/local/local-foo" (any suffix after "local-")
fn parse_data_plane_name(name: &str) -> Option<(CloudProvider, String, String, bool)> {
    let last_segment = name.rsplit('/').next()?;
    let (provider_str, after_provider) = last_segment.split_once('-')?;

    match provider_str {
        "local" => Some((
            CloudProvider::Local,
            "local".to_string(),
            "1".to_string(),
            true,
        )),
        "aws" | "az" | "azure" | "gcp" => {
            // Must have privacy indicator in path.
            if !name.contains("ops/dp/private/") && !name.contains("ops/dp/public/") {
                return None;
            }

            // Parse cluster suffix (e.g., "-c1", "-c5").
            let idx = after_provider.rfind("-c")?;
            let cluster = &after_provider[idx + 2..];
            if cluster.is_empty() || !cluster.chars().all(|c| c.is_ascii_digit()) {
                return None;
            }

            let region = &after_provider[..idx];
            if region.is_empty() {
                return None;
            }

            let cloud_provider = match provider_str {
                "aws" => CloudProvider::Aws,
                "az" | "azure" => CloudProvider::Azure,
                "gcp" => CloudProvider::Gcp,
                _ => unreachable!(),
            };

            let is_public = name.contains("ops/dp/public/");
            Some((
                cloud_provider,
                region.to_string(),
                cluster.to_string(),
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

        // Build row data map for attach_user_capabilities.
        let row_data: std::collections::HashMap<String, &tables::DataPlane> = rows
            .into_iter()
            .map(|dp| (dp.data_plane_name.clone(), dp))
            .collect();

        let edges = crate::server::attach_user_capabilities(
            env.snapshot(),
            env.claims()?,
            row_data.keys().cloned(),
            |data_plane_name, user_capability| {
                let dp = row_data.get(&data_plane_name)?;
                let (cloud_provider, region, tag, is_public) =
                    parse_data_plane_name(&data_plane_name).expect("name validated by pre-filter");
                Some(connection::Edge::new(
                    data_plane_name.clone(),
                    DataPlane {
                        data_plane_name,
                        data_plane_fqdn: dp.data_plane_fqdn.clone(),
                        reactor_address: dp.reactor_address.clone(),
                        user_capability: user_capability
                            .expect("capability guaranteed by pre-filter"),
                        cloud_provider,
                        region,
                        tag,
                        is_public,
                    },
                ))
            },
        );

        let mut conn = PaginatedDataPlanes::new(has_prev, has_next);
        conn.edges = edges;
        Ok(conn)
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

        // Insert a data plane with a parseable name so it passes the
        // name-format filter in the dataPlanes query.
        sqlx::query!(
            r#"insert into public.data_planes (
                id, data_plane_name, data_plane_fqdn, hmac_keys, encrypted_hmac_keys,
                broker_address, reactor_address,
                ops_logs_name, ops_stats_name,
                ops_l1_events_name, ops_l1_inferred_name, ops_l1_stats_name,
                ops_l2_events_transform, ops_l2_inferred_transform, ops_l2_stats_transform,
                enable_l2,
                cidr_blocks, aws_iam_user_arn, gcp_service_account_email, azure_application_client_id
            ) values (
                'aaaaaaaaaaaa', 'ops/dp/public/aws-us-east-1-c1', 'dp.test', '{"dGVzdA=="}', '{}',
                'broker.dp.test', 'reactor.dp.test',
                'ops/tasks/public/one/logs', 'ops/tasks/public/one/stats',
                'ops/rollups/L1/public/one/events', 'ops/rollups/L1/public/one/inferred', 'ops/rollups/L1/public/one/stats',
                'from.dp.test', 'from.dp.test', 'from.dp.test',
                true,
                '{10.0.0.0/16,192.168.1.0/24}', 'arn:aws:iam::123456789:user/test', null, null
            )"#
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
                                    dataPlaneName
                                    dataPlaneFqdn
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

    #[test]
    fn parses_aws_public() {
        let (provider, region, tag, is_public) =
            parse_data_plane_name("ops/dp/public/aws-us-east-1-c1").unwrap();
        assert_eq!(provider, CloudProvider::Aws);
        assert_eq!(region, "us-east-1");
        assert_eq!(tag, "1");
        assert!(is_public);
    }

    #[test]
    fn parses_gcp_private() {
        let (provider, region, tag, is_public) =
            parse_data_plane_name("ops/dp/private/estuary/gcp-us-central1-c5").unwrap();
        assert_eq!(provider, CloudProvider::Gcp);
        assert_eq!(region, "us-central1");
        assert_eq!(tag, "5");
        assert!(!is_public);
    }

    #[test]
    fn parses_azure_variants() {
        // "az" prefix
        let (provider, region, tag, _) =
            parse_data_plane_name("ops/dp/private/EastPack/az-australiaeast-c1").unwrap();
        assert_eq!(provider, CloudProvider::Azure);
        assert_eq!(region, "australiaeast");
        assert_eq!(tag, "1");

        // "azure" prefix
        let (provider, region, tag, _) =
            parse_data_plane_name("ops/dp/private/AccumTech/azure-eastus-c1").unwrap();
        assert_eq!(provider, CloudProvider::Azure);
        assert_eq!(region, "eastus");
        assert_eq!(tag, "1");
    }

    #[test]
    fn parses_local() {
        let (provider, region, tag, is_public) =
            parse_data_plane_name("ops/dp/local/local-foo").unwrap();
        assert_eq!(provider, CloudProvider::Local);
        assert_eq!(region, "local");
        assert_eq!(tag, "1");
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
