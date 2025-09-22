use anyhow::Context;
use graphql_client::GraphQLQuery;

pub type DateTime = chrono::DateTime<chrono::Utc>;
pub type JSON = serde_json::Value;

/// The GraphQL query for fetching firing alerts
#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "control-plane-api.graphql",
    query_path = "src/alerts_query.graphql",
    response_derives = "Debug,Clone,Serialize,Deserialize"
)]
pub struct FiringAlertsQuery;

/// Type alias for the alert type returned by the GraphQL query
pub type FiringAlert = firing_alerts_query::FiringAlertsQueryAlerts;

/// Fetch firing alerts for the given catalog prefixes using the GraphQL API
pub async fn fetch_firing_alerts(
    client: &crate::Client,
    prefixes: Vec<String>,
) -> anyhow::Result<Vec<FiringAlert>> {
    let request_body = FiringAlertsQuery::build_query(firing_alerts_query::Variables { prefixes });

    let response: graphql_client::Response<firing_alerts_query::ResponseData> = client
        .agent_unary("/api/graphql", &request_body)
        .await
        .context("executing graphql request")?;

    if let Some(errors) = response.errors {
        anyhow::bail!("GraphQL errors: {:?}", errors);
    }

    let data = response.data.context("GraphQL response missing data")?;

    Ok(data.alerts)
}
