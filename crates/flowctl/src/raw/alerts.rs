use anyhow::Context;
use graphql_client::GraphQLQuery;

use crate::output;

pub type DateTime = chrono::DateTime<chrono::Utc>;
pub type JSON = serde_json::Value;

/// The GraphQL query for fetching firing alerts
#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/raw/alerts_query.graphql",
    response_derives = "Debug,Clone,Serialize,Deserialize"
)]
pub struct FiringAlertsQuery;

/// Type alias for the alert type returned by the GraphQL query
pub type FiringAlert = firing_alerts_query::FiringAlertsQueryAlertsEdgesNode;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Alerts {
    #[clap(long)]
    prefix: String,
}

/// Fetch firing alerts for the given catalog prefixes using the GraphQL API
pub async fn fetch_firing_alerts(
    client: &crate::Client,
    prefix: String,
) -> anyhow::Result<Vec<FiringAlert>> {
    let mut before: Option<String> = None;
    let mut alerts: Vec<FiringAlert> = Vec::new();

    for page in 0.. {
        let request_body = FiringAlertsQuery::build_query(firing_alerts_query::Variables {
            prefix: prefix.clone(),
            before: before.take(),
            last: 500,
        });

        let response: graphql_client::Response<firing_alerts_query::ResponseData> = client
            .agent_unary("/api/graphql", &request_body)
            .await
            .context("executing graphql request")?;

        if let Some(errors) = response.errors {
            anyhow::bail!("GraphQL errors: {:?}", errors);
        }

        let data = response.data.context("GraphQL response missing data")?;
        before = data.alerts.page_info.end_cursor;
        tracing::debug!(page, alert_count = data.alerts.edges.len(), end_cursor = ?before, %prefix, "got firing alerts response");
        alerts.extend(data.alerts.edges.into_iter().map(|edge| edge.node));
        if before.is_none() {
            break;
        }
    }

    Ok(alerts)
}

impl output::CliOutput for FiringAlert {
    type TableAlt = ();

    type CellValue = output::JsonCell;

    fn table_headers(_alt: Self::TableAlt) -> Vec<&'static str> {
        vec!["Catalog Name", "Fired At", "Alert Type", "Error detail"]
    }

    fn into_table_row(self, _alt: Self::TableAlt) -> Vec<Self::CellValue> {
        output::to_table_row(
            self,
            &["/catalogName", "/firedAt", "/alertType", "/arguments/error"],
        )
    }
}

pub async fn do_alerts(ctx: &mut crate::CliContext, alerts: &Alerts) -> anyhow::Result<()> {
    let resp = fetch_firing_alerts(&ctx.client, alerts.prefix.clone()).await?;
    ctx.write_all(resp, ())
}
