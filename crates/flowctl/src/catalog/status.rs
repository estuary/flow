use std::collections::BTreeMap;

use crate::output::{self, JsonCell, to_table_row};

use crate::graphql::*;

#[derive(Debug, clap::Args)]
pub struct Status {
    /// Names of the live specs to fetch the status of
    #[clap(required(true))]
    pub catalog_names: Vec<String>,
    /// Show the status of all other live specs that are connected to the given
    /// names. Connected specs are tasks that read from or write to a given
    /// collection, or collections that are written or read by a given task. It
    /// also includes materialization source captures, tests that reference a
    /// given collection, or collections referenced by a given test.
    #[clap(long)]
    pub connected: bool,
}

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/catalog/status.graphql",
    response_derives = "Debug,Serialize",
    extern_enums("CatalogType", "Capability"),
    skip_serializing_none
)]
struct ConnectedStatusQuery;

pub async fn do_controller_status(
    ctx: &mut crate::CliContext,
    Status {
        catalog_names,
        connected,
    }: &Status,
) -> anyhow::Result<()> {
    // If the arguments say to output as a table, we can cut down on the number of
    // fields that we return from the query.
    let summary_only = ctx.get_output_type() == output::OutputType::Table;
    let statuses = fetch_statuses(ctx, catalog_names.clone(), *connected, !summary_only).await?;

    ctx.write_all(statuses, ())?;

    Ok(())
}

async fn fetch_statuses(
    ctx: &crate::CliContext,
    names: Vec<String>,
    connected: bool,
    full_status: bool,
) -> anyhow::Result<Vec<StatusRow>> {
    let vars = connected_status_query::Variables {
        names: Some(names),
        connected,
        full_status,
    };

    let data = post_graphql::<ConnectedStatusQuery>(&ctx.client, vars).await?;

    // We'll merge all the statuses into this map in order to deduplicate them
    // when fetching connected statuses
    let mut status_map: std::collections::BTreeMap<String, StatusRow> =
        std::collections::BTreeMap::new();

    for edge in data.live_specs.edges.into_iter() {
        // First get the status for the top-level live specs.
        let node = edge.node;
        let key = node.catalog_name.to_string();

        status_map.insert(
            key,
            StatusRow {
                catalog_name: node.catalog_name,
                live_spec_updated_at: node.live_spec.as_ref().map(|ls| ls.updated_at),
                catalog_type: node.live_spec.as_ref().map(|ls| ls.catalog_type),
                user_capability: node.user_capability,
                status: node.status,
            },
        );
        let Some(live) = node.live_spec else {
            continue;
        };

        // Then get all statuses for connected specs. Note that for connected
        // specs, we ignore any that have missing liveSpecs or statuses because
        // the command did not explicitly request them by name, and it's
        // expected that the user may not have access to all of them.
        if let Some(source_capture) = live.source_capture {
            let key = source_capture.catalog_name.to_string();
            let row = StatusRow {
                catalog_name: source_capture.catalog_name,
                live_spec_updated_at: source_capture.live_spec.as_ref().map(|ls| ls.updated_at),
                catalog_type: source_capture.live_spec.as_ref().map(|ls| ls.catalog_type),
                status: source_capture.status,
                user_capability: source_capture.user_capability,
            };
            status_map.insert(key, row);
        }
        merge_statuses(&mut status_map, live.read_by);
        merge_statuses(&mut status_map, live.reads_from);
        merge_statuses(&mut status_map, live.written_by);
        merge_statuses(&mut status_map, live.writes_to);
    }
    Ok(status_map.into_values().collect())
}

fn merge_statuses(
    into_map: &mut BTreeMap<String, StatusRow>,
    statuses: Option<connected_status_query::SelectAllStatuses>,
) {
    let Some(statuses) = statuses else {
        return;
    };
    for edge in statuses.edges {
        let node = edge.node;
        let key = node.catalog_name.to_string();

        into_map.insert(
            key,
            StatusRow {
                catalog_name: node.catalog_name,
                live_spec_updated_at: node.live_spec.as_ref().map(|ls| ls.updated_at),
                catalog_type: node.live_spec.as_ref().map(|ls| ls.catalog_type),
                status: node.status,
                user_capability: node.user_capability,
            },
        );
    }
}

#[derive(serde::Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct StatusRow {
    catalog_name: models::Name,
    catalog_type: Option<models::CatalogType>,
    live_spec_updated_at: Option<DateTime>,
    status: Option<connected_status_query::SelectStatus>,
    // User capability is here in case it helps differentiate cases where
    // connected specs are deleted vs unauthorized
    user_capability: Option<models::Capability>,
}

impl output::CliOutput for StatusRow {
    type TableAlt = ();
    type CellValue = JsonCell;

    fn table_headers(_alt: Self::TableAlt) -> Vec<&'static str> {
        vec![
            "Name",
            "Type",
            "Status",
            "Message",
            "Live Spec Updated At",
            "Controller Updated At",
        ]
    }

    fn into_table_row(self, _alt: Self::TableAlt) -> Vec<Self::CellValue> {
        to_table_row(
            self,
            &[
                "/catalogName",
                "/catalogType",
                "/status/type",
                "/status/summary",
                "/liveSpecUpdatedAt",
                "/status/controller/updatedAt",
            ],
        )
    }
}
