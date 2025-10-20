use crate::{graphql::*, output::OutputType};
use anyhow::Context;
use serde::Serialize;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/catalog/history/query.graphql",
    response_derives = "Serialize,Clone",
    variables_derives = "Clone",
    extern_enums("CatalogType")
)]
struct PublicationHistoryQuery;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct History {
    /// Catalog name or prefix to retrieve history for.
    #[clap(long)]
    pub name: String,

    /// Include the live specs models in the output (requires `--output json|yaml`)
    #[clap(long = "models")]
    pub include_models: bool,
}

#[derive(Serialize)]
pub struct HistoryRow {
    catalog_name: models::Name,
    catalog_type: Option<models::CatalogType>,
    publication: publication_history_query::SelectPublication,
}

pub async fn do_history(ctx: &mut crate::CliContext, history: &History) -> anyhow::Result<()> {
    use futures::TryStreamExt;

    if history.include_models && ctx.get_output_type() == OutputType::Table {
        anyhow::bail!(
            "cannot output models as a table, must pass `--output json` or `--output yaml`"
        );
    }

    let entries: Vec<HistoryRow> = stream_history(
        ctx.client.clone(),
        models::Name::new(&history.name),
        history.include_models,
    )
    .try_collect()
    .await?;

    ctx.write_all(entries, ())
}

impl crate::output::CliOutput for HistoryRow {
    type TableAlt = ();

    type CellValue = String;

    fn table_headers(_alt: Self::TableAlt) -> Vec<&'static str> {
        vec![
            "Name",
            "Type",
            "Publication ID",
            "Published",
            "Published By",
            "Details",
        ]
    }

    fn into_table_row(self, _alt: Self::TableAlt) -> Vec<Self::CellValue> {
        vec![
            self.catalog_name.to_string(),
            self.catalog_type
                .map(|ct| ct.to_string())
                .unwrap_or_default(),
            self.publication.publication_id.to_string(),
            self.publication.published_at.to_rfc3339(),
            crate::format_user(
                self.publication.user_email,
                self.publication.user_full_name,
                Some(self.publication.user_id),
            ),
            self.publication.detail.unwrap_or_default(),
        ]
    }
}

fn stream_history(
    client: crate::Client,
    catalog_name: models::Name,
    include_models: bool,
) -> impl futures::Stream<Item = anyhow::Result<HistoryRow>> {
    let page_size = if include_models { 50 } else { 200 };
    coroutines::try_coroutine(|mut co| async move {
        let mut cursor: Option<String> = None;
        loop {
            let vars = publication_history_query::Variables {
                catalog_name: catalog_name.clone(),
                include_models,
                after: cursor.take(),
                first: page_size,
            };
            let mut resp = post_graphql::<PublicationHistoryQuery>(&client, vars)
                .await
                .context("failed to fetch publication history")?;
            let Some(live) = resp.live_specs.edges.pop() else {
                anyhow::bail!("no live spec found for name: '{catalog_name}'");
            };
            let Some(history) = live.node.publication_history else {
                return Ok(());
            };
            for item in history.edges {
                let () = co
                    .yield_(HistoryRow {
                        catalog_name: catalog_name.clone(),
                        catalog_type: live.node.live_spec.as_ref().map(|ls| ls.catalog_type),
                        publication: item.node,
                    })
                    .await;
            }
            if !history.page_info.has_next_page {
                return Ok(());
            }
            cursor = history.page_info.end_cursor;
        }
    })
}
