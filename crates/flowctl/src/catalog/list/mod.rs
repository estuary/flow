use anyhow::Context;

use crate::catalog::{DataPlaneSelector, NameSelector, SpecTypeSelector};
use crate::{graphql::*, output};

#[derive(Default, Debug, Clone, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct List {
    /// Include "Reads From" / "Writes To" columns in the output.
    #[clap(short = 'f', long = "flows")]
    pub include_flows: bool,
    /// Include the models in the output (requires '--output json|yaml')
    #[clap(long = "models")]
    pub include_models: bool,
    #[clap(flatten)]
    pub name_selector: NameSelector,
    #[clap(flatten)]
    pub type_selector: SpecTypeSelector,
    #[clap(flatten)]
    pub data_plane_selector: DataPlaneSelector,

    /// This option is not exposed as a CLI argument. It just allows us to skip
    /// fetching publication info in contexts where it's not necessary.
    #[clap(skip = true)]
    pub include_last_publication: bool,
}

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/catalog/list/query.graphql",
    response_derives = "Serialize,Clone",
    variables_derives = "Clone",
    extern_enums("CatalogType")
)]
struct ListLiveSpecsQuery;

pub async fn do_list(ctx: &mut crate::CliContext, list_args: &List) -> anyhow::Result<()> {
    if list_args.include_models && ctx.get_output_type() == output::OutputType::Table {
        anyhow::bail!(
            "cannot output models as a table, must pass `--output json` or `--output yaml`"
        );
    }
    let rows = fetch_live_specs(ctx, list_args.clone()).await?;

    ctx.write_all(rows, list_args.include_flows)
}

pub async fn fetch_live_specs(
    ctx: &mut crate::CliContext,
    mut list: List,
) -> anyhow::Result<Vec<list_live_specs_query::SelectRef>> {
    use futures::TryStreamExt;

    if list.name_selector.name.is_empty() && list.name_selector.prefix.is_empty() {
        let prefixes =
            crate::get_default_prefix_arguments(ctx, models::Capability::Read, 5).await?;
        list.name_selector.prefix = prefixes;
    }

    fetch_paginated_live_specs(ctx.rest.clone(), ctx.access_token(), list)
        .try_collect()
        .await
}

pub fn into_draft(
    specs: Vec<list_live_specs_query::SelectRef>,
) -> anyhow::Result<tables::DraftCatalog> {
    let mut catalog = tables::DraftCatalog::default();

    fn parse<T: serde::de::DeserializeOwned>(
        model: Option<&models::RawValue>,
    ) -> anyhow::Result<Option<T>> {
        if let Some(model) = model {
            Ok(Some(serde_json::from_str::<T>(model.get())?))
        } else {
            Ok(None)
        }
    }

    for row in specs {
        let list_live_specs_query::SelectRef {
            catalog_name,
            live_spec: Some(live_spec),
            ..
        } = row
        else {
            continue;
        };

        let scope = tables::synthetic_scope("control", &catalog_name);

        match live_spec.catalog_type {
            CatalogType::Capture => {
                catalog.captures.insert_row(
                    models::Capture::new(catalog_name),
                    &scope,
                    Some(live_spec.last_pub_id),
                    parse::<models::CaptureDef>(live_spec.model.as_ref())?,
                    false, // !is_touch
                );
            }
            CatalogType::Collection => {
                catalog.collections.insert_row(
                    models::Collection::new(catalog_name),
                    &scope,
                    Some(live_spec.last_pub_id),
                    parse::<models::CollectionDef>(live_spec.model.as_ref())?,
                    false, // !is_touch
                );
            }
            CatalogType::Materialization => {
                catalog.materializations.insert_row(
                    models::Materialization::new(catalog_name),
                    &scope,
                    Some(live_spec.last_pub_id),
                    parse::<models::MaterializationDef>(live_spec.model.as_ref())?,
                    false, // !is_touch
                );
            }
            CatalogType::Test => {
                catalog.tests.insert_row(
                    models::Test::new(catalog_name),
                    &scope,
                    Some(live_spec.last_pub_id),
                    parse::<models::TestDef>(live_spec.model.as_ref())?,
                    false, // !is_touch
                );
            }
        }
    }
    Ok(catalog)
}

impl output::CliOutput for list_live_specs_query::SelectRef {
    type TableAlt = bool;
    type CellValue = String;

    fn table_headers(flows: Self::TableAlt) -> Vec<&'static str> {
        let mut headers = vec![
            "ID",
            "Name",
            "Type",
            "Updated",
            "Updated By",
            "Data Plane ID",
        ];
        if flows {
            headers.push("Reads From");
            headers.push("Writes To");
        }
        headers
    }

    fn into_table_row(self, flows: Self::TableAlt) -> Vec<Self::CellValue> {
        let user_info = self
            .last_publication
            .map(|last_pub| {
                crate::format_user(
                    last_pub.user_email,
                    last_pub.user_full_name,
                    Some(last_pub.user_id),
                )
            })
            .unwrap_or_else(|| String::from("unknown"));
        let mut out = vec![
            self.live_spec
                .as_ref()
                .map(|ls| ls.live_spec_id.to_string())
                .unwrap_or_default(),
            self.catalog_name.to_string(),
            self.live_spec
                .as_ref()
                .map(|ls| ls.catalog_type.as_ref().to_string())
                .unwrap_or_default(),
            self.live_spec
                .as_ref()
                .map(|ls| ls.updated_at.to_rfc3339())
                .unwrap_or_default(),
            user_info,
            self.live_spec
                .as_ref()
                .map(|ls| ls.data_plane_id.to_string())
                .unwrap_or_default(),
        ];
        if flows {
            out.push(
                self.live_spec
                    .as_ref()
                    .map(|ls| format_flows(ls.reads_from.as_ref()))
                    .unwrap_or_default(),
            );
            out.push(
                self.live_spec
                    .as_ref()
                    .map(|ls| format_flows(ls.writes_to.as_ref()))
                    .unwrap_or_default(),
            );
        }
        out
    }
}

fn format_flows(conn: Option<&list_live_specs_query::SelectConnection>) -> String {
    use itertools::Itertools;

    conn.into_iter()
        .flat_map(|n| n.edges.iter())
        .map(|e| e.node.catalog_name.as_str())
        .join("\n")
}

/// Executes the graphql query for the given `list` arguments, making additional
/// requests as necessary to read all of the results.
fn fetch_paginated_live_specs(
    rest: flow_client_next::rest::Client,
    access_token: Option<String>,
    list: List,
) -> impl futures::Stream<Item = anyhow::Result<list_live_specs_query::SelectRef>> + 'static {
    if list.name_selector.name.is_empty() && list.name_selector.prefix.is_empty() {
        panic!("fetch_paginated_live_specs requires either a name or prefix selector");
    }
    // Use a smaller batch size if we're including the models, since they can be quite large.
    let page_size = if list.include_models { 50 } else { 200 };
    let is_by_name = !list.name_selector.name.is_empty();
    coroutines::try_coroutine(|mut co| async move {
        for query_by in to_vars(&list) {
            let mut cursor: Option<String> = None;

            'pagination: loop {
                let vars = list_live_specs_query::Variables {
                    by: query_by.clone(),
                    after: cursor.take(),
                    first: Some(page_size),
                    include_models: list.include_models,
                    include_flows: list.include_flows,
                    include_last_publication: list.include_last_publication,
                };
                let resp = post_graphql::<ListLiveSpecsQuery>(&rest, access_token.as_deref(), vars)
                    .await
                    .context("failed to fetch live specs")?;

                for edge in resp.live_specs.edges {
                    // Only error when the user explicitly requested the spec by
                    // name and it does not exist. Otherwise, a missing live spec
                    // just indicates that the spec is in the process of being
                    // deleted.
                    if edge.node.live_spec.is_none() && is_by_name {
                        anyhow::bail!("no live spec exists for name: '{}'", edge.node.catalog_name);
                    }
                    let () = co.yield_(edge.node).await;
                }
                if !resp.live_specs.page_info.has_next_page {
                    break 'pagination;
                }
                cursor = resp.live_specs.page_info.end_cursor;
                assert!(cursor.is_some(), "liveSpecs pageInfo missing endCursor");
            }
        }
        Ok(())
    })
}

fn to_vars(list: &List) -> Vec<list_live_specs_query::LiveSpecsBy> {
    let data_plane_name = list
        .data_plane_selector
        .data_plane_name
        .as_deref()
        .map(models::Name::new);
    let catalog_type = list.type_selector.get_single_type_selection();

    let mut vars = Vec::new();
    let mut seen_prefixes = std::collections::HashSet::new();
    for prefix in list.name_selector.prefix.iter() {
        let prefix = normalize_prefix(prefix);
        // Skip prefixes that normalize to an already-queued value. Because we
        // append a trailing '/', a bare `acmeCo` and an explicit `acmeCo/`
        // collapse to the same prefix; issuing both queries would list every
        // matching spec twice.
        if !seen_prefixes.insert(prefix.as_str().to_string()) {
            continue;
        }
        vars.push(list_live_specs_query::LiveSpecsBy {
            names: None,
            prefix: Some(prefix),
            catalog_type,
            data_plane_name: data_plane_name.clone(),
        });
    }
    if !list.name_selector.name.is_empty() {
        let names = list
            .name_selector
            .name
            .iter()
            .map(|n| models::Name::new(n.as_str()))
            .collect::<Vec<_>>();
        vars.push(list_live_specs_query::LiveSpecsBy {
            names: Some(names),
            prefix: None,
            catalog_type,
            data_plane_name: data_plane_name.clone(),
        });
    }
    vars
}

/// Normalizes a user-supplied `--prefix` value by appending a trailing '/' when
/// it's missing. Estuary prefixes must end in a '/' (for example, `acmeCo/`).
/// When the slash is omitted, the control plane treats the value as an
/// unrecognized catalog name and returns a misleading `PermissionDenied` error
/// even for fully-authorized users, so we fix it up here at the CLI boundary.
/// An empty prefix is left as-is: it's a valid selector meaning "everything the
/// user can access", and a bare "/" would be rejected.
fn normalize_prefix(prefix: &str) -> models::Prefix {
    if prefix.is_empty() || prefix.ends_with('/') {
        models::Prefix::new(prefix)
    } else {
        models::Prefix::new(format!("{prefix}/"))
    }
}

#[cfg(test)]
mod test {
    use super::{List, normalize_prefix, to_vars};

    #[test]
    fn test_to_vars_dedups_prefixes() {
        // A bare prefix and its trailing-slash form normalize to the same value;
        // querying both would double-list every matching spec, so `to_vars`
        // should emit a single query. The same-value-twice case dedups too.
        let mut list = List::default();
        list.name_selector.prefix = vec![
            "acmeCo".to_string(),
            "acmeCo/".to_string(),
            "acmeCo".to_string(),
            "acmeCo/widgets".to_string(),
        ];

        let prefixes = to_vars(&list)
            .into_iter()
            .map(|by| by.prefix.expect("prefix selector").as_str().to_string())
            .collect::<Vec<_>>();
        assert_eq!(prefixes, vec!["acmeCo/", "acmeCo/widgets/"]);
    }

    #[test]
    fn test_normalize_prefix() {
        // A missing trailing slash is the reported foot-gun, and is filled in.
        assert_eq!(normalize_prefix("acmeCo").as_str(), "acmeCo/");
        assert_eq!(
            normalize_prefix("acmeCo/widgets").as_str(),
            "acmeCo/widgets/"
        );
        // Already-normalized prefixes pass through unchanged.
        assert_eq!(normalize_prefix("acmeCo/").as_str(), "acmeCo/");
        assert_eq!(
            normalize_prefix("acmeCo/widgets/").as_str(),
            "acmeCo/widgets/"
        );
        // An empty prefix stays empty (a valid "everything" selector); we must
        // not turn it into the invalid "/".
        assert_eq!(normalize_prefix("").as_str(), "");
    }
}
