use std::collections::BTreeSet;

use crate::graphql::*;
use crate::output::{CliOutput, JsonCell, to_table_row};
use anyhow::Context;
use serde::{Deserialize, Serialize};

mod author;
mod develop;
mod encrypt;

pub use author::{author, upsert_draft_specs};
pub use develop::develop;
pub use encrypt::encrypt_configs;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/draft/create-mutation.graphql"
)]
struct CreateDraftMutation;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/draft/delete-mutation.graphql"
)]
struct DeleteDraftMutation;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/draft/stage-specs-mutation.graphql",
    variables_derives = "Debug",
    extern_enums("CatalogType")
)]
struct StageDraftSpecsMutation;

pub type DraftSpecInput = stage_draft_specs_mutation::DraftSpecInput;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/draft/unstage-specs-mutation.graphql"
)]
struct UnstageDraftSpecsMutation;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/draft/fetch-query.graphql"
)]
struct FetchDraftQuery;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/draft/fetch-errors-query.graphql"
)]
struct FetchDraftErrorsQuery;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/draft/list-query.graphql"
)]
struct ListDraftsQuery;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/draft/list-specs-query.graphql",
    extern_enums("CatalogType")
)]
struct ListDraftSpecsQuery;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Draft {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum Command {
    /// Author to a draft.
    ///
    /// Authoring a draft fetches and resolves all specifications from your
    /// local Flow catalog files and populates them into your current Draft.
    /// If a specification is already part of your draft then it is replaced.
    ///
    /// Once authored, you can go on to make further edits to draft within
    /// the UI, test your draft, or publish it.
    Author(author::Author),
    /// Create a new draft.
    ///
    /// The created draft will be empty and will be selected.
    Create,
    /// Delete your current draft.
    ///
    /// Its specifications will be dropped, and you will have no selected draft.
    Delete,
    /// Describe your current draft.
    ///
    /// Enumerate all of the specifications within your selected draft.
    Describe,
    /// Develop your current draft within a local directory.
    ///
    /// Fetch all of your draft specifications and place them in a local
    /// Flow catalog file hierarchy for easy editing and development.
    ///
    /// You can then `author` to push your local sources back to your draft,
    /// and repeat this `develop` <=> `author` flow as often as you like.
    Develop(develop::Develop),
    /// List your catalog drafts.
    List,
    /// Test and then publish the current draft.
    ///
    /// A publication only occurs if tests pass.
    /// Once published, your draft is deleted.
    Publish(Publish),
    /// Select a draft to work on.
    ///
    /// You must provide an ID of the draft to select, which can be found via `list`.
    Select(Select),
    /// Test the current draft without publishing it.
    ///
    /// When testing a draft, the control-plane identifies captures,
    /// materializations, derivations, and tests which could be affected by
    /// your change. It verifies the end-to-end effects of your changes to
    /// prevent accidental disruptions due to behavior changes or incompatible
    /// schemas.
    Test(Publish),
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Publish {
    /// Data-plane into which newly initialized specifications will be placed.
    /// This data-plane must be included in the set of data-planes associated
    /// with the specification's covering prefix.
    /// If omitted, the default data-plane of the covering prefix is used.
    #[clap(long, alias = "default-data-plane")]
    init_data_plane: Option<String>,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Select {
    #[clap(long)]
    id: models::Id,
}

impl Draft {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> Result<(), anyhow::Error> {
        match &self.cmd {
            Command::Author(author) => author::do_author(ctx, author).await,
            Command::Create => do_create(ctx).await,
            Command::Delete => do_delete(ctx).await,
            Command::Describe => do_describe(ctx).await,
            Command::Develop(develop) => develop::do_develop(ctx, develop).await,
            Command::List => do_list(ctx).await,
            Command::Publish(publish) => {
                do_publish(ctx, publish.init_data_plane.as_deref(), false).await
            }
            Command::Select(select) => do_select(ctx, select).await,
            Command::Test(publish) => {
                do_publish(ctx, publish.init_data_plane.as_deref(), true).await
            }
        }
    }
}

#[derive(Serialize)]
pub struct DraftRow {
    pub id: models::Id,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime>,
}
impl CliOutput for DraftRow {
    type TableAlt = ();
    type CellValue = JsonCell;

    fn table_headers(_alt: Self::TableAlt) -> Vec<&'static str> {
        vec!["Draft ID", "Created"]
    }

    fn into_table_row(self, _alt: Self::TableAlt) -> Vec<Self::CellValue> {
        to_table_row(self, &["/id", "/created_at"])
    }
}

pub async fn create_draft(ctx: &crate::CliContext) -> Result<DraftRow, anyhow::Error> {
    let vars = create_draft_mutation::Variables {
        detail: Some("Created by flowctl".to_string()),
    };
    let draft = post_graphql::<CreateDraftMutation>(&ctx.rest, ctx.access_token().as_deref(), vars)
        .await?
        .create_draft;

    let row = DraftRow {
        id: draft.id,
        created_at: Some(draft.created_at),
    };
    tracing::info!(draft_id = %row.id, "created draft");
    Ok(row)
}

pub async fn delete_draft(
    ctx: &crate::CliContext,
    draft_id: models::Id,
) -> Result<DraftRow, anyhow::Error> {
    let vars = delete_draft_mutation::Variables { id: draft_id };
    let id = post_graphql::<DeleteDraftMutation>(&ctx.rest, ctx.access_token().as_deref(), vars)
        .await?
        .delete_draft;

    tracing::info!(draft_id = %id, "deleted draft");
    Ok(DraftRow {
        id,
        created_at: None,
    })
}

/// Prints any errors that were found for the given draft id
pub async fn print_draft_errors(
    ctx: &mut crate::CliContext,
    draft_id: models::Id,
) -> anyhow::Result<()> {
    let vars = fetch_draft_errors_query::Variables { id: draft_id };
    let draft =
        post_graphql::<FetchDraftErrorsQuery>(&ctx.rest, ctx.access_token().as_deref(), vars)
            .await?
            .draft;

    // A successful publication deletes its draft, which leaves no errors.
    let errors = draft.map(|draft| draft.errors).unwrap_or_default();

    // TODO(phil): respect the output format when printing errors
    for error in errors {
        let scope = error.scope.unwrap_or_default();
        tracing::error!(%scope, detail = %error.detail);
    }
    Ok(())
}

/// Stages `specs` into the draft, replacing any specs already staged under
/// the same names.
pub async fn stage_draft_specs(
    ctx: &crate::CliContext,
    draft_id: models::Id,
    specs: Vec<DraftSpecInput>,
) -> anyhow::Result<()> {
    // Batches are sent one at a time, as each holds an agent database
    // connection while it stages its specs.
    for specs in batch_draft_specs(specs) {
        let vars = stage_draft_specs_mutation::Variables { draft_id, specs };
        post_graphql::<StageDraftSpecsMutation>(&ctx.rest, ctx.access_token().as_deref(), vars)
            .await
            .context("failed to stage draft specs")?;
    }
    Ok(())
}

/// Splits `specs` into batches of bounded model size, because the agent API
/// rejects request bodies larger than 2MiB. A larger spec is batched on its own.
fn batch_draft_specs(specs: Vec<DraftSpecInput>) -> Vec<Vec<DraftSpecInput>> {
    const BATCH_SPECS: usize = 100;
    const BATCH_MODEL_BYTES: usize = 1 << 20;

    let mut batches = Vec::new();
    let mut batch = Vec::new();
    let mut batch_bytes = 0;

    for spec in specs {
        let spec_bytes = spec.model.as_ref().map_or(0, |model| model.get().len());

        if batch.len() == BATCH_SPECS
            || (!batch.is_empty() && batch_bytes + spec_bytes > BATCH_MODEL_BYTES)
        {
            batches.push(std::mem::take(&mut batch));
            batch_bytes = 0;
        }
        batch.push(spec);
        batch_bytes += spec_bytes;
    }
    if !batch.is_empty() {
        batches.push(batch);
    }
    batches
}

/// Removes staged specs from the draft, and returns the names which were removed.
async fn unstage_draft_specs(
    ctx: &crate::CliContext,
    draft_id: models::Id,
    catalog_names: Vec<models::Name>,
) -> anyhow::Result<Vec<models::Name>> {
    // Bounds each request well under the agent API's 2MiB body limit.
    const BATCH_NAMES: usize = 1000;

    let mut removed = Vec::new();
    for catalog_names in catalog_names.chunks(BATCH_NAMES) {
        let vars = unstage_draft_specs_mutation::Variables {
            draft_id,
            catalog_names: catalog_names.to_vec(),
        };
        removed.extend(
            post_graphql::<UnstageDraftSpecsMutation>(
                &ctx.rest,
                ctx.access_token().as_deref(),
                vars,
            )
            .await?
            .unstage_draft_specs,
        );
    }
    Ok(removed)
}

/// Fetches all specs of the draft, in catalog-name order.
async fn fetch_draft_specs(
    ctx: &crate::CliContext,
    draft_id: models::Id,
    include_models: bool,
) -> anyhow::Result<Vec<list_draft_specs_query::SelectDraftSpec>> {
    // Use a smaller page size if we're including the models, since they can be quite large.
    let page_size = if include_models { 50 } else { 200 };

    let mut specs = Vec::new();
    let mut after = None;

    loop {
        let vars = list_draft_specs_query::Variables {
            draft_id,
            after: after.take(),
            first: Some(page_size),
            include_models,
        };
        let resp =
            post_graphql::<ListDraftSpecsQuery>(&ctx.rest, ctx.access_token().as_deref(), vars)
                .await
                .context("failed to fetch draft specs")?;

        let Some(draft) = resp.draft else {
            anyhow::bail!("draft {draft_id} does not exist");
        };
        specs.extend(draft.specs.edges.into_iter().map(|edge| edge.node));

        if !draft.specs.page_info.has_next_page {
            return Ok(specs);
        }
        after = draft.specs.page_info.end_cursor;
        assert!(after.is_some(), "draft specs pageInfo missing endCursor");
    }
}

async fn do_create(ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    let row = create_draft(ctx).await?;

    ctx.config.draft = Some(row.id.clone());
    ctx.write_all(Some(row), ())
}

async fn do_delete(ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    let draft_id = ctx.config.selected_draft()?;
    let row = delete_draft(ctx, draft_id).await?;

    ctx.config.draft.take();
    ctx.write_all(Some(row), ())
}

async fn do_describe(ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    #[derive(Serialize)]
    struct Row {
        catalog_name: String,
        detail: Option<String>,
        expect_pub_id: Option<models::Id>,
        last_pub_id: Option<models::Id>,
        spec_type: Option<CatalogType>,
        updated_at: DateTime,
    }
    impl CliOutput for Row {
        type TableAlt = ();
        type CellValue = String;

        fn table_headers(_alt: Self::TableAlt) -> Vec<&'static str> {
            vec!["Name", "Type", "Updated", "Expected Publish ID", "Details"]
        }

        fn into_table_row(self, _alt: Self::TableAlt) -> Vec<Self::CellValue> {
            vec![
                self.catalog_name,
                self.spec_type.map(|t| t.to_string()).unwrap_or_default(),
                self.updated_at
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                match (self.expect_pub_id, self.last_pub_id) {
                    (None, _) => "(any)".to_string(),
                    (Some(expect), Some(last)) if expect == last => expect.to_string(),
                    (Some(expect), Some(last)) => format!("{expect}\n(stale; current is {last})"),
                    (Some(expect), None) => format!("{expect}\n(does not exist)"),
                },
                self.detail.unwrap_or_default(),
            ]
        }
    }
    let draft_id = ctx.config.selected_draft()?;
    let rows = fetch_draft_specs(ctx, draft_id, false)
        .await?
        .into_iter()
        .map(|spec| Row {
            catalog_name: spec.catalog_name.to_string(),
            detail: spec.detail,
            expect_pub_id: spec.expect_pub_id,
            last_pub_id: spec.last_pub_id,
            spec_type: spec.catalog_type,
            updated_at: spec.updated_at,
        });

    ctx.write_all(rows, ())
}

async fn do_list(ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    #[derive(Serialize)]
    struct Row {
        created_at: DateTime,
        detail: Option<String>,
        id: String,
        num_specs: i64,
        updated_at: DateTime,
    }
    impl CliOutput for Row {
        type TableAlt = ();
        type CellValue = JsonCell;

        fn table_headers(_alt: Self::TableAlt) -> Vec<&'static str> {
            vec!["Id", "# of Specs", "Created", "Updated", "Details"]
        }

        fn into_table_row(self, _alt: Self::TableAlt) -> Vec<Self::CellValue> {
            to_table_row(
                self,
                &["/id", "/num_specs", "/created_at", "/updated_at", "/detail"],
            )
        }
    }
    let mut rows = Vec::new();
    let mut after = None;

    loop {
        let vars = list_drafts_query::Variables {
            after: after.take(),
        };
        let drafts =
            post_graphql::<ListDraftsQuery>(&ctx.rest, ctx.access_token().as_deref(), vars)
                .await
                .context("failed to list drafts")?
                .drafts;

        rows.extend(drafts.edges.into_iter().map(|edge| Row {
            created_at: edge.node.created_at,
            detail: edge.node.detail,
            id: edge.node.id.to_string(),
            num_specs: edge.node.num_specs,
            updated_at: edge.node.updated_at,
        }));

        if !drafts.page_info.has_next_page {
            break;
        }
        after = drafts.page_info.end_cursor;
        assert!(after.is_some(), "drafts pageInfo missing endCursor");
    }

    // Decorate the id to mark the selected draft, but only if we're outputting a table
    let cur_draft = ctx
        .config
        .draft
        .map(|id| id.to_string())
        .unwrap_or_default();

    let output_type = ctx.get_output_type();
    let rows = rows.into_iter().map(move |mut row| {
        if output_type == crate::output::OutputType::Table && row.id == cur_draft {
            row.id = format!("{} (selected)", row.id);
        }
        row
    });

    ctx.write_all(rows, ())
}

/// Removes any draft specs that are textually identical to their live specs.
/// Returns the set of specs that were removed from the draft (as a `BTreeSet` so they're ordered).
pub async fn remove_unchanged(
    ctx: &crate::CliContext,
    draft_id: models::Id,
) -> anyhow::Result<BTreeSet<String>> {
    let unchanged = fetch_draft_specs(ctx, draft_id, false)
        .await?
        .into_iter()
        .filter(|spec| spec.is_unchanged)
        .map(|spec| spec.catalog_name)
        .collect();

    let pruned = unstage_draft_specs(ctx, draft_id, unchanged)
        .await
        .context("pruning unchanged specs")?;
    Ok(pruned.into_iter().map(|name| name.to_string()).collect())
}

async fn do_select(
    ctx: &mut crate::CliContext,
    Select { id: select_id }: &Select,
) -> anyhow::Result<()> {
    let vars = fetch_draft_query::Variables { id: *select_id };
    let draft = post_graphql::<FetchDraftQuery>(&ctx.rest, ctx.access_token().as_deref(), vars)
        .await?
        .draft;

    if draft.is_none() {
        anyhow::bail!("draft {select_id} does not exist");
    }

    ctx.config.draft = Some(select_id.clone());
    do_list(ctx).await
}

async fn do_publish(
    ctx: &mut crate::CliContext,
    init_data_plane: Option<&str>,
    dry_run: bool,
) -> anyhow::Result<()> {
    let draft_id = ctx.config.selected_draft()?;

    publish(ctx, init_data_plane, draft_id, dry_run).await?;

    if !dry_run {
        ctx.config.draft.take();
    }
    Ok(())
}

pub async fn publish(
    ctx: &mut crate::CliContext,
    init_data_plane: Option<&str>,
    draft_id: models::Id,
    dry_run: bool,
) -> Result<(), anyhow::Error> {
    #[derive(Deserialize)]
    struct Row {
        id: models::Id,
        logs_token: String,
    }
    // Add the flowctl version to the detail, so we can tell when a publication
    // was created by an old version.
    let detail = format!("Published via flowctl ({})", env!("CARGO_PKG_VERSION"));
    let Row { id, logs_token } = flow_client_next::postgrest::exec(
        ctx.pg
            .from("publications")
            .select("id,logs_token")
            .insert(
                serde_json::json!({
                    "data_plane_name": init_data_plane.unwrap_or_default(),
                    "detail": detail,
                    "draft_id": draft_id,
                    "dry_run": dry_run,
                })
                .to_string(),
            )
            .single(),
        ctx.access_token().as_deref(),
    )
    .await?;
    tracing::info!(%id, %logs_token, %dry_run, "created publication");
    let outcome = crate::poll_while_queued(ctx, "publications", id, &logs_token).await?;

    print_draft_errors(ctx, draft_id).await?;
    if outcome != "success" {
        anyhow::bail!("failed with status: {outcome}");
    }
    tracing::info!(%id, %dry_run, "publication successful");
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_batch_draft_specs() {
        fn spec(model_bytes: Option<usize>) -> DraftSpecInput {
            let model = model_bytes.map(|n| {
                models::RawValue::from_string(format!("\"{}\"", "x".repeat(n - 2))).unwrap()
            });
            DraftSpecInput {
                catalog_name: models::Name::new("acmeCo/anvils"),
                catalog_type: model.as_ref().map(|_| CatalogType::Collection),
                model,
                expect_pub_id: None,
                detail: None,
            }
        }
        fn batch_lens(specs: Vec<DraftSpecInput>) -> Vec<usize> {
            batch_draft_specs(specs).iter().map(Vec::len).collect()
        }

        assert_eq!(batch_lens(Vec::new()), Vec::<usize>::new());

        // Small specs and deletions are bounded by count.
        assert_eq!(
            batch_lens((0..250).map(|_| spec(Some(10))).collect()),
            vec![100, 100, 50]
        );
        assert_eq!(
            batch_lens((0..150).map(|_| spec(None)).collect()),
            vec![100, 50]
        );

        // Larger specs are bounded by model bytes, and a spec larger than
        // the bound is batched on its own.
        assert_eq!(
            batch_lens(vec![
                spec(Some(600_000)),
                spec(Some(400_000)),
                spec(Some(600_000)),
                spec(Some(3_000_000)),
                spec(Some(10)),
            ]),
            vec![2, 1, 1, 1]
        );
    }
}
