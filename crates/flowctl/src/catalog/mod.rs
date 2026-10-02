mod delete;
mod history;
mod list;
mod publish;
mod pull_specs;
mod status;
mod test;

use self::list::{List, do_list};
use crate::graphql::{Id, JSON, Name, post_graphql};
use crate::output::{CliOutput, JsonCell, to_table_row};
use anyhow::Context;
use models::CatalogType;
use serde::Serialize;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Catalog {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum Command {
    /// List catalog specifications.
    List(List),

    /// Delete catalog specifications.
    ///
    /// Permanently deletes catalog specifications.
    /// **WARNING:** deleting a task is permanent and cannot be undone.
    Delete(delete::Delete),

    /// Pull down catalog specifications into a local directory.
    ///
    /// Writes catalog specifications into a local directory so that
    /// you can edit them locally. Accepts the same arguments as `flowctl catalog list`
    /// for selecting specs from the catalog. By default, specs will be written to
    /// nested subdirectories within the current directory, based on the catalog
    /// name of each selected spec. You may instead pass `--no-expand-dirs` to
    /// instead write the spec (and all associated endpoint config and schema resources)
    /// directly to the current directory.
    PullSpecs(pull_specs::PullSpecs),
    /// Publish catalog specifications
    ///
    /// Updates the running tasks, collections, and tests based on specifications in a
    /// local directory or a remote URL.
    Publish(publish::Publish),
    /// Test catalog specifications
    ///
    /// Runs catalog tests based on specifications in a
    /// local directory or a remote URL. This
    Test(test::TestArgs),
    /// History of a catalog specification.
    ///
    /// Print all historical publications of catalog specifications.
    History(history::History),
    /// Add a catalog specification to your current draft.
    ///
    /// A copy of the current specification is added to your draft.
    /// If --publication-id, then the specification as it was at that
    /// publication is added.
    ///
    /// Or, if --delete then the publication is marked for deletion
    /// upon publication of your draft.
    ///
    /// The added draft specification is marked to expect that the latest
    /// publication of the live specification is still current. Put
    /// differently, if some other draft publication updates the live
    /// specification between now and when you eventually publish your draft,
    /// then your publication will fail so you don't accidently clobber the
    /// updated specification.
    ///
    /// Once in your draft, use `draft develop` and `draft author` to
    /// develop and author updates to the specification.
    Draft(Draft),

    /// Print status information for a given task or collection (beta).
    ///
    /// Note: This command is still in beta and the output is likely to change in the future.
    ///
    /// The status shows the current state of the live spec, as known to the
    /// control plane. This does not yet include _shard_ status from the data
    /// plane, so not all failures will be visible here.
    Status(status::Status),
}

/// Common selection criteria based on the spec name.
/// Note that at most one of `name` or `prefix` can be non-empty.
/// If both are specified, then `fetch_live_specs` will panic.
#[derive(Default, Debug, Clone, clap::Args)]
pub struct NameSelector {
    /// Select a spec by name. May be provided multiple times.
    #[clap(long)]
    pub name: Vec<String>,
    /// Select catalog items under the given prefix
    ///
    /// Selects all items whose name begins with the prefix.
    #[clap(long, conflicts_with = "name")]
    pub prefix: Vec<String>,
}

/// Common selection criteria based on the type of catalog item.
#[derive(Default, Debug, Clone, clap::Args)]
#[group(multiple = false)]
pub struct SpecTypeSelector {
    /// Whether to include captures in the selection
    ///
    /// If true, or if no value was given, then captures will
    /// be included. You can also use `--captures=false` to exclude
    /// captures.
    #[clap(long)]
    pub captures: bool,
    /// Whether to include collections in the selection
    ///
    /// If true, or if no value was given, then collections will
    /// be included. You can also use `--collections=false` to exclude
    /// collections.
    #[clap(long)]
    pub collections: bool,
    /// Whether to include materializations in the selection
    ///
    /// If true, or if no value was given, then materializations will
    /// be included. You can also use `--materializations=false` to exclude
    /// materializations.
    #[clap(long)]
    pub materializations: bool,
    /// Whether to include tests in the selection
    ///
    /// If true, or if no value was given, then tests will
    /// be included. You can also use `--tests=false` to exclude
    /// tests.
    #[clap(long)]
    pub tests: bool,
}

impl SpecTypeSelector {
    pub fn get_single_type_selection(&self) -> Option<models::CatalogType> {
        let all = [
            (models::CatalogType::Capture, self.captures),
            (models::CatalogType::Collection, self.collections),
            (models::CatalogType::Materialization, self.materializations),
            (models::CatalogType::Test, self.tests),
        ];
        all.into_iter()
            .find(|(_, selected)| *selected)
            .map(|(ty, _)| ty)
    }
}

/// Common selection criteria based on the data plane name.
#[derive(Default, Debug, Clone, clap::Args)]
pub struct DataPlaneSelector {
    /// Selects only specs assigned to the given data plane.
    #[clap(long)]
    pub data_plane_name: Option<String>,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Draft {
    /// Catalog name to add to your draft.
    #[clap(long)]
    pub name: String,
    /// If set, add the deletion of this specification to your draft.
    #[clap(long)]
    pub delete: bool,
    // Populate the draft from a specific version rather than the latest.
    //
    // The publication ID can be found in the history of the specification.
    // If not set then the latest version is used.
    //
    // You can use the previous publication to "revert" a specification
    // to a known-good version.
    #[clap(long, conflicts_with("delete"))]
    pub publication_id: Option<models::Id>,
}

impl Catalog {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> Result<(), anyhow::Error> {
        match &self.cmd {
            Command::List(list) => do_list(ctx, list).await,
            Command::Delete(del) => delete::do_delete(ctx, del).await,
            Command::PullSpecs(pull) => pull_specs::do_pull_specs(ctx, pull).await,
            Command::Publish(publish) => publish::do_publish(ctx, publish).await,
            Command::Test(source) => test::do_test(ctx, source).await,
            Command::History(history) => history::do_history(ctx, history).await,
            Command::Draft(draft) => do_draft(ctx, draft).await,
            Command::Status(status) => status::do_controller_status(ctx, status).await,
        }
    }
}

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/catalog/draft-query.graphql",
    extern_enums("CatalogType")
)]
struct DraftFromPublicationQuery;

async fn do_draft(
    ctx: &mut crate::CliContext,
    Draft {
        name,
        delete,
        publication_id,
    }: &Draft,
) -> anyhow::Result<()> {
    let draft_id = ctx.config.selected_draft()?;

    let (last_pub_id, mut spec, spec_type) = if let Some(publication_id) = publication_id {
        let mut response = post_graphql::<DraftFromPublicationQuery>(
            &ctx.rest,
            ctx.access_token().as_deref(),
            draft_from_publication_query::Variables {
                name: models::Name::new(name),
                publication_id: *publication_id,
            },
        )
        .await?;
        let reference = response
            .live_specs
            .edges
            .pop()
            .with_context(|| format!("catalog specification '{name}' not found"))?
            .node;
        let historical = reference
            .publication_for_id
            .with_context(|| format!("publication {publication_id} of '{name}' not found"))?;
        let spec_type = historical.catalog_type.with_context(|| {
            format!("publication {publication_id} of '{name}' has no recorded catalog type")
        })?;
        // A revert is conditional on the current publication, including a deletion,
        // rather than the historical publication whose model is being restored.
        let last_pub_id = reference
            .last_publication
            .with_context(|| format!("latest publication of '{name}' not found"))?
            .publication_id;
        (last_pub_id, historical.model, spec_type)
    } else {
        let live = list::fetch_live_specs(
            ctx,
            List {
                name_selector: NameSelector {
                    name: vec![name.clone()],
                    ..Default::default()
                },
                include_models: !*delete,
                ..Default::default()
            },
        )
        .await?
        .pop()
        .and_then(|reference| reference.live_spec)
        .with_context(|| format!("live catalog specification '{name}' not found"))?;
        (live.last_pub_id, live.model, live.catalog_type)
    };
    tracing::info!(catalog_name = %name, %last_pub_id, ?publication_id, ?spec_type, "resolved catalog spec");

    if *delete {
        spec = None;
    }

    // Staging replaces all editable fields, so carry forward the existing description.
    let detail = crate::draft::fetch_draft_specs(ctx, draft_id, false)
        .await?
        .into_iter()
        .find(|spec| spec.catalog_name.as_str() == name.as_str())
        .and_then(|spec| spec.detail);

    let draft_spec = crate::draft::DraftSpecInput {
        catalog_name: models::Name::new(name),
        // A drafted deletion has neither a model nor a type.
        catalog_type: spec.is_some().then_some(spec_type),
        model: spec,
        expect_pub_id: Some(last_pub_id),
        detail,
    };
    tracing::debug!(?draft_spec, "staging draft spec");

    crate::draft::stage_draft_specs(ctx, draft_id, vec![draft_spec]).await?;

    ctx.write_all(
        Some(SpecSummaryItem {
            catalog_name: name.clone(),
            spec_type,
        }),
        (),
    )
}

/// Used for simple listings of specs, such as for listing the specs contained within a draft.
#[derive(Serialize)]
pub struct SpecSummaryItem {
    pub catalog_name: String,
    pub spec_type: CatalogType,
}

impl CliOutput for SpecSummaryItem {
    type TableAlt = ();
    type CellValue = JsonCell;

    fn table_headers(_alt: Self::TableAlt) -> Vec<&'static str> {
        vec!["Name", "Type"]
    }

    fn into_table_row(self, _alt: Self::TableAlt) -> Vec<Self::CellValue> {
        to_table_row(self, &["/catalog_name", "/spec_type"])
    }
}
