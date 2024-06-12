mod delete;
mod publish;
mod pull_specs;
mod test;

use crate::{
    api_exec, api_exec_paginated, controlplane,
    output::{to_table_row, CliOutput, JsonCell},
};
use anyhow::Context;
use futures::stream::{FuturesUnordered, StreamExt};
use itertools::Itertools;
use models::{CatalogType, RawValue};
use serde::{Deserialize, Serialize};

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
    History(History),
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
    /// Can be provided multiple times to select items under multiple
    /// prefixes.
    #[clap(long, conflicts_with = "name")]
    pub prefix: Vec<String>,
}

/// Common selection criteria based on the type of catalog item.
#[derive(Default, Debug, Clone, clap::Args)]
pub struct SpecTypeSelector {
    /// Whether to include captures in the selection
    ///
    /// If true, or if no value was given, then captures will
    /// be included. You can also use `--captures=false` to exclude
    /// captures.
    #[clap(long, default_missing_value = "true", value_name = "INCLUDE")]
    pub captures: Option<bool>,
    /// Whether to include collections in the selection
    ///
    /// If true, or if no value was given, then collections will
    /// be included. You can also use `--collections=false` to exclude
    /// collections.
    #[clap(long, default_missing_value = "true", value_name = "INCLUDE")]
    pub collections: Option<bool>,
    /// Whether to include materializations in the selection
    ///
    /// If true, or if no value was given, then materializations will
    /// be included. You can also use `--materializations=false` to exclude
    /// materializations.
    #[clap(long, default_missing_value = "true", value_name = "INCLUDE")]
    pub materializations: Option<bool>,
    /// Whether to include tests in the selection
    ///
    /// If true, or if no value was given, then tests will
    /// be included. You can also use `--tests=false` to exclude
    /// tests.
    #[clap(long, default_missing_value = "true", value_name = "INCLUDE")]
    pub tests: Option<bool>,
}

impl SpecTypeSelector {
    /// Adds postgrest query parameters based on the arugments provided to filter specs based on the `spec_type` column.
    pub fn add_spec_type_filters(&self, mut builder: postgrest::Builder) -> postgrest::Builder {
        let all = &[
            (CatalogType::Capture.as_ref(), self.captures),
            (CatalogType::Collection.as_ref(), self.collections),
            (CatalogType::Materialization.as_ref(), self.materializations),
            (CatalogType::Test.as_ref(), self.tests),
        ];

        // If any of the types were explicitly included, then we'll add
        // an `or.` that only includes items for each explicitly included type.
        if self.has_any_include_types() {
            let expr = all
                .iter()
                .filter(|(_, inc)| inc.unwrap_or(false))
                .map(|(ty, _)| format!("spec_type.eq.{ty}"))
                .join(",");
            builder = builder.or(expr);
        } else {
            // If no types were explicitly included, then we can just filter out the types we _don't_ want.
            for (ty, _) in all.iter().filter(|(_, inc)| *inc == Some(false)) {
                builder = builder.neq("spec_type", ty);
            }
            // We need to use `IS NOT NULL` to filter out deleted specs, rather than using `neq`.
            // Postgrest implicitly applies AND logic for these.
            builder = builder.not("is", "spec_type", "null");
        }
        builder
    }

    fn has_any_include_types(&self) -> bool {
        self.captures == Some(true)
            || self.collections == Some(true)
            || self.materializations == Some(true)
            || self.tests == Some(true)
    }
}

#[derive(Default, Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct List {
    /// Include "Reads From" / "Writes To" columns in the output.
    #[clap(short = 'f', long)]
    pub flows: bool,
    #[clap(flatten)]
    pub name_selector: NameSelector,
    #[clap(flatten)]
    pub type_selector: SpecTypeSelector,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct History {
    /// Catalog name or prefix to retrieve history for.
    #[clap(long)]
    pub name: String,
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
            Command::History(history) => do_history(ctx, history).await,
            Command::Draft(draft) => do_draft(ctx, draft).await,
        }
    }
}

/// Fetches `LiveSpecRow`s from the `live_specs_ext` view.
/// This may make multiple requests as necessary.
///
/// # Panics
/// If the name_selector `name` and `prefix` are both non-empty.
pub async fn fetch_live_specs<T>(
    cp_client: controlplane::Client,
    list: &List,
    columns: Vec<&'static str>,
) -> anyhow::Result<Vec<T>>
where
    T: serde::de::DeserializeOwned + Send + Sync + 'static,
{
    // When fetching by name or prefix, we break the requested names into chunks
    // and send a separate request for each. This is to avoid overflowing the
    // URL length limit in postgREST.
    const BATCH_SIZE: usize = 25;

    if !list.name_selector.name.is_empty() && !list.name_selector.prefix.is_empty() {
        panic!("cannot specify both 'name' and 'prefix' for filtering live specs");
    }

    let builder = cp_client.from("live_specs_ext").select(columns.join(","));
    let builder = list.type_selector.add_spec_type_filters(builder);

    // Drive the actual request(s) based on the name selector, since the arguments there may
    // necessitate multiple requests.
    if !list.name_selector.name.is_empty() {
        let mut stream = list
            .name_selector
            .name
            .chunks(BATCH_SIZE)
            .map(|batch| {
                // These weird extra scopes are to convince the borrow checker
                // that we're moving the cloned builder into the async block,
                // not the original builder. And also to clarify that we're not
                // moving `batch` into the async block.
                let builder = builder.clone().in_("catalog_name", batch);
                async move {
                    // No need for pagination because we're paginating the inputs.
                    api_exec::<Vec<T>>(builder).await
                }
            })
            .collect::<FuturesUnordered<_>>();
        let mut rows = Vec::with_capacity(list.name_selector.name.len());
        while let Some(result) = stream.next().await {
            rows.extend(result.context("executing live_specs_ext fetch")?);
        }
        Ok(rows)
    } else if !list.name_selector.prefix.is_empty() {
        let mut stream = list
            .name_selector
            .prefix
            .chunks(BATCH_SIZE)
            .map(|batch| async {
                let conditions = batch
                    .iter()
                    .map(|prefix| format!("catalog_name.like.\"{prefix}%\""))
                    .join(",");
                // We need to paginate the results, since prefixes can match many rows.
                api_exec_paginated::<T>(builder.clone().or(conditions)).await
            })
            .collect::<FuturesUnordered<_>>();

        let mut rows = Vec::with_capacity(list.name_selector.name.len());
        while let Some(result) = stream.next().await {
            rows.extend(result.context("executing live_specs_ext fetch")?);
        }
        Ok(rows)
    } else {
        // For anything else, just execute a single request and paginate the results.
        api_exec_paginated::<T>(builder).await
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct LiveSpecRow {
    pub catalog_name: String,
    pub id: models::Id,
    pub updated_at: crate::Timestamp,
    pub spec_type: CatalogType,
    pub last_pub_id: models::Id,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_pub_user_email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_pub_user_full_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_pub_user_id: Option<uuid::Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reads_from: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub writes_to: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<RawValue>,
}

impl crate::output::CliOutput for LiveSpecRow {
    type TableAlt = bool;
    type CellValue = String;

    fn table_headers(flows: Self::TableAlt) -> Vec<&'static str> {
        let mut headers = vec!["ID", "Name", "Type", "Updated", "Updated By"];
        if flows {
            headers.push("Reads From");
            headers.push("Writes To");
        }
        headers
    }

    fn into_table_row(self, flows: Self::TableAlt) -> Vec<Self::CellValue> {
        let mut out = vec![
            self.id.to_string(),
            self.catalog_name,
            self.spec_type.to_string(),
            self.updated_at.to_string(),
            crate::format_user(
                self.last_pub_user_email,
                self.last_pub_user_full_name,
                self.last_pub_user_id,
            ),
        ];
        if flows {
            out.push(self.reads_from.iter().flatten().join("\n"));
            out.push(self.writes_to.iter().flatten().join("\n"));
        }
        out
    }
}

/// Trait that's common to database rows of catalog specs, which can be turned into a bundled catalog.
pub trait SpecRow {
    fn catalog_name(&self) -> &str;
    fn spec_type(&self) -> CatalogType;
    fn spec(&self) -> Option<&RawValue>;
    fn expect_pub_id(&self) -> Option<models::Id>;
}

impl SpecRow for LiveSpecRow {
    fn catalog_name(&self) -> &str {
        &self.catalog_name
    }
    fn spec_type(&self) -> CatalogType {
        self.spec_type
    }
    fn spec(&self) -> Option<&RawValue> {
        self.spec.as_ref()
    }
    fn expect_pub_id(&self) -> Option<models::Id> {
        Some(self.last_pub_id)
    }
}

/// Collects an iterator of `SpecRow`s into a `tables::DraftCatalog`.
pub fn collect_specs(
    rows: impl IntoIterator<Item = impl SpecRow>,
) -> anyhow::Result<tables::DraftCatalog> {
    let mut catalog = tables::DraftCatalog::default();

    fn parse<T: serde::de::DeserializeOwned>(
        model: Option<&RawValue>,
    ) -> anyhow::Result<Option<T>> {
        if let Some(model) = model {
            Ok(Some(serde_json::from_str::<T>(model.get())?))
        } else {
            Ok(None)
        }
    }

    for row in rows {
        let scope = url::Url::parse(&format!("flow://control/{}", row.catalog_name())).unwrap();

        match row.spec_type() {
            CatalogType::Capture => {
                catalog.captures.insert_row(
                    models::Capture::new(row.catalog_name()),
                    &scope,
                    row.expect_pub_id(),
                    parse::<models::CaptureDef>(row.spec())?,
                );
            }
            CatalogType::Collection => {
                catalog.collections.insert_row(
                    models::Collection::new(row.catalog_name()),
                    &scope,
                    row.expect_pub_id(),
                    parse::<models::CollectionDef>(row.spec())?,
                );
            }
            CatalogType::Materialization => {
                catalog.materializations.insert_row(
                    models::Materialization::new(row.catalog_name()),
                    &scope,
                    row.expect_pub_id(),
                    parse::<models::MaterializationDef>(row.spec())?,
                );
            }
            CatalogType::Test => {
                catalog.tests.insert_row(
                    models::Test::new(row.catalog_name()),
                    &scope,
                    row.expect_pub_id(),
                    parse::<models::TestDef>(row.spec())?,
                );
            }
        }
    }
    Ok(catalog)
}

async fn do_list(ctx: &mut crate::CliContext, list_args: &List) -> anyhow::Result<()> {
    let mut columns = vec![
        "catalog_name",
        "id",
        "last_pub_id",
        "last_pub_user_email",
        "last_pub_user_full_name",
        "last_pub_user_id",
        "spec_type",
        "updated_at",
    ];
    if list_args.flows {
        columns.push("reads_from");
        columns.push("writes_to");
    }
    let client = ctx.controlplane_client().await?;
    let rows = fetch_live_specs::<LiveSpecRow>(client, list_args, columns).await?;

    ctx.write_all(rows, list_args.flows)
}

async fn do_history(ctx: &mut crate::CliContext, History { name }: &History) -> anyhow::Result<()> {
    #[derive(Deserialize, Serialize)]
    struct Row {
        catalog_name: String,
        detail: Option<String>,
        last_pub_id: String,
        pub_id: String,
        published_at: crate::Timestamp,
        spec_type: Option<String>,
        user_email: Option<String>,
        user_full_name: Option<String>,
        user_id: Option<uuid::Uuid>,
    }

    impl crate::output::CliOutput for Row {
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
                self.catalog_name,
                self.spec_type.unwrap_or_default(),
                if self.pub_id == self.last_pub_id {
                    format!("{}\n(current)", self.pub_id)
                } else {
                    self.pub_id
                },
                self.published_at.to_string(),
                crate::format_user(self.user_email, self.user_full_name, self.user_id),
                self.detail.unwrap_or_default(),
            ]
        }
    }
    let rows: Vec<Row> = api_exec_paginated(
        ctx.controlplane_client()
            .await?
            .from("publication_specs_ext")
            .like("catalog_name", format!("{name}%"))
            .select(
                vec![
                    "catalog_name",
                    "detail",
                    "last_pub_id",
                    "pub_id",
                    "published_at",
                    "spec_type",
                    "user_email",
                    "user_full_name",
                    "user_id",
                ]
                .join(","),
            ),
    )
    .await?;

    ctx.write_all(rows, ())
}

async fn do_draft(
    ctx: &mut crate::CliContext,
    Draft {
        name,
        delete,
        publication_id,
    }: &Draft,
) -> anyhow::Result<()> {
    let draft_id = ctx.config().cur_draft()?;

    #[derive(Deserialize)]
    struct Row {
        catalog_name: String,
        last_pub_id: models::Id,
        pub_id: models::Id,
        spec: Option<RawValue>,
        spec_type: CatalogType,
    }

    let Row {
        catalog_name,
        last_pub_id,
        pub_id,
        mut spec,
        spec_type,
    } = if let Some(publication_id) = publication_id {
        api_exec(
            ctx.controlplane_client()
                .await?
                .from("publication_specs_ext")
                .eq("catalog_name", name)
                .eq("pub_id", publication_id.to_string())
                .select("catalog_name,last_pub_id,pub_id,spec,spec_type")
                .single(),
        )
        .await?
    } else {
        api_exec(
            ctx.controlplane_client()
                .await?
                .from("live_specs")
                .eq("catalog_name", name)
                .not("is", "spec_type", "null")
                .select("catalog_name,last_pub_id,pub_id:last_pub_id,spec,spec_type")
                .single(),
        )
        .await?
    };
    tracing::info!(%catalog_name, %last_pub_id, %pub_id, ?spec_type, "resolved live catalog spec");

    if *delete {
        spec = None;
    }

    // Build up the array of `draft_specs` to upsert.
    #[derive(Serialize, Debug)]
    struct DraftSpec {
        draft_id: models::Id,
        catalog_name: String,
        spec_type: CatalogType,
        spec: Option<RawValue>,
        expect_pub_id: models::Id,
    }
    let draft_spec = DraftSpec {
        draft_id,
        catalog_name,
        spec_type,
        spec,
        expect_pub_id: last_pub_id,
    };
    tracing::debug!(?draft_spec, "inserting draft");

    let rows: Vec<SpecSummaryItem> = api_exec_paginated(
        ctx.controlplane_client()
            .await?
            .from("draft_specs")
            .select("catalog_name,spec_type")
            .upsert(serde_json::to_string(&draft_spec).unwrap())
            .on_conflict("draft_id,catalog_name"),
    )
    .await?;

    ctx.write_all(rows, ())
}

/// Used for simple listings of specs, such as for listing the specs contained within a draft.
#[derive(Deserialize, Serialize)]
pub struct SpecSummaryItem {
    pub catalog_name: String,
    pub spec_type: CatalogType,
}

impl SpecSummaryItem {
    fn summarize_catalog(catalog: tables::DraftCatalog) -> Vec<SpecSummaryItem> {
        let mut summary = Vec::new();
        let tables::DraftCatalog {
            captures,
            collections,
            materializations,
            tests,
            ..
        } = catalog;

        summary.extend(captures.into_iter().map(|r| SpecSummaryItem {
            catalog_name: r.capture.to_string(),
            spec_type: CatalogType::Capture,
        }));
        summary.extend(collections.into_iter().map(|r| SpecSummaryItem {
            catalog_name: r.collection.to_string(),
            spec_type: CatalogType::Collection,
        }));
        summary.extend(materializations.into_iter().map(|r| SpecSummaryItem {
            catalog_name: r.materialization.to_string(),
            spec_type: CatalogType::Materialization,
        }));
        summary.extend(tests.into_iter().map(|r| SpecSummaryItem {
            catalog_name: r.test.to_string(),
            spec_type: CatalogType::Test,
        }));

        summary
    }
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
