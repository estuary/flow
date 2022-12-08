mod publish;
mod pull_specs;
mod test;

use crate::{
    api_exec, controlplane,
    output::{to_table_row, CliOutput, JsonCell},
    source,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

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
    Test(source::SourceArgs),
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
#[derive(Debug, Clone, clap::Args)]
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

impl NameSelector {
    pub fn add_live_specs_filters<'a>(
        &self,
        mut builder: postgrest::Builder<'a>,
    ) -> postgrest::Builder<'a> {
        if !self.prefix.is_empty() {
            let conditions = self
                .prefix
                .iter()
                .map(|prefix| format!("catalog_name.like.\"{prefix}%\""))
                .join(",");
            builder = builder.or(conditions);
        }

        if !self.name.is_empty() {
            let name_sel = self
                .name
                .iter()
                .map(|name| format!("catalog_name.eq.\"{name}\""))
                .join(",");
            builder = builder.or(name_sel);
        }
        builder
    }
}

/// Common selection criteria based on the type of catalog item.
#[derive(Debug, Clone, clap::Args)]
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
    pub fn add_live_specs_filters<'a>(
        &self,
        mut builder: postgrest::Builder<'a>,
    ) -> postgrest::Builder<'a> {
        let all = &[
            (CatalogSpecType::Capture, self.captures),
            (CatalogSpecType::Collection, self.collections),
            (CatalogSpecType::Materialization, self.materializations),
            (CatalogSpecType::Test, self.tests),
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
            // If no types were explicitly included, then we can just add
            // an `neq.` for each explicitly excluded type, since postgrest
            // implicitly applies AND logic there.
            for (ty, _) in all.iter().filter(|(_, inc)| *inc == Some(false)) {
                builder = builder.neq("spec_type", ty);
            }
        }
        builder
    }

    pub fn has_any_include_types(&self) -> bool {
        self.captures == Some(true)
            || self.collections == Some(true)
            || self.materializations == Some(true)
            || self.tests == Some(true)
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CatalogSpecType {
    Capture,
    Collection,
    Materialization,
    Test,
}

impl std::fmt::Display for CatalogSpecType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl std::convert::AsRef<str> for CatalogSpecType {
    fn as_ref(&self) -> &str {
        // These strings match what's used by serde, and also match the definitions in the database.
        match *self {
            CatalogSpecType::Capture => "capture",
            CatalogSpecType::Collection => "collection",
            CatalogSpecType::Materialization => "materialization",
            CatalogSpecType::Test => "test",
        }
    }
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct List {
    /// Include "Reads From" / "Writes To" columns in the output.
    #[clap(short = 'f', long)]
    pub flows: bool,
    #[clap(flatten)]
    pub prefix_selector: NameSelector,
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
    pub publication_id: Option<String>,
}

impl Catalog {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> Result<(), anyhow::Error> {
        match &self.cmd {
            Command::List(list) => do_list(ctx, list).await,
            Command::PullSpecs(pull) => pull_specs::do_pull_specs(ctx, pull).await,
            Command::Publish(publish) => publish::do_publish(ctx, publish).await,
            Command::Test(source) => test::do_test(ctx, source).await,
            Command::History(history) => do_history(ctx, history).await,
            Command::Draft(draft) => do_draft(ctx, draft).await,
        }
    }
}

pub async fn fetch_live_specs(
    cp_client: controlplane::Client,
    list: &List,
    columns: Vec<&'static str>,
) -> anyhow::Result<Vec<LiveSpecRow>> {
    let builder = cp_client.from("live_specs_ext").select(columns.join(","));
    let builder = list.type_selector.add_live_specs_filters(builder);
    let builder = list.prefix_selector.add_live_specs_filters(builder);

    let rows = api_exec(builder).await?;
    Ok(rows)
}

#[derive(Deserialize, Serialize)]
pub struct LiveSpecRow {
    pub catalog_name: String,
    pub id: String,
    pub last_pub_user_email: Option<String>,
    pub last_pub_user_full_name: Option<String>,
    pub last_pub_user_id: Option<uuid::Uuid>,
    pub spec_type: Option<CatalogSpecType>,
    pub updated_at: crate::Timestamp,
    pub reads_from: Option<Vec<String>>,
    pub writes_to: Option<Vec<String>>,
    pub spec: Option<Box<serde_json::value::RawValue>>,
}

impl LiveSpecRow {
    fn parse_spec<T: serde::de::DeserializeOwned>(&self) -> anyhow::Result<T> {
        let spec = self.spec.as_ref().ok_or_else(|| {
            anyhow::anyhow!("missing spec for catalog item: '{}'", self.catalog_name)
        })?;
        let parsed = serde_json::from_str::<T>(spec.get())?;
        Ok(parsed)
    }
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
            self.id,
            self.catalog_name,
            self.spec_type
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| String::from("DELETED")),
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

/// Collects an iterator of `LiveSpecRow`s into a `models::Catalog`. The rows must
/// all have a `spec` unless they are deleted (`spec_type = null`).
pub fn collect_specs(
    rows: impl IntoIterator<Item = LiveSpecRow>,
) -> anyhow::Result<models::Catalog> {
    let mut catalog = models::Catalog::default();

    for row in rows {
        match row.spec_type {
            Some(CatalogSpecType::Capture) => {
                let cap = row.parse_spec::<models::CaptureDef>()?;
                catalog
                    .captures
                    .insert(models::Capture::new(row.catalog_name), cap);
            }
            Some(CatalogSpecType::Collection) => {
                let collection = row.parse_spec::<models::CollectionDef>()?;
                catalog
                    .collections
                    .insert(models::Collection::new(row.catalog_name), collection);
            }
            Some(CatalogSpecType::Materialization) => {
                let materialization = row.parse_spec::<models::MaterializationDef>()?;
                catalog.materializations.insert(
                    models::Materialization::new(row.catalog_name),
                    materialization,
                );
            }
            Some(CatalogSpecType::Test) => {
                let test = row.parse_spec::<Vec<models::TestStep>>()?;
                catalog
                    .tests
                    .insert(models::Test::new(row.catalog_name), test);
            }
            None => {
                tracing::debug!(catalog_name = %row.catalog_name, "ignoring deleted spec from list results");
            }
        }
    }
    Ok(catalog)
}

async fn do_list(ctx: &mut crate::CliContext, list_args: &List) -> anyhow::Result<()> {
    let mut columns = vec![
        "catalog_name",
        "id",
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
    let client = ctx.controlplane_client()?;
    let rows = fetch_live_specs(client, list_args, columns).await?;

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
    let rows: Vec<Row> = api_exec(
        ctx.controlplane_client()?
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
        last_pub_id: String,
        pub_id: String,
        spec: Box<RawValue>,
        spec_type: Option<String>,
    }

    let Row {
        catalog_name,
        last_pub_id,
        pub_id,
        mut spec,
        mut spec_type,
    } = if let Some(publication_id) = publication_id {
        api_exec(
            ctx.controlplane_client()?
                .from("publication_specs_ext")
                .eq("catalog_name", name)
                .eq("pub_id", publication_id)
                .select("catalog_name,last_pub_id,pub_id,spec,spec_type")
                .single(),
        )
        .await?
    } else {
        api_exec(
            ctx.controlplane_client()?
                .from("live_specs")
                .eq("catalog_name", name)
                .select("catalog_name,last_pub_id,pub_id:last_pub_id,spec,spec_type")
                .single(),
        )
        .await?
    };
    tracing::info!(%catalog_name, %last_pub_id, %pub_id, ?spec_type, "resolved live catalog spec");

    if *delete {
        spec = RawValue::from_string("null".to_string()).unwrap();
        spec_type = None;
    }

    // Build up the array of `draft_specs` to upsert.
    #[derive(Serialize, Debug)]
    struct DraftSpec {
        draft_id: String,
        catalog_name: String,
        spec_type: Option<String>,
        spec: Box<RawValue>,
        expect_pub_id: String,
    }
    let draft_spec = DraftSpec {
        draft_id,
        catalog_name,
        spec_type,
        spec,
        expect_pub_id: last_pub_id,
    };
    tracing::debug!(?draft_spec, "inserting draft");

    let rows: Vec<SpecSummaryItem> = api_exec(
        ctx.controlplane_client()?
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
    pub spec_type: Option<String>,
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
