use crate::api_exec;
use crate::output::{to_table_row, CliOutput, JsonCell};
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

/// Common selection criteria based on a prefix of the item name.
#[derive(Debug, clap::Args)]
pub struct PrefixSelector {
    /// Select catalog items under the given prefix
    ///
    /// Selects all items whose name begins with the prefix.
    /// Can be provided multiple times to select items under multiple
    /// prefixes.
    #[clap(long)]
    pub prefix: Vec<String>,
}

impl PrefixSelector {
    pub fn add_live_specs_filters<'a>(
        &self,
        builder: postgrest::Builder<'a>,
    ) -> postgrest::Builder<'a> {
        if !self.prefix.is_empty() {
            let conditions = self
                .prefix
                .iter()
                .map(|prefix| format!("catalog_name.like.\"{prefix}%\""))
                .join(",");
            builder.or(conditions)
        } else {
            builder
        }
    }
}

/// Common selection criteria based on the type of catalog item.
#[derive(Debug, clap::Args)]
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

#[derive(Clone, Copy)]
enum CatalogSpecType {
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
    pub prefix_selector: PrefixSelector,
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
            Command::History(history) => do_history(ctx, history).await,
            Command::Draft(draft) => do_draft(ctx, draft).await,
        }
    }
}

async fn do_list(
    ctx: &mut crate::CliContext,
    List {
        flows,
        type_selector,
        prefix_selector,
    }: &List,
) -> anyhow::Result<()> {
    let mut columns = vec![
        "catalog_name",
        "id",
        "last_pub_user_email",
        "last_pub_user_full_name",
        "last_pub_user_id",
        "spec_type",
        "updated_at",
    ];
    if *flows {
        columns.push("reads_from");
        columns.push("writes_to");
    }

    #[derive(Deserialize, Serialize)]
    struct Row {
        catalog_name: String,
        id: String,
        last_pub_user_email: Option<String>,
        last_pub_user_full_name: Option<String>,
        last_pub_user_id: Option<uuid::Uuid>,
        spec_type: Option<String>,
        updated_at: crate::Timestamp,
        reads_from: Option<Vec<String>>,
        writes_to: Option<Vec<String>>,
    }
    impl crate::output::CliOutput for Row {
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
                self.spec_type.unwrap_or_default(),
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
    let client = ctx.controlplane_client()?;
    let builder = client.from("live_specs_ext").select(columns.join(","));
    let builder = type_selector.add_live_specs_filters(builder);
    let builder = prefix_selector.add_live_specs_filters(builder);

    let rows: Vec<Row> = api_exec(builder).await?;

    ctx.write_all(rows, *flows)
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
