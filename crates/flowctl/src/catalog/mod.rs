use crate::{api_exec, config};
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

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct List {
    /// Include "Reads From" / "Writes To" columns in the output.
    #[clap(short = 'f', long)]
    pub flows: bool,
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
    pub async fn run(&self, cfg: &mut config::Config) -> Result<(), anyhow::Error> {
        match &self.cmd {
            Command::List(list) => do_list(cfg, list).await,
            Command::History(history) => do_history(cfg, history).await,
            Command::Draft(draft) => do_draft(cfg, draft).await,
        }
    }
}

async fn do_list(cfg: &config::Config, List { flows }: &List) -> anyhow::Result<()> {
    let mut columns = vec![
        "catalog_name",
        "id",
        "last_pub_user_email",
        "last_pub_user_full_name",
        "last_pub_user_id",
        "spec_type",
        "updated_at",
    ];
    let mut headers = vec!["ID", "Name", "Type", "Updated", "Updated By"];

    if *flows {
        columns.push("reads_from");
        columns.push("writes_to");
        headers.push("Reads From");
        headers.push("Writes To");
    }

    #[derive(Deserialize)]
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
    let rows: Vec<Row> = api_exec(
        cfg.client()?
            .from("live_specs_ext")
            .select(columns.join(",")),
    )
    .await?;

    let mut table = crate::new_table(headers);
    for row in rows {
        let mut out = vec![
            row.id,
            row.catalog_name,
            row.spec_type.unwrap_or_default(),
            row.updated_at.to_string(),
            crate::format_user(
                row.last_pub_user_email,
                row.last_pub_user_full_name,
                row.last_pub_user_id,
            ),
        ];
        if *flows {
            out.push(row.reads_from.iter().flatten().join("\n"));
            out.push(row.writes_to.iter().flatten().join("\n"));
        }

        table.add_row(out);
    }
    println!("{table}");

    Ok(())
}

async fn do_history(cfg: &config::Config, History { name }: &History) -> anyhow::Result<()> {
    #[derive(Deserialize)]
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
    let rows: Vec<Row> = api_exec(
        cfg.client()?
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

    let mut table = crate::new_table(vec![
        "Name",
        "Type",
        "Publication ID",
        "Published",
        "Published By",
        "Details",
    ]);
    for row in rows {
        table.add_row(vec![
            row.catalog_name,
            row.spec_type.unwrap_or_default(),
            if row.pub_id == row.last_pub_id {
                format!("{}\n(current)", row.pub_id)
            } else {
                row.pub_id
            },
            row.published_at.to_string(),
            crate::format_user(row.user_email, row.user_full_name, row.user_id),
            row.detail.unwrap_or_default(),
        ]);
    }
    println!("{table}");

    Ok(())
}

async fn do_draft(
    cfg: &config::Config,
    Draft {
        name,
        delete,
        publication_id,
    }: &Draft,
) -> anyhow::Result<()> {
    let draft_id = cfg.cur_draft()?;

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
            cfg.client()?
                .from("publication_specs_ext")
                .eq("catalog_name", name)
                .eq("pub_id", publication_id)
                .select("catalog_name,last_pub_id,pub_id,spec,spec_type")
                .single(),
        )
        .await?
    } else {
        api_exec(
            cfg.client()?
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

    #[derive(Deserialize)]
    struct Row2 {
        catalog_name: String,
        spec_type: Option<String>,
    }
    let rows: Vec<Row2> = api_exec(
        cfg.client()?
            .from("draft_specs")
            .select("catalog_name,spec_type")
            .upsert(serde_json::to_string(&draft_spec).unwrap())
            .on_conflict("draft_id,catalog_name"),
    )
    .await?;

    let mut table = crate::new_table(vec!["Name", "Type"]);
    for row in rows {
        table.add_row(vec![row.catalog_name, row.spec_type.unwrap_or_default()]);
    }
    println!("{table}");

    Ok(())
}
