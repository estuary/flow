use crate::{api_exec, config};
use serde::Deserialize;

mod author;
use author::do_author;

mod develop;
use develop::do_develop;

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
    Publish,
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
    Test,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Select {
    #[clap(long)]
    id: String,
}

impl Draft {
    pub async fn run(&self, cfg: &mut config::Config) -> Result<(), anyhow::Error> {
        match &self.cmd {
            Command::Author(author) => do_author(cfg, author).await,
            Command::Create => do_create(cfg).await,
            Command::Delete => do_delete(cfg).await,
            Command::Describe => do_describe(cfg).await,
            Command::Develop(develop) => do_develop(cfg, develop).await,
            Command::List => do_list(cfg).await,
            Command::Publish => do_publish(cfg, false).await,
            Command::Select(select) => do_select(cfg, select).await,
            Command::Test => do_publish(cfg, true).await,
        }
    }
}

async fn do_create(cfg: &mut config::Config) -> anyhow::Result<()> {
    #[derive(Deserialize)]
    struct Row {
        id: String,
        created_at: crate::Timestamp,
    }
    let row: Row = api_exec(
        cfg.client()?
            .from("drafts")
            .select("id, created_at")
            .insert(serde_json::json!({"detail": "Created by flowctl"}).to_string())
            .single(),
    )
    .await?;

    let mut table = crate::new_table(vec!["Created Draft ID", "Created"]);
    table.add_row(vec![row.id.clone(), row.created_at.to_string()]);
    println!("{table}");

    cfg.draft = Some(row.id);
    Ok(())
}

async fn do_delete(cfg: &mut config::Config) -> anyhow::Result<()> {
    #[derive(Deserialize)]
    struct Row {
        id: String,
        updated_at: crate::Timestamp,
    }
    let row: Row = api_exec(
        cfg.client()?
            .from("drafts")
            .select("id,updated_at")
            .delete()
            .eq("id", cfg.cur_draft()?)
            .single(),
    )
    .await?;

    let mut table = crate::new_table(vec!["Deleted Draft ID", "Last Updated"]);
    table.add_row(vec![row.id, row.updated_at.to_string()]);
    println!("{table}");

    cfg.draft = None;
    Ok(())
}

async fn do_describe(cfg: &config::Config) -> anyhow::Result<()> {
    #[derive(Deserialize)]
    struct Row {
        catalog_name: String,
        detail: Option<String>,
        expect_pub_id: Option<String>,
        last_pub_id: Option<String>,
        spec_type: Option<String>,
        updated_at: crate::Timestamp,
    }
    let rows: Vec<Row> = api_exec(
        cfg.client()?
            .from("draft_specs_ext")
            .select(
                vec![
                    "catalog_name",
                    "detail",
                    "expect_pub_id",
                    "last_pub_id",
                    "spec_type",
                    "updated_at",
                ]
                .join(","),
            )
            .eq("draft_id", cfg.cur_draft()?),
    )
    .await?;

    let mut table = crate::new_table(vec![
        "Name",
        "Type",
        "Updated",
        "Expected Publish ID",
        "Details",
    ]);
    for row in rows {
        table.add_row(vec![
            row.catalog_name,
            row.spec_type.unwrap_or_default(),
            row.updated_at.to_string(),
            match (row.expect_pub_id, row.last_pub_id) {
                (None, _) => "(any)".to_string(),
                (Some(expect), Some(last)) if expect == last => expect,
                (Some(expect), Some(last)) => format!("{expect}\n(stale; current is {last})"),
                (Some(expect), None) => format!("{expect}\n(does not exist)"),
            },
            row.detail.unwrap_or_default(),
        ]);
    }
    println!("{table}");

    Ok(())
}

async fn do_list(cfg: &config::Config) -> anyhow::Result<()> {
    #[derive(Deserialize)]
    struct Row {
        created_at: crate::Timestamp,
        detail: String,
        id: String,
        num_specs: u32,
        updated_at: crate::Timestamp,
    }
    let rows: Vec<Row> = api_exec(
        cfg.client()?
            .from("drafts_ext")
            .select("created_at,detail,id,num_specs,updated_at"),
    )
    .await?;

    let cur_draft = cfg.draft.clone().unwrap_or_default();

    let mut table = crate::new_table(vec!["Id", "# of Specs", "Created", "Updated", "Details"]);
    for row in rows {
        table.add_row(vec![
            if row.id == cur_draft {
                format!("{} (selected)", row.id)
            } else {
                row.id
            },
            format!("{}", row.num_specs),
            row.created_at.to_string(),
            row.updated_at.to_string(),
            row.detail,
        ]);
    }
    println!("{table}");

    Ok(())
}

async fn do_select(
    cfg: &mut config::Config,
    Select { id: select_id }: &Select,
) -> anyhow::Result<()> {
    let matched: Vec<serde_json::Value> = api_exec(
        cfg.client()?
            .from("drafts")
            .eq("id", select_id)
            .select("id"),
    )
    .await?;

    if matched.is_empty() {
        anyhow::bail!("draft {select_id} does not exist");
    }

    cfg.draft = Some(select_id.clone());
    do_list(cfg).await
}

async fn do_publish(cfg: &mut config::Config, dry_run: bool) -> anyhow::Result<()> {
    let cur_draft = cfg.cur_draft()?;
    let client = cfg.client()?;

    #[derive(Deserialize)]
    struct Row {
        id: String,
        logs_token: String,
    }
    let Row { id, logs_token } = api_exec(
        client
            .from("publications")
            .select("id,logs_token")
            .insert(
                serde_json::json!({
                    "detail": &format!("Published via flowctl"),
                    "draft_id": cur_draft,
                    "dry_run": dry_run,
                })
                .to_string(),
            )
            .single(),
    )
    .await?;

    tracing::info!(%id, %logs_token, %dry_run, "created publication");

    let outcome = crate::poll_while_queued(&client, "publications", &id, &logs_token).await?;

    #[derive(Deserialize, Debug)]
    struct DraftError {
        scope: String,
        detail: String,
    }
    let errors: Vec<DraftError> = api_exec(
        client
            .from("draft_errors")
            .select("scope,detail")
            .eq("draft_id", cur_draft),
    )
    .await?;

    for DraftError { scope, detail } in errors {
        tracing::error!(%scope, %detail);
    }

    if outcome != "success" {
        anyhow::bail!("failed with status: {outcome}");
    }
    tracing::info!(%id, %dry_run, "publication successful");

    if !dry_run {
        cfg.draft = None;
    }
    Ok(())
}
