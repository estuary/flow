use std::collections::BTreeSet;

use crate::{
    api_exec, api_exec_paginated,
    controlplane::Client,
    output::{to_table_row, CliOutput, JsonCell},
};
use anyhow::Context;
use serde::{Deserialize, Serialize};

mod author;
use author::do_author;

mod develop;
use develop::do_develop;

pub use author::upsert_draft_specs;

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
    pub async fn run(&self, ctx: &mut crate::CliContext) -> Result<(), anyhow::Error> {
        match &self.cmd {
            Command::Author(author) => do_author(ctx, author).await,
            Command::Create => do_create(ctx).await,
            Command::Delete => do_delete(ctx).await,
            Command::Describe => do_describe(ctx).await,
            Command::Develop(develop) => do_develop(ctx, develop).await,
            Command::List => do_list(ctx).await,
            Command::Publish => do_publish(ctx, false).await,
            Command::Select(select) => do_select(ctx, select).await,
            Command::Test => do_publish(ctx, true).await,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct DraftRow {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<crate::Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<crate::Timestamp>,
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

pub async fn create_draft(client: Client) -> Result<DraftRow, anyhow::Error> {
    let row: DraftRow = api_exec(
        client
            .from("drafts")
            .select("id, created_at")
            .insert(serde_json::json!({"detail": "Created by flowctl"}).to_string())
            .single(),
    )
    .await?;
    tracing::info!(draft_id = %row.id, "created draft");
    Ok(row)
}

pub async fn delete_draft(client: Client, draft_id: &str) -> Result<DraftRow, anyhow::Error> {
    let row: DraftRow = api_exec(
        client
            .from("drafts")
            .select("id,created_at")
            .delete()
            .eq("id", draft_id)
            .single(),
    )
    .await?;
    tracing::info!(draft_id = %row.id, "deleted draft");
    Ok(row)
}

async fn do_create(ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    let client = ctx.controlplane_client().await?;
    let row = create_draft(client).await?;

    ctx.config_mut().draft = Some(row.id.clone());
    ctx.write_all(Some(row), ())
}

async fn do_delete(ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    #[derive(Deserialize, Serialize)]
    struct Row {
        id: String,
        updated_at: crate::Timestamp,
    }
    impl CliOutput for Row {
        type TableAlt = ();
        type CellValue = JsonCell;

        fn table_headers(_alt: Self::TableAlt) -> Vec<&'static str> {
            vec!["Deleted Draft ID", "Last Updated"]
        }

        fn into_table_row(self, _alt: Self::TableAlt) -> Vec<Self::CellValue> {
            to_table_row(self, &["/id", "/updated_at"])
        }
    }
    let client = ctx.controlplane_client().await?;
    let draft_id = ctx.config().cur_draft()?;
    let row = delete_draft(client, &draft_id).await?;

    ctx.config_mut().draft.take();
    ctx.write_all(Some(row), ())
}

async fn do_describe(ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    #[derive(Deserialize, Serialize)]
    struct Row {
        catalog_name: String,
        detail: Option<String>,
        expect_pub_id: Option<String>,
        last_pub_id: Option<String>,
        spec_type: Option<String>,
        updated_at: crate::Timestamp,
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
                self.spec_type.unwrap_or_default(),
                self.updated_at.to_string(),
                match (self.expect_pub_id, self.last_pub_id) {
                    (None, _) => "(any)".to_string(),
                    (Some(expect), Some(last)) if expect == last => expect,
                    (Some(expect), Some(last)) => format!("{expect}\n(stale; current is {last})"),
                    (Some(expect), None) => format!("{expect}\n(does not exist)"),
                },
                self.detail.unwrap_or_default(),
            ]
        }
    }
    let rows: Vec<Row> = api_exec_paginated(
        ctx.controlplane_client()
            .await?
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
            .eq("draft_id", ctx.config().cur_draft()?),
    )
    .await?;

    ctx.write_all(rows, ())
}

async fn do_list(ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    #[derive(Deserialize, Serialize)]
    struct Row {
        created_at: crate::Timestamp,
        detail: Option<String>,
        id: String,
        num_specs: u32,
        updated_at: crate::Timestamp,
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
    let rows: Vec<Row> = api_exec_paginated(
        ctx.controlplane_client()
            .await?
            .from("drafts_ext")
            .select("created_at,detail,id,num_specs,updated_at"),
    )
    .await?;

    // Decorate the id to mark the selected draft, but only if we're outputting a table
    let cur_draft = ctx.config().draft.clone().unwrap_or_default();
    let output_type = ctx.get_output_type();
    let rows = rows.into_iter().map(move |mut row| {
        if output_type == crate::output::OutputType::Table && row.id == cur_draft {
            row.id = format!("{} (selected)", row.id);
        }
        row
    });

    ctx.write_all(rows, ())
}

/// Invokes the `prune_unchanged_draft_specs` RPC (SQL function), which removes any draft specs
/// that are identical to their live specs, accounting for changes to inferred schemas.
/// Returns the set of specs that were removed from the draft (as a `BTreeSet` so they're ordered).
pub async fn remove_unchanged(client: &Client, draft_id: &str) -> anyhow::Result<BTreeSet<String>> {
    #[derive(Deserialize)]
    struct PrunedDraftSpec {
        catalog_name: String,
    }

    let params = serde_json::to_string(&serde_json::json!({ "draft_id": draft_id })).unwrap();
    // We don't use an explicit select of `catalog_name` because we want the other fields to appear
    // in the response when trace logging is enabled. This may be something we wish to change once
    // we gain more confidence in the spec pruning feature.
    let pruned: Vec<PrunedDraftSpec> = api_exec(client.rpc("prune_unchanged_draft_specs", params))
        .await
        .context("pruning unchanged specs")?;
    Ok(pruned.into_iter().map(|r| r.catalog_name).collect())
}

async fn do_select(
    ctx: &mut crate::CliContext,
    Select { id: select_id }: &Select,
) -> anyhow::Result<()> {
    let matched: Vec<serde_json::Value> = api_exec_paginated(
        ctx.controlplane_client()
            .await?
            .from("drafts")
            .eq("id", select_id)
            .select("id"),
    )
    .await?;

    if matched.is_empty() {
        anyhow::bail!("draft {select_id} does not exist");
    }

    ctx.config_mut().draft = Some(select_id.clone());
    do_list(ctx).await
}

async fn do_publish(ctx: &mut crate::CliContext, dry_run: bool) -> anyhow::Result<()> {
    let draft_id = ctx.config().cur_draft()?;
    let client = ctx.controlplane_client().await?;

    publish(client, dry_run, &draft_id).await?;

    if !dry_run {
        ctx.config_mut().draft.take();
    }
    Ok(())
}

pub async fn publish(client: Client, dry_run: bool, draft_id: &str) -> Result<(), anyhow::Error> {
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
                    "draft_id": draft_id,
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
    let errors: Vec<DraftError> = api_exec_paginated(
        client
            .from("draft_errors")
            .select("scope,detail")
            .eq("draft_id", draft_id),
    )
    .await?;
    for DraftError { scope, detail } in errors {
        tracing::error!(%scope, %detail);
    }
    if outcome != "success" {
        anyhow::bail!("failed with status: {outcome}");
    }
    tracing::info!(%id, %dry_run, "publication successful");
    Ok(())
}
