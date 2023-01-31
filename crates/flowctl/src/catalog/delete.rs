use crate::{api_exec, catalog, draft, CliContext};
use anyhow::Context;
use serde::Serialize;

#[derive(Debug, clap::Args)]
pub struct Delete {
    #[clap(flatten)]
    pub name_selector: catalog::NameSelector,
    #[clap(flatten)]
    pub type_selector: catalog::SpecTypeSelector,
    /// Proceed with deletion without prompting for confirmation.
    ///
    /// Normally, delete will stop and ask for confirmation before it proceeds. This flag disables
    /// that confirmation. This is sometimes required in order to run flowctl non-interactively,
    /// such as in a shell script.
    pub dangerous_auto_approve: bool,
}

#[derive(Serialize, Debug)]
struct DraftSpec {
    draft_id: String,
    catalog_name: String,
    expect_pub_id: String,
    spec_type: serde_json::Value, // always null, since we're deleting
    spec: serde_json::Value,      // always null, since we're deleting
}

pub async fn do_delete(
    ctx: &mut CliContext,
    Delete {
        name_selector,
        type_selector,
        dangerous_auto_approve,
    }: &Delete,
) -> anyhow::Result<()> {
    let list_args = catalog::List {
        flows: false,
        name_selector: name_selector.clone(),
        type_selector: type_selector.clone(),
        deleted: false,
    };

    let client = ctx.controlplane_client()?;
    let specs = catalog::fetch_live_specs(
        client.clone(),
        &list_args,
        vec![
            "id",
            "catalog_name",
            "spec_type",
            "updated_at",
            "last_pub_id",
            "last_pub_user_email",
            "last_pub_user_id",
            "last_pub_user_full_name",
        ],
    )
    .await
    .context("fetching live specs")?;

    if specs.is_empty() {
        anyhow::bail!("no specs found matching given selector");
    }

    // show the user the specs before we ask for confirmation
    ctx.write_all(specs.clone(), false)?;

    if !(*dangerous_auto_approve || prompt_to_continue().await) {
        anyhow::bail!("delete operation cancelled");
    }

    let draft = draft::create_draft(client.clone())
        .await
        .context("failed to create draft")?;
    println!(
        "Deleting {} item(s) using draft: {}",
        specs.len(),
        &draft.id
    );
    tracing::info!(draft_id = %draft.id, "created draft");

    // create the draft specs now, so we can pass owned `specs` to `write_all`
    let draft_specs = specs
        .into_iter()
        .map(|spec| DraftSpec {
            draft_id: draft.id.clone(),
            catalog_name: spec.catalog_name.clone(),
            spec_type: serde_json::Value::Null,
            spec: serde_json::Value::Null,
            expect_pub_id: spec
                .last_pub_id
                .clone()
                .expect("spec is missing last_pub_id"),
        })
        .collect::<Vec<DraftSpec>>();

    api_exec::<Vec<serde_json::Value>>(
        ctx.controlplane_client()?
            .from("draft_specs")
            //.select("catalog_name,spec_type")
            .upsert(serde_json::to_string(&draft_specs).unwrap())
            .on_conflict("draft_id,catalog_name"),
    )
    .await?;
    tracing::debug!("added deletions to draft");

    draft::publish(client.clone(), false, &draft.id).await?;

    // extra newline before, since `publish` will output a bunch of logs
    println!("\nsuccessfully deleted {} spec(s)", draft_specs.len());
    Ok(())
}

async fn prompt_to_continue() -> bool {
    tokio::task::spawn_blocking(|| {
        println!(
            "\nIf you continue, the listed specs will all be deleted. This cannot be undone.\n\
            Enter the word 'delete' to continue, or anything else to abort:\n"
        );
        let mut buf = String::with_capacity(8);

        match std::io::stdin().read_line(&mut buf) {
            Ok(_) => buf.trim() == "delete",
            Err(err) => {
                tracing::error!(error = %err, "failed to read from stdin");
                false
            }
        }
    })
    .await
    .expect("failed to join spawned task")
}
