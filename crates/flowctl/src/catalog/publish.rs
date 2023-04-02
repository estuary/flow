use crate::{api_exec, controlplane, draft, local_specs, CliContext};
use anyhow::Context;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, clap::Args)]
pub struct Publish {
    /// Path or URL to a Flow specification file to author.
    #[clap(long)]
    source: String,
    /// Proceed with the publication without prompting for confirmation.
    ///
    /// Normally, publish will stop and ask for confirmation before it proceeds. This disables that confirmation.
    /// This flag is required if running flowctl non-interactively, such as in a shell script.
    #[clap(long)]
    auto_approve: bool,
}

// TODO(whb): This page size is pretty arbitrary, but seemed to work fine during my tests. It needs
// to be large enough to be reasonably efficient for large numbers of specs, but small enough to not
// exceed query length limitations.
const MD5_PAGE_SIZE: usize = 100;

pub async fn remove_unchanged(
    client: &controlplane::Client,
    input_catalog: models::Catalog,
) -> anyhow::Result<models::Catalog> {
    let mut spec_checksums: HashMap<String, String> = HashMap::new();

    #[derive(Deserialize, Debug)]
    struct SpecChecksumRow {
        catalog_name: String,
        md5: Option<String>,
    }

    let spec_names = input_catalog.all_spec_names();
    for names in &spec_names.into_iter().chunks(MD5_PAGE_SIZE) {
        let builder = client
            .from("live_specs_ext")
            .select("catalog_name,md5")
            .in_("catalog_name", names);

        let rows: Vec<SpecChecksumRow> = api_exec(builder).await?;
        let chunk_checksums = rows
            .iter()
            .filter_map(|row| {
                if let Some(md5) = row.md5.as_ref() {
                    Some((row.catalog_name.clone(), md5.clone()))
                } else {
                    None
                }
            })
            .collect::<HashMap<String, String>>();

        spec_checksums.extend(chunk_checksums);
    }

    let models::Catalog {
        mut collections,
        mut captures,
        mut materializations,
        mut tests,
        ..
    } = input_catalog;

    collections.retain(|name, spec| filter_unchanged_catalog_items(&spec_checksums, name, spec));
    captures.retain(|name, spec| filter_unchanged_catalog_items(&spec_checksums, name, spec));
    materializations
        .retain(|name, spec| filter_unchanged_catalog_items(&spec_checksums, name, spec));
    tests.retain(|name, spec| filter_unchanged_catalog_items(&spec_checksums, name, spec));

    Ok(models::Catalog {
        collections,
        captures,
        materializations,
        tests,
        ..Default::default()
    })
}

fn filter_unchanged_catalog_items(
    existing_specs: &HashMap<String, String>,
    new_catalog_name: &impl AsRef<str>,
    new_catalog_spec: &impl Serialize,
) -> bool {
    if let Some(existing_spec_md5) = existing_specs.get(&new_catalog_name.as_ref().to_string()) {
        let buf = serde_json::to_vec(new_catalog_spec).expect("new spec must be serializable");

        let new_spec_md5 = format!("{:x}", md5::compute(buf));

        return *existing_spec_md5 != new_spec_md5;
    }

    // Catalog name does not yet exist in live specs.
    true
}

pub async fn do_publish(ctx: &mut CliContext, args: &Publish) -> anyhow::Result<()> {
    use crossterm::tty::IsTty;

    // The order here is intentional, to minimize the number of things that might be left dangling
    // in common error scenarios. For example, we don't create the draft until after bundling, because
    // then we'd have to clean up the empty draft if the bundling fails. The very first thing is to create the client,
    // since that can fail due to missing/expired credentials.
    let client = ctx.controlplane_client().await?;

    anyhow::ensure!(args.auto_approve || std::io::stdin().is_tty(), "The publish command must be run interactively unless the `--auto-approve` flag is provided");

    let (sources, _validations) =
        local_specs::load_and_validate(client.clone(), &args.source).await?;
    let catalog = remove_unchanged(&client, local_specs::into_catalog(sources)).await?;

    if catalog.is_empty() {
        println!("No specs would be changed by this publication, nothing to publish.");
        return Ok(());
    }

    let draft = draft::create_draft(client.clone()).await?;
    println!("Created draft: {}", &draft.id);
    tracing::info!(draft_id = %draft.id, "created draft");
    let spec_rows = draft::upsert_draft_specs(client.clone(), &draft.id, &catalog).await?;
    println!("Will publish the following {} specs", spec_rows.len());
    ctx.write_all(spec_rows, ())?;

    if !(args.auto_approve || prompt_to_continue().await) {
        println!("\nCancelling");
        try_delete_draft(client.clone(), &draft.id).await;
        anyhow::bail!("publish cancelled");
    }
    println!("Proceeding to publish...");

    let publish_result = draft::publish(client.clone(), false, &draft.id).await;
    // The draft will have been deleted automatically if the publish was successful.
    if let Err(err) = publish_result.as_ref() {
        tracing::error!(draft_id = %draft.id, error = %err, "publication error");
        try_delete_draft(client, &draft.id).await;
    }
    publish_result.context("Publish failed")?;
    println!("\nPublish successful");
    Ok(())
}

async fn prompt_to_continue() -> bool {
    use tokio::io::AsyncReadExt;

    println!("\nEnter Y to publish these specs, or anything else to abort: ");
    let mut buf = [0u8];
    match tokio::io::stdin().read_exact(&mut buf[..]).await {
        Ok(_) => &buf == b"y" || &buf == b"Y",
        Err(err) => {
            tracing::error!(error = %err, "error reading from stdin, cancelling publish");
            false
        }
    }
}

async fn try_delete_draft(client: controlplane::Client, draft_id: &str) {
    if let Err(del_err) = draft::delete_draft(client.clone(), &draft_id).await {
        tracing::error!(draft_id = %draft_id, error = %del_err, "failed to delete draft");
    }
}
