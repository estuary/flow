use crate::{catalog::SpecSummaryItem, draft, local_specs, CliContext};
use anyhow::Context;

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
    /// Data-plane into which created specifications will be placed.
    #[clap(long, default_value = "ops/dp/public/gcp-us-central1-c1")]
    default_data_plane: String,
}

pub async fn do_publish(ctx: &mut CliContext, args: &Publish) -> anyhow::Result<()> {
    use crossterm::tty::IsTty;

    // The order here is intentional, to minimize the number of things that might be left dangling
    // in common error scenarios. For example, we don't create the draft until after bundling, because
    // then we'd have to clean up the empty draft if the bundling fails. The very first thing is to create the client,
    // since that can fail due to missing/expired credentials.
    anyhow::ensure!(args.auto_approve || std::io::stdin().is_tty(), "The publish command must be run interactively unless the `--auto-approve` flag is provided");

    let (mut draft_catalog, _validations) =
        local_specs::load_and_validate(&ctx.client, &args.source).await?;

    let draft = draft::create_draft(&ctx.client).await?;
    println!("Created draft: {}", &draft.id);
    tracing::info!(draft_id = %draft.id, "created draft");
    draft::author(&ctx.client, draft.id, &mut draft_catalog).await?;

    let removed = draft::remove_unchanged(&ctx.client, draft.id).await?;
    if !removed.is_empty() {
        println!("The following specs are identical to the currently published specs, and have been pruned from the draft:");
        for name in removed.iter() {
            println!("{name}");
        }
        println!(""); // blank line to give a bit of spacing
    }

    let mut summary = SpecSummaryItem::summarize_catalog(draft_catalog);
    summary.retain(|s| !removed.contains(&s.catalog_name));

    if summary.is_empty() {
        println!("No specs would be changed by this publication, nothing to publish.");
        try_delete_draft(&ctx.client, draft.id).await;
        return Ok(());
    }

    println!("Will publish the following {} specs", summary.len());
    ctx.write_all(summary, ())?;

    if !(args.auto_approve || prompt_to_continue().await) {
        println!("\nCancelling");
        try_delete_draft(&ctx.client, draft.id).await;
        anyhow::bail!("publish cancelled");
    }
    println!("Proceeding to publish...");

    let publish_result =
        draft::publish(&ctx.client, &args.default_data_plane, draft.id, false).await;
    // The draft will have been deleted automatically if the publish was successful.
    if let Err(err) = publish_result.as_ref() {
        tracing::error!(draft_id = %draft.id, error = ?err, "publication error");
        try_delete_draft(&ctx.client, draft.id).await;
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
            tracing::error!(error = ?err, "error reading from stdin, cancelling publish");
            false
        }
    }
}

async fn try_delete_draft(client: &crate::Client, draft_id: models::Id) {
    if let Err(del_err) = draft::delete_draft(client, draft_id).await {
        tracing::error!(draft_id = %draft_id, error = %del_err, "failed to delete draft");
    }
}
