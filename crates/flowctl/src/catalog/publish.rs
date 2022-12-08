use crate::{controlplane, draft, source, CliContext};
use anyhow::Context;

#[derive(Debug, clap::Args)]
pub struct Publish {
    #[clap(flatten)]
    pub source_args: source::SourceArgs,
    /// Proceed with the publication without prompting for confirmation.
    ///
    /// Normally, publish will stop and ask for confirmation before it proceeds. This disables that confirmation.
    /// This flag is required if running flowctl non-interactively, such as in a shell script.
    #[clap(long)]
    pub auto_approve: bool,
}

pub async fn do_publish(ctx: &mut CliContext, args: &Publish) -> anyhow::Result<()> {
    use crossterm::tty::IsTty;

    // The order here is intentional, to minimize the number of things that might be left dangling
    // in common error scenarios. For example, we don't create the draft until after bundling, because
    // then we'd have to clean up the empty draft if the bundling fails. The very first thing is to create the client,
    // since that can fail due to missing/expired credentials.
    let client = ctx.controlplane_client()?;

    anyhow::ensure!(args.auto_approve || std::io::stdin().is_tty(), "The publish command must be run interactively unless the `--auto-approve` flag is provided");

    let sources = args.source_args.resolve_sources().await?;
    let catalog = crate::source::bundle(sources).await?;

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

    print!("\nEnter Y to publish these specs, or anything else to abort: ");
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
