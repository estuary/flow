use crate::local_specs;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Generate {
    /// Path or URL to a Flow specification file to generate development files for.
    #[clap(long)]
    source: String,
}

impl Generate {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        let source = build::arg_source_to_url(&self.source, false)?;
        let sources = local_specs::load(&source).await;
        let client = ctx.controlplane_client().await?;
        let () = local_specs::generate_files(client, sources).await?;
        Ok(())
    }
}
