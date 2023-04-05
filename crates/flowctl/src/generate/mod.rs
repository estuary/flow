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
        let source = local_specs::arg_source_to_url(&self.source, false)?;

        let (sources, errors) = local_specs::load(&source).await;

        for tables::Error { scope, error } in errors.iter() {
            match error.downcast_ref() {
                Some(sources::LoadError::Fetch { .. }) => {
                    // Omit load errors about missing resources. That's the point!
                }
                _ => tracing::error!(%scope, ?error),
            }
        }

        // Build to generate associated files.
        let client = ctx.controlplane_client().await?;
        let (_, errors) = local_specs::build(client, sources).await;

        for tables::Error { scope, error } in errors.iter() {
            tracing::error!(%scope, ?error);
        }

        Ok(())
    }
}
