use std::{io, path};

use crate::{render::Renderer, TenantInfo};

#[derive(Debug, clap::Args)]
pub struct GenerateArgs {
    /// Directory to output rendered templates. Will be created if it does not exist.
    #[clap(long = "output-dir")]
    output_dir: String,
}

impl GenerateArgs {
    pub fn run(&self) -> anyhow::Result<()> {
        let output_dir = path::Path::new(&self.output_dir);

        let mut tenants: Vec<TenantInfo> = Vec::new();
        for line in io::stdin().lines() {
            tenants.push(serde_json::from_str(&line?)?);
        }

        let r = Renderer::new(false, true)?;
        r.render(tenants, output_dir)?;

        Ok(())
    }
}
