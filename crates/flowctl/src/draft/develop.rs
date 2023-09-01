use crate::{api_exec_paginated, catalog, local_specs};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Develop {
    /// Root flow specification to create or update.
    #[clap(long, default_value = "flow.yaml")]
    target: String,
    /// Should existing specs be over-written by specs from the Flow control plane?
    #[clap(long)]
    overwrite: bool,
    /// Should specs be written to the single specification file, or written in the canonical layout?
    #[clap(long)]
    flat: bool,
}

pub async fn do_develop(
    ctx: &mut crate::CliContext,
    Develop {
        target,
        overwrite,
        flat,
    }: &Develop,
) -> anyhow::Result<()> {
    let draft_id = ctx.config().cur_draft()?;
    let client = ctx.controlplane_client().await?;
    let rows: Vec<DraftSpecRow> = api_exec_paginated(
        client
            .from("draft_specs")
            .select("catalog_name,spec,spec_type")
            .not("is", "spec_type", "null")
            .eq("draft_id", draft_id),
    )
    .await?;

    let target = build::arg_source_to_url(&target, true)?;
    let mut sources = local_specs::surface_errors(local_specs::load(&target).await.into_result())?;

    let count = local_specs::extend_from_catalog(
        &mut sources,
        catalog::collect_specs(rows)?,
        local_specs::pick_policy(*overwrite, *flat),
    );
    let sources = local_specs::indirect_and_write_resources(sources)?;

    println!("Wrote {count} specifications under {target}.");
    let () = local_specs::generate_files(client, sources).await?;

    Ok(())
}

#[derive(Deserialize, Serialize)]
pub struct DraftSpecRow {
    pub catalog_name: String,
    pub spec: Box<RawValue>,
    pub spec_type: Option<catalog::CatalogSpecType>,
}

impl catalog::SpecRow for DraftSpecRow {
    fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    fn spec_type(&self) -> Option<catalog::CatalogSpecType> {
        self.spec_type
    }

    fn spec(&self) -> Option<&RawValue> {
        Some(self.spec.as_ref())
    }
}
