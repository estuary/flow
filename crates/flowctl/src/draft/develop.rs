use crate::{catalog, local_specs};
use flow_client::api_exec_paginated;
use models::{CatalogType, RawValue};
use serde::{Deserialize, Serialize};

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
    let draft_id = ctx.config.selected_draft()?;
    let rows: Vec<DraftSpecRow> = api_exec_paginated(
        ctx.client
            .from("draft_specs")
            .select("catalog_name,spec,spec_type,expect_pub_id")
            .not("is", "spec_type", "null")
            .eq("draft_id", draft_id.to_string()),
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
    let () = local_specs::generate_files(&ctx.client, sources).await?;

    Ok(())
}

#[derive(Deserialize, Serialize)]
pub struct DraftSpecRow {
    pub catalog_name: String,
    pub spec: RawValue,
    pub spec_type: CatalogType,
    pub expect_pub_id: Option<models::Id>,
}

impl catalog::SpecRow for DraftSpecRow {
    fn catalog_name(&self) -> &str {
        &self.catalog_name
    }
    fn spec_type(&self) -> CatalogType {
        self.spec_type
    }
    fn spec(&self) -> Option<&RawValue> {
        Some(&self.spec)
    }
    fn expect_pub_id(&self) -> Option<models::Id> {
        self.expect_pub_id
    }
}
