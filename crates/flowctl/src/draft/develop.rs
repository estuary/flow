use crate::{api_exec, catalog, source, typescript};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Develop {
    #[clap(flatten)]
    local_specs: source::LocalSpecsArgs,
}

pub async fn do_develop(
    ctx: &mut crate::CliContext,
    Develop { local_specs }: &Develop,
) -> anyhow::Result<()> {
    let draft_id = ctx.config().cur_draft()?;
    let rows: Vec<DraftSpecRow> = api_exec(
        ctx.controlplane_client()?
            .from("draft_specs")
            .select("catalog_name,spec,spec_type")
            .not("is", "spec_type", "null")
            .eq("draft_id", draft_id),
    )
    .await?;
    let rows_len = rows.len();

    let bundled_catalog = catalog::collect_specs(rows)?;
    source::write_local_specs(bundled_catalog, local_specs).await?;

    tracing::info!(dir = %local_specs.output_dir.display(), "wrote root catalog");

    let source_args = crate::source::SourceArgs {
        source_dir: vec![local_specs.output_dir.display().to_string()],
        ..Default::default()
    };
    typescript::do_generate(
        ctx,
        &typescript::Generate {
            root_dir: local_specs.output_dir.clone(),
            source: source_args,
        },
    )
    .await
    .context("generating TypeScript project")?;

    println!(
        "Wrote {rows_len} specifications under {}.",
        local_specs.output_dir.display()
    );
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
