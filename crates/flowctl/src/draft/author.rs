use crate::{api_exec, catalog::SpecSummaryItem, controlplane, local_specs};
use anyhow::Context;
use serde::Serialize;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Author {
    /// Path or URL to a Flow specification file to author.
    #[clap(long)]
    source: String,
}

pub async fn clear_draft(client: controlplane::Client, draft_id: &str) -> anyhow::Result<()> {
    tracing::info!(%draft_id, "clearing existing specs from draft");
    api_exec::<Vec<serde_json::Value>>(
        client.from("draft_specs").eq("draft_id", draft_id).delete(),
    )
    .await
    .context("failed to clear existing draft specs")?;
    Ok(())
}

pub async fn upsert_draft_specs(
    client: controlplane::Client,
    draft_id: &str,
    bundled_catalog: &models::Catalog,
) -> anyhow::Result<Vec<SpecSummaryItem>> {
    let models::Catalog {
        collections,
        captures,
        materializations,
        tests,
        ..
    } = bundled_catalog;
    // Build up the array of `draft_specs` to upsert.
    #[derive(Serialize, Debug)]
    struct DraftSpec<'a, P: serde::Serialize> {
        draft_id: &'a str,
        catalog_name: String,
        spec_type: &'static str,
        spec: &'a P,
    }

    let mut body: Vec<u8> = Vec::new();
    body.push('[' as u8);

    for (name, spec) in collections.iter() {
        if body.len() != 1 {
            body.push(',' as u8);
        }
        serde_json::to_writer(
            &mut body,
            &DraftSpec {
                draft_id,
                catalog_name: name.to_string(),
                spec_type: "collection",
                spec,
            },
        )
        .unwrap();
    }
    for (name, spec) in captures.iter() {
        if body.len() != 1 {
            body.push(',' as u8);
        }
        serde_json::to_writer(
            &mut body,
            &DraftSpec {
                draft_id,
                catalog_name: name.to_string(),
                spec_type: "capture",
                spec,
            },
        )
        .unwrap();
    }
    for (name, spec) in materializations.iter() {
        if body.len() != 1 {
            body.push(',' as u8);
        }
        serde_json::to_writer(
            &mut body,
            &DraftSpec {
                draft_id,
                catalog_name: name.to_string(),
                spec_type: "materialization",
                spec,
            },
        )
        .unwrap();
    }
    for (name, steps) in tests.iter() {
        if body.len() != 1 {
            body.push(',' as u8);
        }
        serde_json::to_writer(
            &mut body,
            &DraftSpec {
                draft_id,
                catalog_name: name.to_string(),
                spec_type: "test",
                spec: steps,
            },
        )
        .unwrap();
    }
    body.push(']' as u8);

    let rows: Vec<SpecSummaryItem> = api_exec(
        client
            .from("draft_specs")
            .select("catalog_name,spec_type")
            .upsert(String::from_utf8(body).expect("serialized JSON is always UTF-8"))
            .on_conflict("draft_id,catalog_name"),
    )
    .await?;
    Ok(rows)
}

pub async fn do_author(
    ctx: &mut crate::CliContext,
    Author { source }: &Author,
) -> anyhow::Result<()> {
    let cur_draft = ctx.config().cur_draft()?;
    let (sources, _) =
        local_specs::load_and_validate(ctx.controlplane_client().await?, &source).await?;
    let catalog = local_specs::into_catalog(sources);
    let client = ctx.controlplane_client().await?;
    clear_draft(client.clone(), &cur_draft).await?;
    let rows = upsert_draft_specs(ctx.controlplane_client().await?, &cur_draft, &catalog).await?;

    ctx.write_all(rows, ())
}
