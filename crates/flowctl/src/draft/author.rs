use crate::{api_exec, catalog::SpecSummaryItem, controlplane, source};
use serde::Serialize;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Author {
    #[clap(flatten)]
    source_args: source::SourceArgs,
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
    Author { source_args }: &Author,
) -> anyhow::Result<()> {
    let cur_draft = ctx.config().cur_draft()?;
    let specs = source_args.resolve_sources().await?;
    let catalog = crate::source::bundle(specs).await?;
    let rows = upsert_draft_specs(ctx.controlplane_client()?, &cur_draft, &catalog).await?;

    ctx.write_all(rows, ())
}
