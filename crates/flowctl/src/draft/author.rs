use crate::{api_exec, api_exec_paginated, catalog::SpecSummaryItem, controlplane, local_specs};
use anyhow::Context;
use serde::Serialize;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Author {
    /// Path or URL to a Flow specification file to author.
    #[clap(long)]
    source: String,
}

pub async fn clear_draft(client: controlplane::Client, draft_id: models::Id) -> anyhow::Result<()> {
    tracing::info!(%draft_id, "clearing existing specs from draft");
    api_exec::<Vec<serde_json::Value>>(
        client
            .from("draft_specs")
            .eq("draft_id", draft_id.to_string())
            .delete(),
    )
    .await
    .context("failed to clear existing draft specs")?;
    Ok(())
}

pub async fn upsert_draft_specs(
    client: controlplane::Client,
    draft_id: models::Id,
    draft: &tables::DraftCatalog,
) -> anyhow::Result<Vec<SpecSummaryItem>> {
    let tables::DraftCatalog {
        collections,
        captures,
        materializations,
        tests,
        ..
    } = draft;

    // Build up the array of `draft_specs` to upsert.
    #[derive(Serialize, Debug)]
    struct DraftSpec<'a, P: serde::Serialize> {
        draft_id: models::Id,
        catalog_name: String,
        spec_type: &'static str,
        spec: &'a P,
        expect_pub_id: Option<models::Id>,
    }

    let mut body: Vec<u8> = Vec::new();
    body.push('[' as u8);

    for row in collections.iter() {
        if body.len() != 1 {
            body.push(',' as u8);
        }
        serde_json::to_writer(
            &mut body,
            &DraftSpec {
                draft_id,
                catalog_name: row.collection.to_string(),
                spec_type: "collection",
                spec: &row.model,
                expect_pub_id: row.expect_pub_id,
            },
        )
        .unwrap();
    }
    for row in captures.iter() {
        if body.len() != 1 {
            body.push(',' as u8);
        }
        serde_json::to_writer(
            &mut body,
            &DraftSpec {
                draft_id,
                catalog_name: row.capture.to_string(),
                spec_type: "capture",
                spec: &row.model,
                expect_pub_id: row.expect_pub_id,
            },
        )
        .unwrap();
    }
    for row in materializations.iter() {
        if body.len() != 1 {
            body.push(',' as u8);
        }
        serde_json::to_writer(
            &mut body,
            &DraftSpec {
                draft_id,
                catalog_name: row.materialization.to_string(),
                spec_type: "materialization",
                spec: &row.model,
                expect_pub_id: row.expect_pub_id,
            },
        )
        .unwrap();
    }
    for row in tests.iter() {
        if body.len() != 1 {
            body.push(',' as u8);
        }
        serde_json::to_writer(
            &mut body,
            &DraftSpec {
                draft_id,
                catalog_name: row.test.to_string(),
                spec_type: "test",
                spec: &row.model,
                expect_pub_id: row.expect_pub_id,
            },
        )
        .unwrap();
    }
    body.push(']' as u8);

    let rows: Vec<SpecSummaryItem> = api_exec_paginated(
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
    let client = ctx.controlplane_client().await?;
    let draft_id = ctx.config().cur_draft()?;
    let (draft, _) = local_specs::load_and_validate(client.clone(), &source).await?;

    clear_draft(client.clone(), draft_id).await?;
    let rows = upsert_draft_specs(client, draft_id, &draft).await?;

    ctx.write_all(rows, ())
}
