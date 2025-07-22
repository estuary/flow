use crate::{api_exec, catalog::SpecSummaryItem, draft::encrypt, local_specs};
use anyhow::Context;
use futures::{stream::FuturesOrdered, StreamExt};
use serde::Serialize;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Author {
    /// Path or URL to a Flow specification file to author.
    #[clap(long)]
    source: String,
}

pub async fn clear_draft(client: &crate::Client, draft_id: models::Id) -> anyhow::Result<()> {
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

/// Encrypts any unencrypted endpoint configurations in the draft catalog,
/// and then upserts the draft specs to the given draft ID.
pub async fn author(
    client: &crate::Client,
    draft_id: models::Id,
    draft: &mut tables::DraftCatalog,
) -> anyhow::Result<Vec<SpecSummaryItem>> {
    encrypt::encrypt_endpoint_configs(draft, client).await?;
    upsert_draft_specs(client, draft_id, &*draft).await
}

async fn upsert_draft_specs(
    client: &crate::Client,
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

    // Serialize DraftSpecs directly to JSON without going through
    // serde_json::Value in order to avoid re-ordering fields which
    // breaks sops hmac hashes.
    let mut draft_specs: Vec<String> = vec![];

    for row in collections.iter() {
        draft_specs.push(
            serde_json::to_string(&DraftSpec {
                draft_id,
                catalog_name: row.collection.to_string(),
                spec_type: "collection",
                spec: &row.model,
                expect_pub_id: row.expect_pub_id,
            })
            .unwrap(),
        );
    }
    for row in captures.iter() {
        draft_specs.push(
            serde_json::to_string(&DraftSpec {
                draft_id,
                catalog_name: row.capture.to_string(),
                spec_type: "capture",
                spec: &row.model,
                expect_pub_id: row.expect_pub_id,
            })
            .unwrap(),
        );
    }
    for row in materializations.iter() {
        draft_specs.push(
            serde_json::to_string(&DraftSpec {
                draft_id,
                catalog_name: row.materialization.to_string(),
                spec_type: "materialization",
                spec: &row.model,
                expect_pub_id: row.expect_pub_id,
            })
            .unwrap(),
        );
    }
    for row in tests.iter() {
        draft_specs.push(
            serde_json::to_string(&DraftSpec {
                draft_id,
                catalog_name: row.test.to_string(),
                spec_type: "test",
                spec: &row.model,
                expect_pub_id: row.expect_pub_id,
            })
            .unwrap(),
        );
    }

    const BATCH_SIZE: usize = 100;

    // Upsert draft specs in batches
    let mut futures = draft_specs
        .chunks(BATCH_SIZE)
        .map(|batch| {
            let builder = client
                .clone()
                .from("draft_specs")
                .select("catalog_name,spec_type")
                .upsert(format!("[{}]", batch.join(",")))
                .on_conflict("draft_id,catalog_name");
            async move { api_exec::<Vec<SpecSummaryItem>>(builder).await }
        })
        .collect::<FuturesOrdered<_>>();

    let mut rows = Vec::new();

    while let Some(result) = futures.next().await {
        rows.extend(result.context("executing live_specs_ext fetch")?);
    }

    Ok(rows)
}

pub async fn do_author(
    ctx: &mut crate::CliContext,
    Author { source }: &Author,
) -> anyhow::Result<()> {
    let draft_id = ctx.config.selected_draft()?;
    let (mut draft, _) = local_specs::load_and_validate(&ctx.client, &source).await?;

    clear_draft(&ctx.client, draft_id).await?;
    let rows = author(&ctx.client, draft_id, &mut draft).await?;

    ctx.write_all(rows, ())
}
