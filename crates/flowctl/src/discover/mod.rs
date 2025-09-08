use crate::{api_exec, api_exec_paginated, catalog, draft, local_specs, poll_while_queued, Client};
use anyhow::Context;
use serde::Deserialize;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Discover {
    /// Path or URL to a Flow specification file.
    #[clap(long)]
    source: String,
    /// Name of the capture to discover within the Flow specification file.
    /// Capture is required if there are multiple captures in --source specifications.
    #[clap(long)]
    capture: Option<String>,
    /// Should specs be written to the single specification file, or written in the canonical layout?
    #[clap(long)]
    flat: bool,
    /// Explicit data-plane on which discovery should occur.
    /// If not specified, the data-plane is inferred from storage mappings.
    #[clap(long)]
    data_plane: Option<String>,
}

impl Discover {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> Result<(), anyhow::Error> {
        do_discover(ctx, self).await
    }
}

async fn do_discover(ctx: &mut crate::CliContext, args: &Discover) -> anyhow::Result<()> {
    // Load, inline, and validate the source specifications.
    let (mut draft_catalog, live, _validations) =
        local_specs::load_and_validate(&ctx.client, &args.source).await?;

    // Identify the capture to discover.
    let needle = if let Some(needle) = &args.capture {
        needle.as_str()
    } else if draft_catalog.captures.len() == 1 {
        draft_catalog.captures.first().unwrap().capture.as_str()
    } else if draft_catalog.captures.is_empty() {
        anyhow::bail!("sourced specification files do not contain any captures");
    } else {
        anyhow::bail!("sourced specification files contain multiple captures. Use --capture to identify a specific one");
    };

    let capture_index = match draft_catalog
        .captures
        .binary_search_by_key(&needle, |c| c.capture.as_str())
    {
        Ok(index) => index,
        Err(_) => anyhow::bail!("could not find the capture {needle}"),
    };

    // Data-plane to which the discover will be submitted.
    // Use an explicit plane if provided, otherwise use the plane of the built capture.
    let data_plane_name = if let Some(data_plane) = &args.data_plane {
        data_plane.as_str()
    } else {
        let data_plane_id = _validations
            .built_captures
            .get_by_key(&models::Capture::new(needle))
            .expect("capture validated")
            .data_plane_id;

        live.data_planes
            .get_by_key(&data_plane_id)
            .with_context(|| {
                format!("couldn't resolve data-plane {data_plane_id}; you may not have access")
            })?
            .data_plane_name
            .as_str()
    };
    tracing::info!(%data_plane_name, "using data-plane for discovery");

    let connector_tag_id = match &draft_catalog.captures[capture_index]
        .model
        .as_ref()
        .context("the specification to be discovered is marked for deletion")?
        .endpoint
    {
        models::CaptureEndpoint::Connector(config) => {
            extract_connector_tag_id(&ctx.client, &config.image)
                .await
                .context("extracting connector tag ID from capture endpoint")?
        }
        models::CaptureEndpoint::Local(_) => {
            anyhow::bail!("You must use `raw discover` for local connectors");
        }
    };

    draft::encrypt_endpoint_configs(&mut draft_catalog, &ctx.client)
        .await
        .context("encrypting endpoint configurations")?;

    let endpoint_config = match &draft_catalog.captures[capture_index]
        .model
        .as_ref()
        .unwrap()
        .endpoint
    {
        models::CaptureEndpoint::Connector(config) => {
            serde_json::to_value(&config.config).context("serializing endpoint config")?
        }
        models::CaptureEndpoint::Local(_) => unreachable!(), // Already checked above
    };

    let draft = draft::create_draft(&ctx.client).await?;
    draft::upsert_draft_specs(&ctx.client, draft.id, &draft_catalog)
        .await
        .context("upserting draft specifications")?;
    tracing::info!(draft_id = %draft.id, "created draft for discovery");

    // Submit the discovery job.
    let discover_id = submit_discovery(
        &ctx.client,
        draft.id,
        &draft_catalog.captures[capture_index].capture,
        connector_tag_id,
        endpoint_config,
        &data_plane_name,
    )
    .await?;

    // Poll the discovery job until completion.
    let outcome = poll_while_queued(
        &ctx.client,
        "discovers",
        discover_id.id,
        &discover_id.logs_token,
    )
    .await?;

    if outcome != "success" {
        _ = draft::delete_draft(&ctx.client, draft.id).await; // Best effort.
        anyhow::bail!("discovery failed with status: {outcome}");
    }

    download_draft_specs(ctx, draft.id, &args.source, args.flat).await?;
    tracing::info!(%discover_id.id, "discovery completed successfully");
    _ = draft::delete_draft(&ctx.client, draft.id).await; // Best effort.

    Ok(())
}

async fn extract_connector_tag_id(client: &Client, image: &str) -> anyhow::Result<models::Id> {
    // Parse the image URL to extract the image name and tag.
    // Expected format: "image-name:tag" or "registry/image-name:tag"
    let (image_name, tag) = if let Some((name, tag)) = image.rsplit_once(':') {
        (name, format!(":{}", tag)) // Tags in DB have : prefix
    } else {
        anyhow::bail!("invalid connector image format: {}", image);
    };

    // Query for the connector based on image name.
    #[derive(Deserialize)]
    struct ConnectorRow {
        id: models::Id,
    }

    let connectors: Vec<ConnectorRow> = api_exec_paginated(
        client
            .from("connectors")
            .select("id")
            .eq("image_name", image_name),
    )
    .await?;

    if connectors.is_empty() {
        anyhow::bail!("no connector found for image: {}", image_name);
    }

    let connector_id = &connectors[0].id;

    // Query for the connector tag.
    #[derive(Deserialize)]
    struct ConnectorTagRow {
        id: models::Id,
    }

    let tags: Vec<ConnectorTagRow> = api_exec_paginated(
        client
            .from("connector_tags")
            .select("id")
            .eq("connector_id", connector_id.to_string())
            .eq("image_tag", &tag),
    )
    .await?;

    if tags.is_empty() {
        anyhow::bail!(
            "no connector tag found for image: {} with tag: {}",
            image_name,
            tag
        );
    }

    Ok(tags[0].id.clone())
}

#[derive(Deserialize)]
struct DiscoverResponse {
    id: models::Id,
    logs_token: String,
}

async fn submit_discovery(
    client: &Client,
    draft_id: models::Id,
    capture_name: &str,
    connector_tag_id: models::Id,
    endpoint_config: serde_json::Value,
    data_plane_name: &str,
) -> anyhow::Result<DiscoverResponse> {
    let response: DiscoverResponse = api_exec(
        client
            .from("discovers")
            .select("id,logs_token")
            .insert(
                serde_json::json!({
                    "draft_id": draft_id,
                    "capture_name": capture_name,
                    "connector_tag_id": connector_tag_id,
                    "endpoint_config": endpoint_config,
                    "data_plane_name": data_plane_name,
                    "detail": "Discovery initiated via flowctl",
                })
                .to_string(),
            )
            .single(),
    )
    .await?;

    tracing::info!(discover_id = %response.id, %response.logs_token, "submitted discovery job");
    Ok(response)
}

async fn download_draft_specs(
    ctx: &mut crate::CliContext,
    draft_id: models::Id,
    target: &str,
    flat: bool,
) -> anyhow::Result<()> {
    use crate::draft::DraftSpecRow;

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
        local_specs::pick_policy(
            true, // Always overwrite.
            flat,
        ),
    );
    let sources = local_specs::indirect_and_write_resources(sources)?;

    println!("Wrote {count} specifications under {target}.");
    let () = local_specs::generate_files(&ctx.client, sources).await?;

    Ok(())
}
