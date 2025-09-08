use crate::{api_exec, api_exec_paginated, draft, local_specs, Client};
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

    draft::encrypt_endpoint_configs(&mut draft_catalog, &ctx.client)
        .await
        .context("encrypting endpoint configurations")?;

    // Extract discovery metadata.
    let draft_capture = &draft_catalog.captures[capture_index];
    let draft_model = draft_capture
        .model
        .as_ref()
        .context("the specification to be discovered is marked for deletion")?;

    let endpoint_config = match &draft_model.endpoint {
        models::CaptureEndpoint::Connector(config) => {
            serde_json::to_value(&config.config).context("serializing endpoint config")?
        }
        models::CaptureEndpoint::Local(_) => unreachable!(), // Already checked above
    };
    let connector_tag_id = match &draft_model.endpoint {
        models::CaptureEndpoint::Connector(config) => {
            extract_connector_tag_id(&ctx.client, &config.image)
                .await
                .context("extracting connector tag ID from capture endpoint")?
        }
        models::CaptureEndpoint::Local(_) => {
            anyhow::bail!("You must use `raw discover` for local connectors");
        }
    };
    let update_only = draft_model
        .auto_discover
        .as_ref()
        .map(|ad| !ad.add_new_bindings)
        .unwrap_or_default();

    // Upsert the draft into which discovery will be merged by the control plane.
    let draft = draft::create_draft(&ctx.client).await?;
    draft::upsert_draft_specs(&ctx.client, draft.id, &draft_catalog)
        .await
        .context("upserting draft specifications")?;
    tracing::info!(draft_id = %draft.id, "created draft for discovery");

    // Submit the discovery job and poll until completion.
    #[derive(Deserialize)]
    struct DiscoverResponse {
        id: models::Id,
        logs_token: String,
    }

    let DiscoverResponse {
        id: discover_id,
        logs_token,
    }: DiscoverResponse = api_exec(
        ctx.client
            .from("discovers")
            .select("id,logs_token")
            .insert(
                serde_json::json!({
                    "capture_name": &draft_capture.capture,
                    "connector_tag_id": connector_tag_id,
                    "data_plane_name": data_plane_name,
                    "draft_id": draft.id,
                    "endpoint_config": endpoint_config,
                    "update_only": update_only,
                })
                .to_string(),
            )
            .single(),
    )
    .await
    .context("failed to submit discovery job")?;
    tracing::info!(%discover_id, %logs_token, "submitted discovery job");

    let outcome =
        crate::poll_while_queued(&ctx.client, "discovers", discover_id, &logs_token).await?;

    if outcome != "success" {
        _ = draft::delete_draft(&ctx.client, draft.id).await; // Best effort.
        anyhow::bail!("discovery failed with status: {outcome}");
    }

    draft::develop(
        ctx,
        draft.id,
        &args.source,
        true, // Overwrite is implied for discovery.
        args.flat,
    )
    .await
    .context("failed to pull down draft for local development")?;

    tracing::info!(%discover_id, "discovery completed successfully");
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
