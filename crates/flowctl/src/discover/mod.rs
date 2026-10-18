use crate::graphql::*;
use crate::{draft, local_specs, poll};
use anyhow::Context;

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
    /// If not specified, a live capture uses its current data-plane,
    /// and a new capture uses one permitted by its storage mapping.
    #[clap(long)]
    data_plane: Option<String>,
}

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/discover/create-mutation.graphql"
)]
struct CreateDiscoverMutation;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/discover/poll-query.graphql",
    response_derives = "Debug,PartialEq"
)]
struct PollDiscoverQuery;

impl Discover {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> Result<(), anyhow::Error> {
        do_discover(ctx, self).await
    }
}

async fn do_discover(ctx: &mut crate::CliContext, args: &Discover) -> anyhow::Result<()> {
    // Load, inline, and validate the source specifications.
    let build::Output {
        draft: mut draft_catalog,
        ..
    } = local_specs::load_and_validate(ctx, &args.source).await?;

    // Identify the capture to discover.
    let needle = if let Some(needle) = &args.capture {
        needle.as_str()
    } else if draft_catalog.captures.len() == 1 {
        draft_catalog.captures.first().unwrap().capture.as_str()
    } else if draft_catalog.captures.is_empty() {
        anyhow::bail!("sourced specification files do not contain any captures");
    } else {
        anyhow::bail!(
            "sourced specification files contain multiple captures. Use --capture to identify a specific one"
        );
    };

    let capture_index = match draft_catalog
        .captures
        .binary_search_by_key(&needle, |c| c.capture.as_str())
    {
        Ok(index) => index,
        Err(_) => anyhow::bail!("could not find the capture {needle}"),
    };

    draft::encrypt_configs(&mut draft_catalog, ctx)
        .await
        .context("encrypting endpoint configurations")?;

    let draft_capture = &draft_catalog.captures[capture_index];
    let draft_model = draft_capture
        .model
        .as_ref()
        .context("the specification to be discovered is marked for deletion")?;

    if let models::CaptureEndpoint::Local(_) = &draft_model.endpoint {
        anyhow::bail!("You must use `raw discover` for local connectors");
    }

    // Create the draft into which the control plane will merge discovered bindings.
    let draft = draft::create_draft(ctx).await?;
    tracing::info!(draft_id = %draft.id, "created draft for discovery");

    // Discover into the draft, and delete it (best effort) whatever the outcome.
    let result = async {
        draft::upsert_draft_specs(ctx, draft.id, &draft_catalog)
            .await
            .context("upserting draft specifications")?;

        let vars = create_discover_mutation::Variables {
            draft_id: draft.id,
            capture_name: models::Name::new(draft_capture.capture.as_str()),
            data_plane: args.data_plane.clone(),
        };
        let create_discover_mutation::CreateDiscoverMutationCreateDiscover {
            id: discover_id,
            data_plane_name,
        } = post_graphql::<CreateDiscoverMutation>(&ctx.rest, ctx.access_token().as_deref(), vars)
            .await
            .context("failed to submit discovery job")?
            .create_discover;
        tracing::info!(%discover_id, %data_plane_name, "submitted discovery job");

        let status = poll::poll_job(
            |after| poll_discover(ctx, discover_id, after),
            std::time::Duration::from_secs(1),
            |line| println!("{line}"),
        )
        .await?;

        if status != poll_discover_query::DiscoverStatus::SUCCESS {
            draft::print_draft_errors(ctx, draft.id).await?;
            anyhow::bail!("discovery failed with status: {status:?}");
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
        Ok(())
    }
    .await;

    _ = draft::delete_draft(ctx, draft.id).await;
    result
}

async fn poll_discover(
    ctx: &crate::CliContext,
    id: models::Id,
    after: Option<String>,
) -> anyhow::Result<poll::JobPoll<poll_discover_query::DiscoverStatus>> {
    use poll_discover_query::DiscoverStatus;

    let vars = poll_discover_query::Variables { id, after };
    let discover =
        post_graphql::<PollDiscoverQuery>(&ctx.rest, ctx.access_token().as_deref(), vars)
            .await?
            .discover
            .context("discovery job not found")?;

    let logs = discover.logs;
    Ok(poll::JobPoll {
        outcome: match discover.status {
            DiscoverStatus::QUEUED => None,
            status => Some(status),
        },
        logs: logs
            .edges
            .into_iter()
            .map(|edge| poll::LogLine {
                logged_at: edge.node.logged_at,
                stream: edge.node.stream,
                line: edge.node.line,
            })
            .collect(),
        end_cursor: logs.page_info.end_cursor,
        has_next_page: logs.page_info.has_next_page,
    })
}
