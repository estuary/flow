use crate::graphql::*;
use crate::poll;
use anyhow::Context;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/draft/publication-create-mutation.graphql"
)]
struct CreatePublicationMutation;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/draft/publication-poll-query.graphql",
    response_derives = "Debug,PartialEq"
)]
struct PollPublicationQuery;

// Keep the log target, which prefixes log lines, the same as it was before
// the publish function was moved out of mod.rs. Otherwise the log target
// would be "flowctl::draft::publication", causing the log lines to change.
const LOG_TARGET: &str = "flowctl::draft";

pub async fn publish(
    ctx: &mut crate::CliContext,
    init_data_plane: Option<&str>,
    draft_id: models::Id,
    dry_run: bool,
) -> Result<(), anyhow::Error> {
    // Add the flowctl version to the detail, so we can tell when a publication
    // was created by an old version.
    let detail = format!("Published via flowctl ({})", env!("CARGO_PKG_VERSION"));
    let vars = create_publication_mutation::Variables {
        draft_id,
        dry_run,
        default_data_plane: init_data_plane.map(str::to_string),
        detail: Some(detail),
    };

    let id =
        post_graphql::<CreatePublicationMutation>(&ctx.rest, ctx.access_token().as_deref(), vars)
            .await
            .context("failed to submit publication")?
            .create_publication
            .id;

    tracing::info!(target: LOG_TARGET, %id, %dry_run, "created publication");

    let (status, errors) = poll::poll_job(
        |after| poll_publication(ctx, id, after),
        std::time::Duration::from_secs(1),
        |line| println!("{line}"),
    )
    .await?;

    // TODO: respect the output format when printing errors
    for error in errors {
        let scope = error.scope.unwrap_or_default();
        tracing::error!(target: LOG_TARGET, %scope, detail = %error.detail);
    }

    if status != poll_publication_query::StatusType::success {
        anyhow::bail!("failed with status: {status:?}");
    }

    tracing::info!(target: LOG_TARGET, %id, %dry_run, "publication successful");
    Ok(())
}

/// Polls publication `id` for a page of logs following `after`, and,
/// once it's no longer queued, its status and the errors that failed it.
async fn poll_publication(
    ctx: &crate::CliContext,
    id: models::Id,
    after: Option<String>,
) -> anyhow::Result<
    poll::JobPoll<(
        poll_publication_query::StatusType,
        Vec<poll_publication_query::PollPublicationQueryPublicationErrors>,
    )>,
> {
    use poll_publication_query::StatusType;

    let vars = poll_publication_query::Variables { id, after };
    let publication =
        post_graphql::<PollPublicationQuery>(&ctx.rest, ctx.access_token().as_deref(), vars)
            .await?
            .publication
            .context("publication not found")?;

    let logs = publication.logs;
    Ok(poll::JobPoll {
        outcome: match publication.status.type_ {
            StatusType::queued => None,
            status => Some((status, publication.errors)),
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
