use crate::local_specs;
use anyhow::Context;
use futures::{SinkExt, StreamExt};
use proto_flow::{capture, flow};
use std::io::Write;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Capture {
    /// Path or URL to a Flow specification file.
    #[clap(long)]
    source: String,
    /// Name of the capture to preview within the Flow specification file.
    /// Capture is required if there are multiple captures in --source specifications.
    #[clap(long)]
    capture: Option<String>,
    /// How frequently should we emit combined documents?
    /// If not specified, the default is one second.
    #[clap(long)]
    interval: Option<humantime::Duration>,
    /// Docker network to run the connector
    #[clap(long, default_value = "bridge")]
    network: String,
}

pub async fn do_capture(
    ctx: &mut crate::CliContext,
    Capture {
        source,
        capture,
        interval,
        network,
    }: &Capture,
) -> anyhow::Result<()> {
    let client = ctx.controlplane_client().await?;
    let (sources, validations) = local_specs::load_and_validate_full(client, &source, &network).await?;

    // Identify the capture to discover.
    let needle = if let Some(needle) = capture {
        needle.as_str()
    } else if sources.captures.len() == 1 {
        sources.captures.first().unwrap().capture.as_str()
    } else if sources.captures.is_empty() {
        anyhow::bail!("sourced specification files do not contain any captures");
    } else {
        anyhow::bail!("sourced specification files contain multiple captures. Use --capture to identify a specific one");
    };

    let (capture, built_capture) = match sources
        .captures
        .binary_search_by_key(&needle, |c| c.capture.as_str())
    {
        Ok(index) => (&sources.captures[index], &validations.built_captures[index]),
        Err(_) => anyhow::bail!("could not find the capture {needle}"),
    };

    let runtime = runtime::Runtime::new(
        true, // All local.
        network.clone(),
        ops::tracing_log_handler,
        None,
        format!("preview/{}", capture.capture),
    );

    let mut apply = capture::Request {
        apply: Some(capture::request::Apply {
            capture: Some(built_capture.spec.clone()),
            dry_run: false,
            version: "preview".to_string(),
        }),
        ..Default::default()
    };
    let mut open = capture::Request {
        open: Some(capture::request::Open {
            capture: Some(built_capture.spec.clone()),
            version: "preview".to_string(),
            range: Some(flow::RangeSpec {
                key_begin: 0,
                key_end: u32::MAX,
                r_clock_begin: 0,
                r_clock_end: u32::MAX,
            }),
            state_json: "{}".to_string(),
        }),
        ..Default::default()
    };

    if let Some(log_level) = capture
        .spec
        .shards
        .log_level
        .as_ref()
        .and_then(|s| ops::LogLevel::from_str_name(s))
    {
        apply.set_internal_log_level(log_level);
        open.set_internal_log_level(log_level);
    }

    let capture::response::Applied { action_description } = runtime
        .clone()
        .unary_capture(apply, build::CONNECTOR_TIMEOUT)
        .await
        .map_err(crate::status_to_anyhow)?
        .applied
        .context("connector didn't send expected Applied response")?;

    tracing::info!(%action_description, "capture was applied");

    let (mut request_tx, request_rx) = futures::channel::mpsc::channel(runtime::CHANNEL_BUFFER);
    request_tx.send(Ok(open)).await.unwrap();

    let mut response_rx = runtime
        .serve_capture(request_rx)
        .await
        .map_err(crate::status_to_anyhow)?;

    let opened = response_rx
        .next()
        .await
        .context("expected Opened, not EOF")?
        .map_err(crate::status_to_anyhow)?
        .opened
        .context("expected Opened")?;

    tracing::info!(opened=?::ops::DebugJson(opened), "received connector Opened");

    // Read documents from response_rx
    // On checkpoint, send ACK into request_rx.

    let interval = interval
        .map(|i| i.clone().into())
        .unwrap_or(std::time::Duration::from_secs(1));

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    _ = ticker.tick().await; // First tick is immediate.

    let mut output = std::io::stdout();

    // TODO(johnny): This is currently only partly implemented, but is awaiting
    // accompanying changes to the `runtime` crate.
    while let Some(response) = response_rx.next().await {
        let response = response.map_err(crate::status_to_anyhow)?;

        let _internal = response
            .get_internal()
            .context("failed to decode internal runtime.CaptureResponseExt")?;

        serde_json::to_writer(&mut output, &response)?;
        write!(&mut output, "\n")?;

        // Upon a checkpoint, wait until the next tick interval has elapsed before acknowledging.
        if let Some(_checkpoint) = response.checkpoint {
            _ = ticker.tick().await;

            request_tx
                .send(Ok(capture::Request {
                    acknowledge: Some(capture::request::Acknowledge { checkpoints: 1 }),
                    ..Default::default()
                }))
                .await
                .unwrap();
        }
    }

    Ok(())
}
