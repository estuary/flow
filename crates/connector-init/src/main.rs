use anyhow::Context;
use clap::Parser;
use tracing_subscriber::prelude::*;

fn main() -> anyhow::Result<()> {
    let args = connector_init::Args::parse();

    // Map the LOG_LEVEL variable to an equivalent tracing EnvFilter.
    // Restrict logged modules to the current crate, as debug logging
    // for tonic can be quite verbose.
    let log_level =
        std::env::var("LOG_LEVEL").context("missing expected environment variable LOG_LEVEL")?;
    let env_filter = tracing_subscriber::EnvFilter::try_from(format!(
        "flow_connector_init,connector_init={log_level}"
    ))
    .context("parsing LOG_LEVEL environment filter failed")?;

    // Map tracing::info!() and friends to instances of `Log` which are processed
    // by the stderr handler. Note that flow-connector-init *always* logs in the
    // canonical structured ops::Log format.
    tracing_subscriber::registry()
        .with(
            ops::tracing::Layer::new(ops::stderr_log_handler, time::OffsetDateTime::now_utc)
                .with_filter(env_filter),
        )
        .init();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;

    // Run until signaled, then gracefully stop.
    let result = runtime.block_on(connector_init::run(args));

    // Explicitly call Runtime::shutdown_background as an alternative to calling Runtime::Drop.
    // This shuts down the runtime without waiting for blocking background tasks to complete,
    // which is good because they likely never will. Consider a blocking call to read from stdin,
    // where the sender is itself waiting for us to exit or write to our stdout.
    // (Note that tokio::io maps AsyncRead of file descriptors to blocking tasks under the hood).
    runtime.shutdown_background();

    let () = result?;
    tracing::debug!(message = "connector-init exiting");
    Ok(())
}
