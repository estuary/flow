use clap::Parser;
use std::io::Write;
use tracing_subscriber::prelude::*;

fn main() {
    // Write a byte to stderr to let our container host know that we're alive.
    // Whitespace avoids interfering with JSON logs that also write to stderr.
    std::io::stderr().write(" ".as_bytes()).unwrap();

    let args = connector_init::Args::parse();

    // Map the LOG_LEVEL variable to an equivalent tracing EnvFilter.
    // Restrict logged modules to the current crate, as debug logging
    // for tonic can be quite verbose.
    let log_level = std::env::var("LOG_LEVEL").unwrap_or("info".to_string());
    let env_filter = tracing_subscriber::EnvFilter::new(format!(
        "flow_connector_init={log_level},connector_init={log_level}"
    ));

    // Map tracing::info!() and friends to instances of `Log` which are processed
    // by the stderr handler. Note that flow-connector-init *always* logs in the
    // canonical structured ops::Log format.
    tracing_subscriber::registry()
        .with(
            ops::tracing::Layer::new(ops::stderr_log_handler, std::time::SystemTime::now)
                .with_filter(env_filter),
        )
        .init();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build();

    let runtime = match runtime {
        Ok(runtime) => runtime,
        Err(error) => {
            tracing::error!(%error, "couldn't build Tokio runtime");
            std::process::exit(1);
        }
    };

    // Run until signaled, then gracefully stop.
    tracing::info!(%log_level, port=args.port, message = "connector-init started");
    let result = runtime.block_on(connector_init::run(args));

    // Explicitly call Runtime::shutdown_background as an alternative to calling Runtime::Drop.
    // This shuts down the runtime without waiting for blocking background tasks to complete,
    // which is good because they likely never will. Consider a blocking call to read from stdin,
    // where the sender is itself waiting for us to exit or write to our stdout.
    // (Note that tokio::io maps AsyncRead of file descriptors to blocking tasks under the hood).
    runtime.shutdown_background();

    if let Err(error) = result {
        tracing::error!(
            error = format!("{error:#}"),
            "connector-init crashed with error"
        );
    }
    tracing::info!("connector-init exiting");
}
