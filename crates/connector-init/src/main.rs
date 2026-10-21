use tracing_subscriber::prelude::*;

fn main() {
    let args = match <connector_init::Args as clap::Parser>::try_parse() {
        Ok(args) => args,
        // `--help` and `--version` are clap's other "errors", and go to stdout.
        Err(error) if !error.use_stderr() => {
            let _ = error.print();
            std::process::exit(error.exit_code());
        }
        Err(error) => {
            // The launcher treats a leading space on stderr as readiness;
            // prefix clap's indented argument lists to avoid signaling on failure.
            for line in error.render().to_string().trim_end().lines() {
                match line.is_empty() {
                    true => eprintln!("flow-connector-init:"),
                    false => eprintln!("flow-connector-init: {line}"),
                }
            }
            std::process::exit(error.exit_code());
        }
    };

    run(args);
}

fn run(args: connector_init::Args) {
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
    let handle = runtime.spawn(connector_init::run(args, log_level));
    let result = runtime.block_on(handle);

    // Explicitly call Runtime::shutdown_background as an alternative to calling Runtime::Drop.
    // This shuts down the runtime without waiting for blocking background tasks to complete,
    // which is good because they likely never will. Consider a blocking call to read from stdin,
    // where the sender is itself waiting for us to exit or write to our stdout.
    // (Note that tokio::io maps AsyncRead of file descriptors to blocking tasks under the hood).
    runtime.shutdown_background();

    if let Err(error) = result.unwrap() {
        tracing::error!(
            error = format!("{error:#}"),
            "connector-init crashed with error"
        );
        std::process::exit(1);
    }
    tracing::debug!("connector-init exiting");
}

#[test]
fn a_listener_bind_failure_exits_nonzero() {
    const PORT: &str = "CONNECTOR_INIT_TEST_OCCUPIED_PORT";
    if let Ok(port) = std::env::var(PORT) {
        run(connector_init::Args {
            image_inspect_json_path: "unused.json".to_string(),
            port: Some(port.parse().unwrap()),
            vsock_port: None,
        });
        return;
    }

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "a_listener_bind_failure_exits_nonzero",
            "--nocapture",
        ])
        .env(PORT, listener.local_addr().unwrap().port().to_string())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("tcp incoming error"), "{stderr}");
}
