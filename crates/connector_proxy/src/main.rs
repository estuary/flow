pub mod apis;
pub mod connector_runner;
pub mod errors;
pub mod interceptors;
pub mod libs;
use std::fs::File;
use std::io::BufReader;

use clap::{ArgEnum, Parser, Subcommand};
use tokio::{
    io::AsyncReadExt,
    signal::unix::{signal, SignalKind},
};

use apis::{FlowCaptureOperation, FlowMaterializeOperation, FlowRuntimeProtocol};

use flow_cli_common::{init_logging, LogArgs};

use connector_runner::{
    run_airbyte_source_connector, run_flow_capture_connector, run_flow_materialize_connector,
};
use errors::Error;
use libs::{
    command::{check_exit_status, invoke_connector, read_ready, CommandConfig},
    image_inspect::ImageInspect,
};
use std::process::Stdio;

#[derive(Debug, ArgEnum, Clone)]
pub enum CaptureConnectorProtocol {
    Airbyte,
    FlowCapture,
}

#[derive(Debug, clap::Parser)]
struct ProxyFlowCapture {
    /// The operation (in the FlowCapture Protocol) that is being proxied.
    #[clap(arg_enum)]
    operation: FlowCaptureOperation,
}

#[derive(Debug, ArgEnum, Clone)]
pub enum MaterializeConnectorProtocol {
    FlowMaterialize,
}

#[derive(Debug, clap::Parser)]
struct ProxyFlowMaterialize {
    /// The operation (in the FlowMaterialize Protocol) that is being proxied.
    #[clap(arg_enum)]
    operation: FlowMaterializeOperation,
}

#[derive(Debug, clap::Parser)]
struct DelayedExecutionConfig {
    config_file_path: String,
}

#[derive(Debug, Subcommand)]
enum ProxyCommand {
    /// proxies the Flow runtime Capture Protocol to the connector.
    ProxyFlowCapture(ProxyFlowCapture),
    /// proxies the Flow runtime Materialize Protocol to the connector.
    ProxyFlowMaterialize(ProxyFlowMaterialize),
    /// internal command used by the connector proxy itself to delay execution until signaled.
    DelayedExecute(DelayedExecutionConfig),
}

#[derive(Parser, Debug)]
#[clap(about = "Command to start connector proxies for Flow runtime.")]
pub struct Args {
    /// The path (in the container) to the JSON file that contains the inspection results from the connector image.
    /// Normally produced via command "docker inspect <image>".
    #[clap(short, long)]
    image_inspect_json_path: Option<String>,

    /// The type of proxy service to provide.
    #[clap(subcommand)]
    proxy_command: ProxyCommand,

    #[clap(flatten)]
    log_args: LogArgs,
}

static DEFAULT_CONNECTOR_ENTRYPOINT: &str = "/connector/connector";

// The connector proxy is a service between Flow Runtime and connectors. It adapts the protocol of the Flow Runtime to
// to protocol of the connector, and allows additional functionalities to be triggered during the communications.
// 1. The protocol of Flow Runtime is determined by proxyCommand,
//    "proxy-flow-capture" and "proxy-flow-materialize" for FlowCapture and FlowMaterialize protocols, respectively.
// 2. The interceptors translate the Flow Runtime protocols to the native protocols of different connectors, and add functionalities
//    that affect multiple operations during the communication. E.g. network proxy needs to modify the spec response to add the
//    network proxy specs, and starts the network proxy for the rest commands. See apis.rs for details.nd allows additional
//    functionalities to be triggered during the communications.

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let Args {
        image_inspect_json_path,
        proxy_command,
        log_args,
    } = Args::parse();
    init_logging(&log_args);

    let result = async_main(image_inspect_json_path, proxy_command).await;
    if let Err(err) = result.as_ref() {
        tracing::error!(error = ?err, "connector proxy execution failed.");
        std::process::exit(1);
    }
    Ok(())
}

async fn sigterm_handler() {
    let mut signal_stream = signal(SignalKind::terminate()).expect("failed creating signal.");

    signal_stream
        .recv()
        .await
        .expect("failed receiving os signals.");
    tracing::info!("connector proxy stopped.");
    std::process::exit(0);
}

async fn async_main(
    image_inspect_json_path: Option<String>,
    proxy_command: ProxyCommand,
) -> Result<(), Error> {
    match proxy_command {
        ProxyCommand::ProxyFlowCapture(c) => proxy_flow_capture(c, image_inspect_json_path).await,
        ProxyCommand::ProxyFlowMaterialize(m) => {
            proxy_flow_materialize(m, image_inspect_json_path).await
        }
        ProxyCommand::DelayedExecute(ba) => delayed_execute(ba.config_file_path).await,
    }
}

async fn proxy_flow_capture(
    c: ProxyFlowCapture,
    image_inspect_json_path: Option<String>,
) -> Result<(), Error> {
    let image_inspect = ImageInspect::parse_from_json_file(image_inspect_json_path)?;
    if image_inspect.infer_runtime_protocol() != FlowRuntimeProtocol::Capture {
        return Err(Error::MismatchingRuntimeProtocol);
    }

    let entrypoint = image_inspect.get_entrypoint(vec![DEFAULT_CONNECTOR_ENTRYPOINT.to_string()]);

    match image_inspect
        .get_connector_protocol::<CaptureConnectorProtocol>(CaptureConnectorProtocol::Airbyte)
    {
        CaptureConnectorProtocol::FlowCapture => {
            run_flow_capture_connector(&c.operation, entrypoint).await
        }
        CaptureConnectorProtocol::Airbyte => {
            run_airbyte_source_connector(&c.operation, entrypoint).await
        }
    }
}

async fn proxy_flow_materialize(
    m: ProxyFlowMaterialize,
    image_inspect_json_path: Option<String>,
) -> Result<(), Error> {
    // Respond to OS sigterm signal.
    tokio::task::spawn(async move { sigterm_handler().await });

    let image_inspect = ImageInspect::parse_from_json_file(image_inspect_json_path)?;
    if image_inspect.infer_runtime_protocol() != FlowRuntimeProtocol::Materialize {
        return Err(Error::MismatchingRuntimeProtocol);
    }

    run_flow_materialize_connector(
        &m.operation,
        image_inspect.get_entrypoint(vec![DEFAULT_CONNECTOR_ENTRYPOINT.to_string()]),
    )
    .await
}

async fn delayed_execute(command_config_path: String) -> Result<(), Error> {
    // Wait for the "READY" signal from the parent process before starting the connector.
    read_ready(&mut tokio::io::stdin()).await?;

    tracing::info!("delayed process execution continue...");

    let reader = BufReader::new(File::open(command_config_path)?);
    let command_config: CommandConfig = serde_json::from_reader(reader)?;

    let mut child = invoke_connector(
        Stdio::inherit(),
        Stdio::inherit(),
        Stdio::inherit(),
        &command_config.entrypoint,
        &command_config.args,
    )?;

    match check_exit_status("delayed process", child.wait().await) {
        Err(e) => {
            tracing::error!("connector failed. command_config: {:?}.", &command_config);
            Err(e)
        }
        _ => Ok(()),
    }
}
