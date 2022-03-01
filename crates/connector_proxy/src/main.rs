pub mod apis;
pub mod connector_runner;
pub mod errors;
pub mod interceptors;
pub mod libs;

use clap::{ArgEnum, Parser, Subcommand};
use tokio::signal::unix::{signal, SignalKind};

use apis::{compose, FlowCaptureOperation, FlowMaterializeOperation, Interceptor};

use flow_cli_common::{init_logging, LogArgs};

use connector_runner::run_connector;
use errors::Error;
use libs::image_config::ImageConfig;

use interceptors::{
    airbyte_capture_interceptor::AirbyteCaptureInterceptor,
    default_interceptors::{DefaultFlowCaptureInterceptor, DefaultFlowMaterializeInterceptor},
    network_proxy_capture_interceptor::NetworkProxyCaptureInterceptor,
    network_proxy_materialize_interceptor::NetworkProxyMaterializeInterceptor,
};

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

#[derive(Debug, Subcommand)]
enum ProxyCommand {
    /// proxies the Flow runtime Capture Protocol to the connector.
    ProxyFlowCapture(ProxyFlowCapture),
    /// proxies the Flow runtime Materialize Protocol to the connector.
    ProxyFlowMaterialize(ProxyFlowMaterialize),
}

#[derive(Parser, Debug)]
#[clap(about = "Command to start connector proxies for Flow runtime.")]
pub struct Args {
    /// The path (in the container) to the JSON file that contains the inspection results from the connector image.
    /// Normally produced via command "docker inspect <image>".
    #[clap(short, long)]
    image_inspect_json_path: String,

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

    // respond to os signals.
    tokio::task::spawn(async move { signal_handler().await });

    let result = async_main(image_inspect_json_path, proxy_command).await;
    if let Err(err) = result.as_ref() {
        tracing::error!(error = %err, "connector proxy execution failed.");
        std::process::exit(1);
    }
    Ok(())
}

async fn signal_handler() {
    let mut signal_stream = signal(SignalKind::terminate()).expect("failed creating signal.");

    signal_stream
        .recv()
        .await
        .expect("failed receiving os signals.");
    tracing::info!("connector proxy stopped.");
    std::process::exit(0);
}

async fn async_main(
    image_inspect_json_path: String,
    proxy_command: ProxyCommand,
) -> Result<(), Error> {
    let image_config = ImageConfig::parse_from_json_file(image_inspect_json_path)?;

    match proxy_command {
        ProxyCommand::ProxyFlowCapture(c) => proxy_flow_capture(c, image_config).await,
        ProxyCommand::ProxyFlowMaterialize(m) => proxy_flow_materialize(m, image_config).await,
    }
}

async fn proxy_flow_capture(c: ProxyFlowCapture, image_config: ImageConfig) -> Result<(), Error> {
    let mut converter_pair = match image_config
        .get_connector_protocol::<CaptureConnectorProtocol>(CaptureConnectorProtocol::Airbyte)
    {
        CaptureConnectorProtocol::FlowCapture => DefaultFlowCaptureInterceptor::get_converters(),
        CaptureConnectorProtocol::Airbyte => AirbyteCaptureInterceptor::get_converters(),
    };

    converter_pair = compose(
        converter_pair,
        NetworkProxyCaptureInterceptor::get_converters(),
    );

    run_connector::<FlowCaptureOperation>(
        c.operation,
        image_config.get_entrypoint(vec![DEFAULT_CONNECTOR_ENTRYPOINT.to_string()]),
        converter_pair,
    )
    .await
}

async fn proxy_flow_materialize(
    m: ProxyFlowMaterialize,
    image_config: ImageConfig,
) -> Result<(), Error> {
    // There is only one type of connector protocol for flow materialize.
    let mut converter_pair = DefaultFlowMaterializeInterceptor::get_converters();
    converter_pair = compose(
        converter_pair,
        NetworkProxyMaterializeInterceptor::get_converters(),
    );

    run_connector::<FlowMaterializeOperation>(
        m.operation,
        image_config.get_entrypoint(vec![DEFAULT_CONNECTOR_ENTRYPOINT.to_string()]),
        converter_pair,
    )
    .await
}
