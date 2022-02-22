pub mod connector_runners;
pub mod errors;
pub mod flow_capture_api;
pub mod flow_materialize_api;
pub mod plugins;

use clap::{ArgEnum, Parser, Subcommand};

use flow_capture_api::{FlowCapture, FlowCapturePlugin};
use flow_cli_common::{init_logging, LogArgs};
use flow_materialize_api::{FlowMaterialize, FlowMaterializePlugin};

use connector_runners::{
    airbyte_source_connector_runner::AirbyteSourceConnectorRunner,
    flow_capture_connector_runner::FlowCaptureConnectorRunner,
    flow_materialize_connector_runner::FlowMaterializeConnectorRunner,
};
use errors::Error;

use plugins::network_proxy_plugins::NetworkProxyPlugin;

#[derive(Debug, ArgEnum, Clone)]
pub enum FlowCaptureOperation {
    Spec,
    Discover,
    Validate,
    ApplyUpsert,
    ApplyDelete,
    Pull,
}

#[derive(Debug, ArgEnum, Clone)]
pub enum FlowCaptureConnectorProtocol {
    Airbyte,
    FlowCapture,
}

#[derive(Debug, clap::Parser)]
struct ProxyFlowCapture {
    /// The protocol that the connector is implemented in.
    #[clap(arg_enum)]
    connector_protocol: FlowCaptureConnectorProtocol,

    /// The operation (in the Flow runtime Capture Protocol) that is being proxied.
    #[clap(arg_enum)]
    operation: FlowCaptureOperation,
}

#[derive(Debug, ArgEnum, Clone)]
pub enum FlowMaterializeOperation {
    Spec,
    Validate,
    ApplyUpsert,
    ApplyDelete,
    Transactions,
}

#[derive(Debug, ArgEnum, Clone)]
pub enum FlowMaterializeConnectorProtocol {
    FlowMaterialize,
}

#[derive(Debug, clap::Parser)]
struct ProxyFlowMaterialize {
    /// The protocol that the connector is implemented in.
    #[clap(arg_enum)]
    connector_protocol: FlowMaterializeConnectorProtocol,
    /// The operation (in the Flow runtime Materialize Protocol) that is being proxied.
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
#[clap(about = "Command to start connector proxies for by flow runtime.")]
pub struct Args {
    /// The entrypoint to start the connector inside the docker container without proxy.
    #[clap(short, long)]
    connector_entrypoint: Vec<String>,

    /// The type of proxy service to provide.
    #[clap(subcommand)]
    proxy_command: ProxyCommand,

    #[clap(flatten)]
    log_args: LogArgs,
}

fn main() {
    let Args {
        connector_entrypoint,
        proxy_command,
        log_args,
    } = Args::parse();
    init_logging(&log_args);

    let result = match proxy_command {
        ProxyCommand::ProxyFlowMaterialize(m) => proxy_flow_materialize(m, connector_entrypoint),
        ProxyCommand::ProxyFlowCapture(c) => proxy_flow_capture(c, connector_entrypoint),
    };

    if let Err(err) = result.as_ref() {
        tracing::error!(error = ?err, "proxy execution failed.");
        std::process::exit(1);
    }
}

fn proxy_flow_materialize(
    m: ProxyFlowMaterialize,
    connector_entrypoint: Vec<String>,
) -> Result<(), Error> {
    let connector_runner: Box<dyn FlowMaterialize> = match m.connector_protocol {
        FlowMaterializeConnectorProtocol::FlowMaterialize => {
            Box::new(FlowMaterializeConnectorRunner {})
        }
    };

    // make plugins optional.
    let plugins: Vec<Box<dyn FlowMaterializePlugin>> = vec![Box::new(NetworkProxyPlugin {})];

    match m.operation {
        FlowMaterializeOperation::Spec => connector_runner.do_spec(connector_entrypoint, plugins),
        FlowMaterializeOperation::Validate => {
            connector_runner.do_validate(connector_entrypoint, plugins)
        }
        FlowMaterializeOperation::ApplyUpsert => {
            connector_runner.do_apply_upsert(connector_entrypoint, plugins)
        }
        FlowMaterializeOperation::ApplyDelete => {
            connector_runner.do_apply_delete(connector_entrypoint, plugins)
        }
        FlowMaterializeOperation::Transactions => {
            connector_runner.do_transactions(connector_entrypoint, plugins)
        }
    }
}
fn proxy_flow_capture(c: ProxyFlowCapture, connector_entrypoint: Vec<String>) -> Result<(), Error> {
    let connector_runner: Box<dyn FlowCapture> = match c.connector_protocol {
        FlowCaptureConnectorProtocol::Airbyte => Box::new(AirbyteSourceConnectorRunner {}),
        FlowCaptureConnectorProtocol::FlowCapture => Box::new(FlowCaptureConnectorRunner {}),
    };

    let plugins: Vec<Box<dyn FlowCapturePlugin>> = vec![Box::new(NetworkProxyPlugin {})];

    match c.operation {
        FlowCaptureOperation::Spec => connector_runner.do_spec(connector_entrypoint, plugins),
        FlowCaptureOperation::Discover => {
            connector_runner.do_discover(connector_entrypoint, plugins)
        }
        FlowCaptureOperation::Validate => {
            connector_runner.do_validate(connector_entrypoint, plugins)
        }
        FlowCaptureOperation::ApplyUpsert => {
            connector_runner.do_apply_upsert(connector_entrypoint, plugins)
        }
        FlowCaptureOperation::ApplyDelete => {
            connector_runner.do_apply_delete(connector_entrypoint, plugins)
        }
        FlowCaptureOperation::Pull => connector_runner.do_pull(connector_entrypoint, plugins),
    }
}
