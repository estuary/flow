use std::collections::BTreeMap;

use anyhow::Context;
use tokio::signal::unix;

mod capture;
mod inspect;
mod materialize;
mod proxy;
mod rpc;

#[derive(clap::Parser, Debug)]
#[clap(about = "Command to start connector proxies for Flow runtime.")]
pub struct Args {
    /// The path (in the container) to the JSON file that contains the
    /// inspection results from the connector image.
    /// Normally produced via command "docker inspect <image>".
    #[clap(short, long)]
    pub image_inspect_json_path: String,

    /// Port on which to listen for requests from the runtime.
    #[clap(short, long, default_value = "8080")]
    pub port: u16,

    #[clap(long, value_parser = parse_expose_val)]
    pub expose: Vec<(String, u16)>,
}

/// Parse a single expose argument value, given in the form of: portName=80
fn parse_expose_val(
    s: &str,
) -> Result<(String, u16), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let pos = s
        .find('=')
        .ok_or_else(|| anyhow::anyhow!("invalid --expose argument: '{}'", s))?;
    let port_name = s[..pos].trim().to_string();
    let port_num = s[pos + 1..].parse().context("invalid u16 port value")?;
    Ok((port_name, port_num))
}

pub async fn run(mut args: Args) -> anyhow::Result<()> {
    let image = inspect::Image::parse_from_json_file(&args.image_inspect_json_path)
        .context("reading image inspect JSON")?;
    let mut entrypoint = image.get_argv()?;

    // TODO(johnny): Remove this in preference of always using the LOG_LEVEL variable.
    if let Ok(log_level) = std::env::var("LOG_LEVEL") {
        entrypoint.push(format!("--log.level={log_level}"));
    }
    let expose_ports = args.expose.drain(..).collect::<BTreeMap<_, _>>();

    let mut proxy_handler = proxy::ProxyHandler::new("localhost", expose_ports);

    let capture = proto_grpc::capture::driver_server::DriverServer::new(capture::Driver {
        entrypoint: entrypoint.clone(),
    });
    let materialize =
        proto_grpc::materialize::driver_server::DriverServer::new(materialize::Driver {
            entrypoint: entrypoint.clone(),
        });

    let proxy = proto_grpc::flow::network_proxy_server::NetworkProxyServer::new(proxy_handler);

    let addr = format!("0.0.0.0:{}", args.port).parse().unwrap();

    // Gracefully exit on either SIGINT (ctrl-c) or SIGTERM.
    let mut sigint = unix::signal(unix::SignalKind::interrupt()).unwrap();
    let mut sigterm = unix::signal(unix::SignalKind::terminate()).unwrap();

    let signal = async move {
        tokio::select! {
            _ = sigint.recv() => (),
            _ = sigterm.recv() => (),
        }
        tracing::info!("caught signal to exit");
    };

    let () = tonic::transport::Server::builder()
        .add_service(capture)
        .add_service(materialize)
        .add_service(proxy)
        .serve_with_shutdown(addr, signal)
        .await?;

    Ok(())
}
