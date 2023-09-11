use anyhow::Context;
pub use codec::Codec;
use std::io::Write;
use tokio::signal::unix;
use tonic::transport::server::TcpIncoming;

mod capture;
mod codec;
mod derive;
mod inspect;
mod materialize;
mod proxy;
pub mod rpc;

#[derive(clap::Parser, Debug)]
#[clap(about = "Command to start connector proxies for Flow runtime.")]
pub struct Args {
    /// The path (in the container) to the JSON file that contains the
    /// inspection results from the connector image.
    /// Normally produced via command "docker inspect <image>".
    #[clap(short, long)]
    pub image_inspect_json_path: String,

    /// Port on which to listen for requests from the runtime.
    #[clap(short, long)]
    pub port: u16,
}

pub async fn run(
    Args {
        image_inspect_json_path,
        port,
    }: Args,
    log_level: String,
) -> anyhow::Result<()> {
    // Bind our port before we do anything else.
    let addr = format!("0.0.0.0:{}", port).parse().unwrap();
    let incoming = TcpIncoming::new(addr, true, None)
        .map_err(|e| anyhow::anyhow!("tcp incoming error {}", e))?;

    // Now write a byte to stderr to let our container host know that we're alive.
    // Whitespace avoids interfering with JSON logs that also write to stderr.
    std::io::stderr().write(" ".as_bytes()).unwrap();
    tracing::info!(%log_level, port, message = "connector-init started");

    let image = inspect::Image::parse_from_json_file(&image_inspect_json_path)
        .context("reading image inspect JSON")?;
    let entrypoint = image.get_argv()?;

    let codec = match image.get_label_or_env("FLOW_RUNTIME_CODEC") {
        Some(protocol) if protocol == "json" => Codec::Json,
        _ => Codec::Proto,
    };

    check_protocol(&entrypoint, codec).await?;

    let capture = proto_grpc::capture::connector_server::ConnectorServer::new(capture::Proxy {
        entrypoint: entrypoint.clone(),
        codec,
    })
    .max_decoding_message_size(usize::MAX) // Up from 4MB. Accept whatever the runtime sends.
    .max_encoding_message_size(usize::MAX); // The default, made explicit.

    let derive = proto_grpc::derive::connector_server::ConnectorServer::new(derive::Proxy {
        entrypoint: entrypoint.clone(),
        codec,
    })
    .max_decoding_message_size(usize::MAX) // Up from 4MB. Accept whatever the runtime sends.
    .max_encoding_message_size(usize::MAX); // The default, made explicit.

    let materialize =
        proto_grpc::materialize::connector_server::ConnectorServer::new(materialize::Proxy {
            entrypoint: entrypoint.clone(),
            codec,
        })
        .max_decoding_message_size(usize::MAX) // Up from 4MB. Accept whatever the runtime sends.
        .max_encoding_message_size(usize::MAX); // The default, made explicit.

    let proxy_handler = proxy::ProxyHandler::new("localhost");
    let proxy = proto_grpc::flow::network_proxy_server::NetworkProxyServer::new(proxy_handler);

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
        .add_service(derive)
        .add_service(materialize)
        .add_service(proxy)
        .serve_with_incoming_shutdown(incoming, signal)
        .await?;

    Ok(())
}

async fn check_protocol(entrypoint: &[String], codec: Codec) -> anyhow::Result<()> {
    // All protocols - capture, derive, & materialize - use the same tags for
    // request::Spec and for response::Spec::protocol.
    let spec_response: proto_flow::capture::Response = rpc::unary(
        rpc::new_command(&entrypoint),
        codec,
        proto_flow::capture::Request {
            spec: Some(proto_flow::capture::request::Spec {
                connector_type: 0,
                config_json: String::new(),
            }),
            ..Default::default()
        },
        ops::stderr_log_handler,
    )
    .await
    .map_err(|status| anyhow::anyhow!(status.message().to_string()))
    .context("querying for spec response")?;

    let actual_protocol = spec_response.spec.map(|s| s.protocol).unwrap_or_default();
    if EXPECT_PROTOCOL != actual_protocol {
        anyhow::bail!("connector returned an unexpected protocol version {actual_protocol} (expected {EXPECT_PROTOCOL}");
    }
    Ok(())
}

const EXPECT_PROTOCOL: u32 = 3032023;
