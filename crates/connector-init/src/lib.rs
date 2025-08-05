use anyhow::Context;
pub use codec::Codec;
use std::{io::Write, sync::atomic};
use tonic::transport::server::TcpIncoming;

mod capture;
mod codec;
mod derive;
mod inspect;
mod materialize;
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
    tracing::debug!(%log_level, port, message = "connector-init started");

    let image = inspect::Image::parse_from_json_file(&image_inspect_json_path)
        .context("reading image inspect JSON")?;
    let entrypoint = image.get_argv()?;

    let codec = match image.get_label_or_env("FLOW_RUNTIME_CODEC") {
        Some(protocol) if protocol == "json" => Codec::Json,
        _ => Codec::Proto,
    };

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

    let () = tonic::transport::Server::builder()
        .add_service(capture)
        .add_service(derive)
        .add_service(materialize)
        .serve_with_incoming_shutdown(incoming, watchdog())
        .await?;

    Ok(())
}

fn check_protocol<R>(
    actual_protocol: Option<u32>,
    response: Result<R, tonic::Status>,
) -> Result<R, tonic::Status> {
    if let Some(actual_protocol) = actual_protocol {
        if EXPECT_PROTOCOL != actual_protocol {
            return Err(tonic::Status::internal(format!(
                "connector returned an unexpected protocol version {actual_protocol} (expected {EXPECT_PROTOCOL}")));
        }
    }
    response
}

// IncOnDrop defers an increment of a metric until it's dropped.
struct IncOnDrop(&'static atomic::AtomicUsize);

impl Drop for IncOnDrop {
    fn drop(&mut self) {
        inc(self.0)
    }
}

fn inc(metric: &'static atomic::AtomicUsize) {
    metric.fetch_add(1, atomic::Ordering::SeqCst);
}

// watchdog returns when no RPCs are started or are still running for a period of time.
async fn watchdog() {
    let mut prev_handled = 0;

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // Stop if no RPCs are running or have been started since our last iteration.
        if GRPC_SERVER_STARTED_TOTAL.load(atomic::Ordering::SeqCst) == prev_handled {
            return;
        }
        prev_handled = GRPC_SERVER_HANDLED_TOTAL.load(atomic::Ordering::SeqCst);
    }
}

// TODO(johnny): Integrate with prometheus crate and export as metrics.
static GRPC_SERVER_STARTED_TOTAL: atomic::AtomicUsize = atomic::AtomicUsize::new(0);
static GRPC_SERVER_HANDLED_TOTAL: atomic::AtomicUsize = atomic::AtomicUsize::new(0);

const EXPECT_PROTOCOL: u32 = 3032023;
