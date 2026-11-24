use anyhow::Context;
pub use codec::Codec;
use std::{io::Write, sync::atomic};

mod capture;
mod codec;
mod connector;
mod derive;
pub mod inspect;
mod materialize;
pub mod refusals;
pub mod rpc;
pub mod serve;

#[cfg(all(test, target_os = "linux"))]
mod vsock_test;

#[derive(clap::Parser, Debug)]
#[clap(about = "Command to start connector proxies for Flow runtime.")]
#[clap(group(
    clap::ArgGroup::new("listen").required(true).args(["port", "vsock_port"])
))]
pub struct Args {
    /// The path (in the container) to the JSON file that contains the
    /// inspection results from the connector image.
    /// Normally produced via command "docker inspect <image>".
    #[clap(short, long)]
    pub image_inspect_json_path: String,

    /// TCP port on which to listen for requests from the runtime.
    #[clap(short, long)]
    pub port: Option<u16>,

    /// AF_VSOCK port on which to listen for requests from the runtime,
    /// accepting from any CID. Used when the connector runs inside a
    /// micro-VM and the runtime reaches it through the hypervisor.
    #[clap(long)]
    pub vsock_port: Option<u32>,

    /// Host AF_VSOCK port at which a VMM's resolver reports DNS refusals,
    /// which are written to the connector's logs.
    #[clap(long)]
    pub resolver_vsock_port: Option<u32>,
}

// The listener is bound before anything else can fail, so that the readiness
// byte means the same thing on both transports: the runtime may now connect.
enum Listener {
    Tcp(tonic::transport::server::TcpIncoming),
    Vsock(tokio_vsock::VsockListener),
}

pub async fn run(
    Args {
        image_inspect_json_path,
        port,
        vsock_port,
        resolver_vsock_port,
    }: Args,
    log_level: String,
) -> anyhow::Result<()> {
    // Bind our port before we do anything else.
    let listener = match (port, vsock_port) {
        (Some(port), None) => {
            let addr = format!("0.0.0.0:{}", port).parse().unwrap();
            Listener::Tcp(
                tonic::transport::server::TcpIncoming::bind(addr)
                    .map_err(|e| anyhow::anyhow!("tcp incoming error {}", e))?
                    .with_nodelay(Some(true)),
            )
        }
        (None, Some(vsock_port)) => Listener::Vsock(
            tokio_vsock::VsockListener::bind(tokio_vsock::VsockAddr::new(
                tokio_vsock::VMADDR_CID_ANY,
                vsock_port,
            ))
            .context("binding vsock listener")?,
        ),
        _ => unreachable!("clap requires exactly one of --port and --vsock-port"),
    };

    // Now write a byte to stderr to let our container host know that we're alive.
    // Whitespace avoids interfering with JSON logs that also write to stderr.
    std::io::stderr().write(" ".as_bytes()).unwrap();
    tracing::debug!(
        %log_level,
        ?port,
        ?vsock_port,
        message = "connector-init started"
    );

    let image_inspect_json =
        std::fs::read(&image_inspect_json_path).context("reading image inspect JSON")?;
    let image = inspect::Image::parse_from_json_slice(&image_inspect_json)
        .context("parsing image inspect JSON")?;
    let entrypoint = image.get_argv()?;

    let codec = image.runtime_codec();

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

    let done = std::sync::Arc::new(tokio::sync::Notify::new());
    let connector =
        proto_grpc::connector::connector_server::ConnectorServer::new(connector::Proxy {
            entrypoint,
            codec,
            spec_alone: image.is_frozen_derive(),
            resolver_vsock_port,
            taken: Default::default(),
            done: done.clone(),
        })
        .max_decoding_message_size(usize::MAX) // Up from 4MB. Accept whatever the runtime sends.
        .max_encoding_message_size(usize::MAX); // The default, made explicit.

    // Stop once a served Connector stream is done. Otherwise, the watchdog
    // stops a server which no stream reaches, and the per-protocol services.
    let shutdown = async move {
        tokio::select! {
            () = watchdog() => (),
            () = done.notified() => (),
        }
    };

    let router = tonic::transport::Server::builder()
        .add_service(capture)
        .add_service(connector)
        .add_service(derive)
        .add_service(materialize);

    let () = match listener {
        Listener::Tcp(incoming) => {
            router
                .serve_with_incoming_shutdown(incoming, shutdown)
                .await?
        }
        Listener::Vsock(listener) => {
            router
                .serve_with_incoming_shutdown(listener.incoming(), shutdown)
                .await?
        }
    };

    Ok(())
}

fn check_protocol<R>(
    actual_protocol: Option<u32>,
    response: Result<R, tonic::Status>,
) -> Result<R, tonic::Status> {
    if let Some(actual_protocol) = actual_protocol {
        if EXPECT_PROTOCOL != actual_protocol {
            return Err(tonic::Status::internal(format!(
                "connector returned an unexpected protocol version {actual_protocol} (expected {EXPECT_PROTOCOL}"
            )));
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

#[cfg(test)]
mod test {
    use crate::serve::test::{SPEC, first, validate};
    use proto_flow::connector;

    #[tokio::test]
    async fn serves_one_stream_and_stops() {
        let script = format!(r#"read spec; echo '{SPEC}'; read v; echo '{{"validated":{{}}}}'"#);
        let dir = tempfile::tempdir().unwrap();
        let inspect = dir.path().join("image-inspect.json");
        std::fs::write(
            &inspect,
            serde_json::json!([{
                "Id": "test-image-id",
                "Config": {
                    "Entrypoint": ["/bin/sh", "-c", script],
                    "Labels": {"FLOW_RUNTIME_CODEC": "json"},
                    "Env": [],
                },
            }])
            .to_string(),
        )
        .unwrap();

        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let server = tokio::spawn(super::run(
            super::Args {
                image_inspect_json_path: inspect.display().to_string(),
                port: Some(port),
                vsock_port: None,
                resolver_vsock_port: None,
            },
            "info".to_string(),
        ));

        // `run` binds its port only once its task is polled.
        let endpoint = tonic::transport::Endpoint::new(format!("http://127.0.0.1:{port}")).unwrap();
        let channel = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                match endpoint.connect().await {
                    Ok(channel) => return channel,
                    Err(_) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
                }
            }
        })
        .await
        .expect("run binds its port");
        let mut client = proto_grpc::connector::connector_client::ConnectorClient::new(channel);

        let (request_tx, request_rx) = tokio::sync::mpsc::channel(4);
        request_tx.send(first("info")).await.unwrap();
        let mut responses = client
            .connector(tokio_stream::wrappers::ReceiverStream::new(request_rx))
            .await
            .unwrap()
            .into_inner();

        let started = responses.message().await.unwrap().unwrap();
        assert!(matches!(
            started.kind,
            Some(connector::response::Kind::Started(_))
        ));

        // While the first stream is open, a second is rejected.
        let second = client
            .connector(futures::stream::empty::<connector::Request>())
            .await
            .unwrap_err();
        assert_eq!(second.code(), tonic::Code::FailedPrecondition);

        request_tx.send(validate()).await.unwrap();
        std::mem::drop(request_tx);

        let validated = responses.message().await.unwrap().unwrap();
        assert!(matches!(
            validated.kind,
            Some(connector::response::Kind::Derive(_))
        ));
        assert!(responses.message().await.unwrap().is_none());

        // `run` returns once its one stream is done, well ahead of the watchdog.
        tokio::time::timeout(std::time::Duration::from_secs(3), server)
            .await
            .expect("run returns after its stream")
            .unwrap()
            .unwrap();
    }
}
