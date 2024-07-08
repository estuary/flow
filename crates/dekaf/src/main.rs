use anyhow::Context;
use clap::Parser;
use dekaf::{KafkaApiClient, Session};
use futures::{FutureExt, TryStreamExt};
use rsasl::config::SASLConfig;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tracing_subscriber::{filter::LevelFilter, EnvFilter};

/// A Kafka-compatible proxy for reading Estuary Flow collections.
#[derive(Debug, Parser, serde::Serialize)]
#[clap(about, version)]
pub struct Cli {
    /// Endpoint of the Estuary API to use.
    #[clap(
        long,
        default_value = MANAGED_API_ENDPOINT,
        env = "API_ENDPOINT"
    )]
    api_endpoint: String,
    /// Public (anon) API key to use during authentication to the Estuary API.
    #[clap(
        long,
        default_value = MANAGED_API_KEY,
        env = "API_KEY"
    )]
    api_key: String,

    /// When true, override the configured API endpoint and token,
    /// in preference of a local control plane.
    #[clap(long)]
    local: bool,
    /// The hostname to advertise when enumerating Kafka "brokers".
    /// This is the hostname at which `dekaf` may be accessed.
    #[clap(long, default_value = "127.0.0.1", env = "ADVERTISE_HOST")]
    advertise_host: String,
    /// The port to listen on and advertise for Kafka API access.
    #[clap(long, default_value = "9092", env = "KAFKA_PORT")]
    kafka_port: u16,
    /// The port to listen on for schema registry API requests.
    #[clap(long, default_value = "9093", env = "SCHEMA_REGISTRY_PORT")]
    schema_registry_port: u16,

    /// The hostname of the default Kafka broker to use for serving group management APIs
    #[clap(long, env = "DEFAULT_BROKER_HOSTNAME")]
    default_broker_hostname: String,
    /// The port of the default Kafka broker to use for serving group management APIs
    #[clap(long, default_value = "9092", env = "DEFAULT_BROKER_PORT")]
    default_broker_port: u16,
    /// The username for the default Kafka broker to use for serving group management APIs.
    /// Currently only supports SASL PLAIN username/password auth.
    #[clap(long, env = "DEFAULT_BROKER_USERNAME")]
    default_broker_username: String,
    /// The password for the default Kafka broker to use for serving group management APIs
    #[clap(long, env = "DEFAULT_BROKER_PASSWORD")]
    default_broker_password: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::WARN.into()) // Otherwise it's ERROR.
        .from_env_lossy();

    tracing_subscriber::fmt::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    tracing::info!(args=?ops::DebugJson(&cli), "starting");

    let (api_endpoint, api_token) = if cli.local {
        (LOCAL_API_ENDPOINT, LOCAL_API_KEY)
    } else {
        (cli.api_endpoint.as_str(), cli.api_key.as_str())
    };

    let upstream_kafka_host = format!(
        "tcp://{}:{}",
        cli.default_broker_hostname, cli.default_broker_port
    );

    let app = Arc::new(dekaf::App {
        anon_client: postgrest::Postgrest::new(api_endpoint).insert_header("apikey", api_token),
        advertise_host: cli.advertise_host,
        advertise_kafka_port: cli.kafka_port,
        kafka_client: KafkaApiClient::new(
            upstream_kafka_host.as_str(),
            SASLConfig::with_credentials(
                None,
                cli.default_broker_username,
                cli.default_broker_password,
            )?,
        ),
    });

    app.kafka_client.validate_auth().await.context(
        "failed to connect or authenticate to upstream Kafka broker used for serving group management APIs",
    )?;

    tracing::info!(
        broker_url = upstream_kafka_host,
        "Successfully authenticated to upstream Kafka broker"
    );

    // Build a server which listens and serves supported schema registry requests.
    let schema_listener =
        tokio::net::TcpListener::bind(format!("[::]:{}", cli.schema_registry_port))
            .await
            .context("failed to bind server port")?;
    let schema_router = dekaf::registry::build_router(app.clone());

    let schema_server_task = axum::serve(schema_listener, schema_router);
    tokio::spawn(async move { schema_server_task.await.unwrap() });

    // Build a listener for Kafka sessions.
    let kafka_listener = tokio::net::TcpListener::bind(format!("[::]:{}", cli.kafka_port))
        .await
        .context("failed to bind server port")?;

    let mut stop = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for CTRL-C")
    }
    .shared();

    // Accept and serve Kafka sessions until we're signaled to stop.
    loop {
        tokio::select! {
            accept = kafka_listener.accept() => {
                let (socket, addr) = accept?;

                tokio::spawn(serve(Session::new(app.clone()), socket, addr, stop.clone()));
            }
            _ = &mut stop => break,
        }
    }

    Ok(())
}

#[tracing::instrument(level = "info", ret, err(Debug, level = "warn"), skip(session, socket, _stop), fields(?addr))]
async fn serve(
    mut session: Session,
    mut socket: tokio::net::TcpStream,
    addr: std::net::SocketAddr,
    _stop: impl futures::Future<Output = ()>, // TODO(johnny): stop.
) -> anyhow::Result<()> {
    tracing::info!("accepted client connection");

    socket.set_nodelay(true)?;
    let (r, mut w) = socket.split();

    let mut r = tokio_util::codec::FramedRead::new(
        r,
        tokio_util::codec::LengthDelimitedCodec::builder()
            .big_endian()
            .length_field_length(4)
            .max_frame_length(1 << 27) // 128 MiB
            .new_codec(),
    );

    let mut out = bytes::BytesMut::new();
    let mut raw_sasl_auth = false;
    while let Some(frame) = r.try_next().await? {
        () = dekaf::dispatch_request_frame(&mut session, &mut raw_sasl_auth, frame, &mut out)
            .await?;
        () = w.write_all(&mut out).await?;
        out.clear();
    }
    Ok(())
}

const MANAGED_API_KEY: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5cmNubXV6enlyaXlwZGFqd2RrIiwicm9sZSI6ImFub24iLCJpYXQiOjE2NDg3NTA1NzksImV4cCI6MTk2NDMyNjU3OX0.y1OyXD3-DYMz10eGxzo1eeamVMMUwIIeOoMryTRAoco";
const MANAGED_API_ENDPOINT: &str = "https://eyrcnmuzzyriypdajwdk.supabase.co/rest/v1";

const LOCAL_API_KEY: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0";
const LOCAL_API_ENDPOINT: &str = "http://127.0.0.1:5431/rest/v1";
