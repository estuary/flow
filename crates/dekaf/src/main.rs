// Links in the allocator crate, which sets the global allocator to jemalloc
extern crate allocator;

use anyhow::{bail, Context};
use axum_server::tls_rustls::RustlsConfig;
use clap::{Args, Parser};
use dekaf::{KafkaApiClient, Session};
use flow_client::{DEFAULT_PG_PUBLIC_TOKEN, DEFAULT_PG_URL, LOCAL_PG_PUBLIC_TOKEN, LOCAL_PG_URL};
use futures::{FutureExt, TryStreamExt};
use rsasl::config::SASLConfig;
use rustls::pki_types::CertificateDer;
use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::io::{split, AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing_subscriber::{filter::LevelFilter, EnvFilter};
use url::Url;

/// A Kafka-compatible proxy for reading Estuary Flow collections.
#[derive(Debug, Parser, serde::Serialize)]
#[command(about, version)]
pub struct Cli {
    /// Endpoint of the Estuary API to use.
    #[arg(
        long,
        default_value = DEFAULT_PG_URL.as_str(),
        env = "API_ENDPOINT"
    )]
    api_endpoint: Url,
    /// Public (anon) API key to use during authentication to the Estuary API.
    #[arg(
        long,
        default_value = DEFAULT_PG_PUBLIC_TOKEN,
        env = "API_KEY"
    )]
    api_key: String,

    /// When true, override the configured API endpoint and token,
    /// in preference of a local control plane.
    #[arg(long)]
    local: bool,
    /// The hostname to advertise when enumerating Kafka "brokers".
    /// This is the hostname at which `dekaf` may be accessed.
    #[arg(long, default_value = "127.0.0.1", env = "ADVERTISE_HOST")]
    advertise_host: String,
    /// The port to listen on and advertise for Kafka API access.
    #[arg(long, default_value = "9092", env = "KAFKA_PORT")]
    kafka_port: u16,
    /// The port to listen on for schema registry API requests.
    #[arg(long, default_value = "9093", env = "SCHEMA_REGISTRY_PORT")]
    schema_registry_port: u16,
    /// The port to listen on for prometheus metrics
    #[arg(long, default_value = "9094", env = "METRICS_PORT")]
    metrics_port: u16,

    /// The hostname of the default Kafka broker to use for serving group management APIs
    #[arg(long, env = "DEFAULT_BROKER_HOSTNAME")]
    default_broker_hostname: String,
    /// The port of the default Kafka broker to use for serving group management APIs
    #[arg(long, default_value = "9092", env = "DEFAULT_BROKER_PORT")]
    default_broker_port: u16,
    /// The username for the default Kafka broker to use for serving group management APIs.
    /// Currently only supports SASL PLAIN username/password auth.
    #[arg(long, env = "DEFAULT_BROKER_USERNAME")]
    default_broker_username: String,
    /// The password for the default Kafka broker to use for serving group management APIs
    #[arg(long, env = "DEFAULT_BROKER_PASSWORD")]
    default_broker_password: String,

    /// The secret used to encrypt/decrypt potentially sensitive strings when sending them
    /// to the upstream Kafka broker, e.g topic names in group management metadata.
    #[arg(long, env = "ENCRYPTION_SECRET")]
    encryption_secret: String,

    #[command(flatten)]
    tls: Option<TlsArgs>,
}

#[derive(Args, Debug, serde::Serialize)]
#[group(required = false)]
struct TlsArgs {
    /// The certificate file used to serve TLS connections. If provided, Dekaf must not be
    /// behind a TLS-terminating proxy and instead be directly exposed.
    #[arg(long, env = "CERTIFICATE_FILE", requires = "certificate_key_file")]
    certificate_file: Option<PathBuf>,
    /// The key file used to serve TLS connections. If provided, Dekaf must not be
    /// behind a TLS-terminating proxy and instead be directly exposed.
    #[arg(long, env = "CERTIFICATE_KEY_FILE", requires = "certificate_file")]
    certificate_key_file: Option<PathBuf>,
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

    metrics_prometheus::install();

    let cli = Cli::parse();
    tracing::info!("Starting dekaf");

    let (api_endpoint, api_key) = if cli.local {
        (LOCAL_PG_URL.to_owned(), LOCAL_PG_PUBLIC_TOKEN.to_string())
    } else {
        (cli.api_endpoint, cli.api_key)
    };

    let upstream_kafka_host = format!(
        "tcp://{}:{}",
        cli.default_broker_hostname, cli.default_broker_port
    );

    let app = Arc::new(dekaf::App {
        advertise_host: cli.advertise_host.to_owned(),
        advertise_kafka_port: cli.kafka_port,
        kafka_client: KafkaApiClient::connect(
            upstream_kafka_host.as_str(),
            SASLConfig::with_credentials(
                None,
                cli.default_broker_username,
                cli.default_broker_password,
            )?,
        ).await.context(
            "failed to connect or authenticate to upstream Kafka broker used for serving group management APIs",
        )?,
        secret: cli.encryption_secret.to_owned(),
        api_endpoint,
        api_key
    });

    tracing::info!(
        broker_url = upstream_kafka_host,
        "Successfully authenticated to upstream Kafka broker"
    );

    let mut stop = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for CTRL-C")
    }
    .shared();

    let schema_addr = format!("[::]:{}", cli.schema_registry_port).parse()?;
    let metrics_addr = format!("[::]:{}", cli.metrics_port).parse()?;
    // Build a listener for Kafka sessions.
    let kafka_listener = tokio::net::TcpListener::bind(format!("[::]:{}", cli.kafka_port))
        .await
        .context("failed to bind server port")?;

    let metrics_router = dekaf::metrics_server::build_router(app.clone());
    let metrics_server_task =
        axum_server::bind(metrics_addr).serve(metrics_router.into_make_service());
    tokio::spawn(async move { metrics_server_task.await.unwrap() });

    let schema_router = dekaf::registry::build_router(app.clone());
    if let Some(tls_cfg) = cli.tls {
        let axum_rustls_config = RustlsConfig::from_pem_file(
            tls_cfg.certificate_file.clone().unwrap(),
            tls_cfg.certificate_key_file.clone().unwrap(),
        )
        .await
        .context("failed to open or read certificate or certificate key file")?;

        let schema_server_task = axum_server::bind_rustls(schema_addr, axum_rustls_config.clone())
            .serve(schema_router.into_make_service());

        let certs = load_certs(&tls_cfg.certificate_file.unwrap())?;
        let key = load_key(&tls_cfg.certificate_key_file.unwrap())?;

        // Verify that our advertise-host is one of the cert's CNs
        if validate_certificate_name(&certs, &cli.advertise_host)? {
            tracing::info!(
                found_name = cli.advertise_host,
                "Validated TLS certificate, Dekaf will terminate TLS"
            )
        } else {
            bail!(format!(
                "Provided certificate does not include '{}' as a common or alternative name",
                cli.advertise_host
            ))
        }

        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));

        tokio::spawn(async move { schema_server_task.await.unwrap() });
        // Accept and serve Kafka sessions until we're signaled to stop.
        loop {
            let acceptor = acceptor.clone();
            tokio::select! {
                accept = kafka_listener.accept() => {
                    let Ok((socket, addr)) = accept else {
                        continue
                    };
                    let Ok(socket) = acceptor.accept(socket).await else {
                        continue
                    };

                    tokio::spawn(serve(Session::new(app.clone(), cli.encryption_secret.to_owned()), socket, addr, stop.clone()));
                }
                _ = &mut stop => break,
            }
        }
    } else {
        tracing::info!("No TLS certificate provided, Dekaf will not terminate TLS");
        let schema_server_task =
            axum_server::bind(schema_addr).serve(schema_router.into_make_service());

        tokio::spawn(async move { schema_server_task.await.unwrap() });

        // Accept and serve Kafka sessions until we're signaled to stop.
        loop {
            tokio::select! {
                accept = kafka_listener.accept() => {
                    let Ok((socket, addr)) = accept else {
                        continue
                    };
                    socket.set_nodelay(true)?;

                    tokio::spawn(serve(Session::new(app.clone(), cli.encryption_secret.to_owned()), socket, addr, stop.clone()));
                }
                _ = &mut stop => break,
            }
        }
    };

    Ok(())
}

#[tracing::instrument(level = "info", ret, err(Debug, level = "warn"), skip(session, socket, _stop), fields(?addr))]
async fn serve<S>(
    mut session: Session,
    socket: S,
    addr: std::net::SocketAddr,
    _stop: impl futures::Future<Output = ()>, // TODO(johnny): stop.
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    tracing::info!("accepted client connection");
    metrics::gauge!("total_connections").increment(1);
    let result = async {
        let (r, mut w) = split(socket);

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
            if let err @ Err(_) =
                dekaf::dispatch_request_frame(&mut session, &mut raw_sasl_auth, frame, &mut out)
                    .await
            {
                // Close the connection on error
                w.shutdown().await?;
                return err;
            }
            () = w.write_all(&mut out).await?;
            out.clear();
        }

        Ok(())
    }
    .await;

    metrics::gauge!("total_connections").decrement(1);

    result
}

fn load_certs(path: &Path) -> io::Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    rustls_pemfile::certs(&mut io::BufReader::new(File::open(path)?)).collect()
}

fn load_key(path: &Path) -> io::Result<rustls::pki_types::PrivateKeyDer<'static>> {
    Ok(
        rustls_pemfile::private_key(&mut io::BufReader::new(File::open(path)?))
            .unwrap()
            .ok_or(io::Error::new(
                io::ErrorKind::Other,
                "no private key found".to_string(),
            ))?,
    )
}

fn validate_certificate_name(
    certs: &Vec<CertificateDer>,
    advertise_host: &str,
) -> anyhow::Result<bool> {
    let parsed_name = webpki::DnsNameRef::try_from_ascii_str(advertise_host)
        .ok()
        .context(format!(
            "Attempting to parse {advertise_host} as a DNS name"
        ))?;
    for cert in certs.iter() {
        match webpki::EndEntityCert::try_from(cert.as_ref())
            .map_err(|e| anyhow::anyhow!(format!("Failed to parse provided certificate: {:?}", e)))?
            .verify_is_valid_for_dns_name(parsed_name)
        {
            Ok(_) => return Ok(true),
            Err(e) => tracing::debug!(e=?e, "Certificate is not valid for provided hostname"),
        }
    }
    return Ok(false);
}
