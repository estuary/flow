// Links in the allocator crate, which sets the global allocator to jemalloc
extern crate allocator;

use anyhow::{bail, Context};
use axum_server::tls_rustls::RustlsConfig;
use clap::{Args, Parser};
use dekaf::{
    log_appender::GazetteWriter, logging, KafkaApiClient, KafkaClientAuth, Session, TaskManager,
};
use flow_client::{
    DEFAULT_AGENT_URL, DEFAULT_DATA_PLANE_FQDN, DEFAULT_PG_PUBLIC_TOKEN, DEFAULT_PG_URL,
    LOCAL_AGENT_URL, LOCAL_DATA_PLANE_FQDN, LOCAL_DATA_PLANE_HMAC, LOCAL_PG_PUBLIC_TOKEN,
    LOCAL_PG_URL,
};
use futures::TryStreamExt;
use rustls::pki_types::CertificateDer;
use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use url::Url;

/// A Kafka-compatible proxy for reading Estuary Flow collections.
#[derive(Debug, Parser, serde::Serialize)]
#[command(about, version)]
pub struct Cli {
    /// Endpoint of the Estuary API to use.
    #[arg(
        long,
        default_value = DEFAULT_PG_URL.as_str(),
        default_value_if("local", "true", Some(LOCAL_PG_URL.as_str())),
        env = "API_ENDPOINT"
    )]
    api_endpoint: Url,
    /// Public (anon) API key to use during authentication to the Estuary API.
    #[arg(
        long,
        default_value = DEFAULT_PG_PUBLIC_TOKEN,
        default_value_if("local", "true", Some(LOCAL_PG_PUBLIC_TOKEN)),
        env = "API_KEY"
    )]
    api_key: String,
    /// Endpoint of the Estuary agent API to use.
    #[arg(
            long,
            default_value = DEFAULT_AGENT_URL.as_str(),
            default_value_if("local", "true", Some(LOCAL_AGENT_URL.as_str())),
            env = "AGENT_ENDPOINT"
        )]
    agent_endpoint: Url,

    /// When true, override the configured API endpoint and token,
    /// in preference of a local control plane.
    #[arg(long, action(clap::ArgAction::SetTrue))]
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

    /// List of Kafka broker URLs to try connecting to for group management APIs
    #[arg(long, env = "DEFAULT_BROKER_URLS", value_delimiter = ',')]
    default_broker_urls: Vec<String>,
    /// The AWS region that the default broker lives in
    #[arg(long, env = "DEFAULT_BROKER_MSK_REGION")]
    default_broker_msk_region: String,

    /// The secret used to encrypt/decrypt potentially sensitive strings when sending them
    /// to the upstream Kafka broker, e.g topic names in group management metadata.
    #[arg(long, env = "ENCRYPTION_SECRET")]
    encryption_secret: String,

    /// How long to wait for a message before closing an idle connection
    #[arg(long, env = "IDLE_SESSION_TIMEOUT", value_parser = humantime::parse_duration, default_value = "30s")]
    idle_session_timeout: std::time::Duration,

    /// How long to cache materialization specs and other task metadata for before refreshing
    #[arg(long, env = "TASK_REFRESH_INTERVAL", value_parser = humantime::parse_duration, default_value = "30s")]
    task_refresh_interval: std::time::Duration,

    /// How long before a request for materialization specs and other task metadata times out
    #[arg(long, env = "TASK_REQUEST_TIMEOUT", value_parser = humantime::parse_duration, default_value = "30s")]
    task_request_timeout: std::time::Duration,

    /// Timeout for TLS handshake completion
    #[arg(long, env = "TLS_HANDSHAKE_TIMEOUT", value_parser = humantime::parse_duration, default_value = "10s")]
    tls_handshake_timeout: std::time::Duration,

    /// The fully-qualified domain name of the data plane that Dekaf is running inside of
    #[arg(
        long,
        env = "DATA_PLANE_FQDN",
        default_value=DEFAULT_DATA_PLANE_FQDN,
        default_value_if("local", "true", Some(LOCAL_DATA_PLANE_FQDN)),
    )]
    data_plane_fqdn: String,
    /// An HMAC key recognized by the data plane that Dekaf is running inside of. Used to
    /// sign data-plane access token requests.
    #[arg(
        long,
        env = "DATA_PLANE_ACCESS_KEY",
        default_value_if("local", "true", Some(LOCAL_DATA_PLANE_HMAC)),
        // This is a work-around to clap_derive's somewhat buggy handling of `default_value_if`.
        // The end result is that `data_plane_access_key` is required iff `--local` is not specified.
        // If --local is specified, it will use the value in LOCAL_DATA_PLANE_HMAC unless overridden.
        // See https://github.com/clap-rs/clap/issues/4086 and https://github.com/clap-rs/clap/issues/4918
        required(false)
    )]
    data_plane_access_key: String,

    /// Maximum number of connections to allow at once
    #[arg(long, env = "MAX_CONNECTIONS", default_value = "300")]
    max_connections: usize,
    /// Maximum number of chunks to buffer for each pending read.
    /// A chunk represents a single ReadResponse from Gazette, and
    /// can be up to 130K in size.
    #[arg(long, env = "READ_BUFFER_CHUNK_LIMIT", default_value = "20")]
    read_buffer_chunk_limit: usize,

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


impl Cli {
    fn build_broker_urls(&self) -> anyhow::Result<Vec<String>> {
        self.default_broker_urls
            .clone()
            .into_iter()
            .map(|url| {
                {
                    let parsed = Url::parse(&url).expect("invalid broker URL {url}");
                    Ok::<_, anyhow::Error>(format!(
                        "tcp://{}:{}",
                        parsed.host().context(format!("invalid broker URL {url}"))?,
                        parsed.port().unwrap_or(9092)
                    ))
                }
                .context(url)
            })
            .collect::<anyhow::Result<Vec<_>>>()
    }
}

fn main() {
    logging::install();

    let cli = Cli::parse();

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .unwrap();

    tracing::info!("Starting dekaf");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build();

    let runtime = match runtime {
        Ok(runtime) => runtime,
        Err(error) => {
            tracing::error!(%error, "couldn't build Tokio runtime");
            std::process::exit(1);
        }
    };

    let handle = runtime.spawn(async_main(cli));
    let result = runtime.block_on(async { handle.await.unwrap() });

    // Explicitly shut down the runtime without waiting for blocking background tasks.
    // This prevents hangs from tasks that may be waiting on I/O that will never complete.
    runtime.shutdown_background();

    if let Err(error) = result {
        tracing::error!(error = format!("{error:#}"), "dekaf crashed with error");
        std::process::exit(1);
    }
}

async fn async_main(cli: Cli) -> anyhow::Result<()> {
    let upstream_kafka_urls = cli.build_broker_urls()?;


    test_kafka(&cli).await?;

    let (api_endpoint, api_key) = if cli.local {
        (LOCAL_PG_URL.to_owned(), LOCAL_PG_PUBLIC_TOKEN.to_string())
    } else {
        (cli.api_endpoint, cli.api_key)
    };

    let client_base = flow_client::Client::new(
        cli.agent_endpoint,
        api_key,
        api_endpoint,
        None,
        ::flow_client::DEFAULT_CONFIG_ENCRYPTION_URL.clone(),
    );
    let signing_token = jsonwebtoken::EncodingKey::from_base64_secret(&cli.data_plane_access_key)?;

    let task_manager = Arc::new(TaskManager::new(
        cli.task_refresh_interval,
        cli.task_request_timeout,
        client_base.clone(),
        cli.data_plane_fqdn.clone(),
        signing_token.clone(),
    ));

    let app = Arc::new(dekaf::App {
        advertise_host: cli.advertise_host.to_owned(),
        advertise_kafka_port: cli.kafka_port,
        secret: cli.encryption_secret.to_owned(),
        data_plane_signer: signing_token,
        data_plane_fqdn: cli.data_plane_fqdn,
        client_base,
        task_manager: task_manager.clone(),
    });

    let cancel_token = tokio_util::sync::CancellationToken::new();

    // Create a task to listen for Ctrl+C and cancel the global cancellation token
    let ctrl_c_token = cancel_token.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for CTRL-C");
        tracing::info!("Received Ctrl+C, initiating shutdown");
        ctrl_c_token.cancel();
    });

    let connection_limit = Arc::new(tokio::sync::Semaphore::new(cli.max_connections));

    let schema_addr = format!("[::]:{}", cli.schema_registry_port).parse()?;
    let metrics_addr = format!("[::]:{}", cli.metrics_port).parse()?;
    // Build a listener for Kafka sessions.
    let kafka_listener = tokio::net::TcpListener::bind(format!("[::]:{}", cli.kafka_port))
        .await
        .context("failed to bind server port")?;

    let metrics_router = dekaf::metrics_server::build_router();
    let metrics_server_task =
        axum_server::bind(metrics_addr).serve(metrics_router.into_make_service());
    tokio::spawn(async move { metrics_server_task.await.unwrap() });

    let schema_router = dekaf::registry::build_router(app.clone());

    let msk_region = cli.default_broker_msk_region.as_str();

    // Setup TLS acceptor if TLS configuration is provided
    let tls_acceptor = if let Some(tls_cfg) = cli.tls {
        let axum_rustls_config = RustlsConfig::from_pem_file(
            tls_cfg.certificate_file.clone().unwrap(),
            tls_cfg.certificate_key_file.clone().unwrap(),
        )
        .await
        .context("failed to open or read certificate or certificate key file")?;

        let schema_server_task = axum_server::bind_rustls(schema_addr, axum_rustls_config.clone())
            .serve(schema_router.into_make_service());
        tokio::spawn(async move { schema_server_task.await.unwrap() });

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

        Some(Arc::new(tokio_rustls::TlsAcceptor::from(Arc::new(config))))
    } else {
        tracing::info!("No TLS certificate provided, Dekaf will not terminate TLS");
        let schema_server_task =
            axum_server::bind(schema_addr).serve(schema_router.into_make_service());
        tokio::spawn(async move { schema_server_task.await.unwrap() });
        None
    };

    let mut backoff = None;
    let backoff_duration = std::time::Duration::from_millis(100);

    // Accept and serve Kafka sessions until we're signaled to stop.
    loop {
        if let Some(duration) = backoff {
            tokio::time::sleep(duration).await;
            backoff = None;
        }
        tokio::select! {
            accept = kafka_listener.accept() => {
                let (socket, addr) = match accept {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!("Failed to accept connection: {e:?}");
                        backoff = Some(backoff_duration);
                        continue;
                    }
                };

                if let Err(e) = socket.set_nodelay(true) {
                    tracing::error!("Failed to set nodelay: {e:?}");
                    continue;
                }

                // > Unlike a cloned CancellationToken, cancelling a child token does not cancel the parent token.
                // So every `task_cancellation` will get cancelled when `cancel_token` (ctrl-c) is cancelled, but only
                // a particular task's `task_cancellation` token will get cancelled if its `TaskForwarder` crashes.
                let task_cancellation = cancel_token.child_token();

                tokio::spawn(
                    logging::forward_logs(
                        GazetteWriter::new(
                            app.task_manager.clone(),
                        ),
                        task_cancellation.clone(),
                        serve(
                            Session::new(
                                app.clone(),
                                cli.encryption_secret.to_owned(),
                                upstream_kafka_urls.clone(),
                                msk_region.to_string(),
                                cli.read_buffer_chunk_limit,
                            ),
                            socket,
                            tls_acceptor.clone(),
                            addr,
                            cli.idle_session_timeout,
                            cli.tls_handshake_timeout,
                            task_cancellation,
                            connection_limit.clone()
                        )
                    )
                );
            }
            _ = cancel_token.cancelled() => break,
        }
    }

    Ok(())
}

/// Trait that combines AsyncRead and AsyncWrite for our connection type
trait Connection: AsyncRead + AsyncWrite + Send + Unpin {}

impl Connection for TcpStream {}
impl<S> Connection for tokio_rustls::server::TlsStream<S> where
    S: AsyncRead + AsyncWrite + Send + Unpin
{
}

#[tracing::instrument(level = "info", err(Debug, level = "warn"), skip(session, socket, tls_acceptor, stop, connection_limit), fields(?addr))]
async fn serve(
    mut session: Session,
    socket: TcpStream,
    tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
    addr: std::net::SocketAddr,
    idle_timeout: std::time::Duration,
    tls_handshake_timeout: std::time::Duration,
    stop: tokio_util::sync::CancellationToken,
    connection_limit: Arc<tokio::sync::Semaphore>,
) -> anyhow::Result<()> {
    let permit = match connection_limit.try_acquire() {
        Ok(permit) => permit,
        Err(_) => {
            metrics::counter!("dekaf_rejected_connections", "reason" => "over_limit").increment(1);
            anyhow::bail!("Connection limit reached, rejecting connection");
        }
    };

    tracing::info!("accepted client connection");

    // Optionally drive TLS handshake if needed
    let socket: Box<dyn Connection> = match tls_acceptor {
        Some(acceptor) => {
            let tls_stream = tokio::time::timeout(tls_handshake_timeout, acceptor.accept(socket))
                .await
                .context("TLS handshake timed out")?
                .context("TLS handshake failed")?;
            Box::new(tls_stream)
        }
        None => Box::new(socket),
    };

    let (r, mut w) = tokio::io::split(socket);
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

    metrics::gauge!("dekaf_total_connections").increment(1);

    let result = async {
        loop {
            tokio::select! {
                resp = r.try_next() => {
                    let Some(frame) = resp.context("failed to read next session request")? else {
                        return Ok(());
                    };

                    dekaf::dispatch_request_frame(&mut session, &mut raw_sasl_auth, frame, &mut out)
                    .await?;

                    () = w.write_all(&mut out).await?;
                    out.clear();
                }
                _ = tokio::time::sleep(idle_timeout) => {
                    anyhow::bail!("timeout waiting for next session request")
                }
                _ = stop.cancelled() => {
                    anyhow::bail!("signalled to stop")
                }
            }
        }
    }
    .await;

    metrics::gauge!("dekaf_total_connections").decrement(1);

    w.shutdown().await?;

    drop(permit);

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

#[tracing::instrument(skip(cli))]
async fn test_kafka(cli: &Cli) -> anyhow::Result<()> {
    let iam_creds = KafkaClientAuth::MSK {
        aws_region: cli.default_broker_msk_region.clone(),
        provider: aws_config::from_env()
            .region(aws_types::region::Region::new(
                cli.default_broker_msk_region.clone(),
            ))
            .load()
            .await
            .credentials_provider()
            .unwrap(),
        cached: None,
    };

    let broker_urls = cli.build_broker_urls()?;

    KafkaApiClient::connect(broker_urls.as_slice(), iam_creds).await?;


    tracing::info!("Successfully connected to upstream kafka");

    Ok(())
}
