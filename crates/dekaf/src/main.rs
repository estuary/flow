// Links in the allocator crate, which sets the global allocator to jemalloc
extern crate allocator;

use anyhow::{bail, Context};
use axum_server::tls_rustls::RustlsConfig;
use clap::{Args, Parser};
use dekaf::{log_appender::GazetteLogWriter, logging, Session};
use flow_client::{
    DEFAULT_AGENT_URL, DEFAULT_DATA_PLANE_FQDN, DEFAULT_PG_PUBLIC_TOKEN, DEFAULT_PG_URL,
    LOCAL_AGENT_URL, LOCAL_DATA_PLANE_FQDN, LOCAL_DATA_PLANE_HMAC, LOCAL_PG_PUBLIC_TOKEN,
    LOCAL_PG_URL,
};
use futures::{FutureExt, TryStreamExt};
use rustls::pki_types::CertificateDer;
use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
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
    #[arg(long, env = "DEFAULT_BROKER_URLS")]
    default_broker_urls: Vec<String>,
    /// The username for the default Kafka broker to use for serving group management APIs.
    /// Currently only supports SASL PLAIN username/password auth.
    #[arg(long, env = "DEFAULT_BROKER_USERNAME")]
    default_broker_username: String,
    /// The password for the default Kafka broker to use for serving group management APIs
    #[arg(long, env = "DEFAULT_BROKER_PASSWORD")]
    default_broker_password: String,

    // ------ This can be cleaned up once everyone is migrated off of the legacy connection mode ------
    /// Brokers to use for connections using the legacy refresh-token based connection mode
    #[arg(long, env = "LEGACY_MODE_BROKER_URLS")]
    legacy_mode_broker_urls: Vec<String>,
    /// The username for the Kafka broker to use for serving group management APIs for connections
    /// using the legacy refresh-token based connection mode
    #[arg(long, env = "LEGACY_MODE_BROKER_USERNAME")]
    legacy_mode_broker_username: String,
    /// The password for the Kafka broker to use for serving group management API for connections
    /// using the legacy refresh-token based connection modes
    #[arg(long, env = "LEGACY_MODE_BROKER_PASSWORD")]
    legacy_mode_broker_password: String,
    // ------------------------------------------------------------------------------------------------
    /// The secret used to encrypt/decrypt potentially sensitive strings when sending them
    /// to the upstream Kafka broker, e.g topic names in group management metadata.
    #[arg(long, env = "ENCRYPTION_SECRET")]
    encryption_secret: String,

    /// How long to wait for a message before closing an idle connection
    #[arg(long, env = "IDLE_SESSION_TIMEOUT", value_parser = humantime::parse_duration, default_value = "30s")]
    idle_session_timeout: std::time::Duration,

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
    let cli = Cli::parse();
    let (api_endpoint, api_key) = if cli.local {
        (LOCAL_PG_URL.to_owned(), LOCAL_PG_PUBLIC_TOKEN.to_string())
    } else {
        (cli.api_endpoint, cli.api_key)
    };

    let app = Arc::new(dekaf::App {
        advertise_host: cli.advertise_host.to_owned(),
        advertise_kafka_port: cli.kafka_port,
        secret: cli.encryption_secret.to_owned(),
        data_plane_signer: jsonwebtoken::EncodingKey::from_base64_secret(
            &cli.data_plane_access_key,
        )?,
        data_plane_fqdn: cli.data_plane_fqdn,
        client_base: flow_client::Client::new(cli.agent_endpoint, api_key, api_endpoint, None),
    });

    logging::install();

    tracing::info!("Starting dekaf");

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .unwrap();

    let upstream_kafka_urls = cli
        .default_broker_urls
        .clone()
        .into_iter()
        .map(|url| {
            {
                let parsed = Url::parse(&url).expect("invalid broker URL");
                Ok::<_, anyhow::Error>(format!(
                    "tcp://{}:{}",
                    parsed.host().context("invalid broker URL")?,
                    parsed.port().unwrap_or(9092)
                ))
            }
            .context(url)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    // ------ This can be cleaned up once everyone is migrated off of the legacy connection mode ------
    let legacy_mode_kafka_urls = cli
        .default_broker_urls
        .into_iter()
        .map(|url| {
            {
                let parsed = Url::parse(&url).expect("invalid broker URL");
                Ok::<_, anyhow::Error>(format!(
                    "tcp://{}:{}",
                    parsed.host().context("invalid broker URL")?,
                    parsed.port().unwrap_or(9092)
                ))
            }
            .context(url)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    // ------------------------------------------------------------------------------------------------

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

    let metrics_router = dekaf::metrics_server::build_router();
    let metrics_server_task =
        axum_server::bind(metrics_addr).serve(metrics_router.into_make_service());
    tokio::spawn(async move { metrics_server_task.await.unwrap() });

    let schema_router = dekaf::registry::build_router(app.clone());

    let broker_username = cli.default_broker_username.as_str();
    let broker_password = cli.default_broker_password.as_str();
    let legacy_broker_username = cli.legacy_mode_broker_username.as_str();
    let legacy_broker_password = cli.legacy_mode_broker_password.as_str();
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

                    tokio::spawn(
                        logging::forward_logs(
                            GazetteLogWriter::new(app.clone()),
                            serve(
                                Session::new(
                                    app.clone(),
                                    cli.encryption_secret.to_owned(),
                                    upstream_kafka_urls.clone(),
                                    broker_username.to_string(),
                                    broker_password.to_string(),
                                    legacy_mode_kafka_urls.clone(),
                                    legacy_broker_username.to_string(),
                                    legacy_broker_password.to_string()
                                ),
                                socket,
                                addr,
                                cli.idle_session_timeout,
                                stop.clone()
                            )
                        )
                    );
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

                    tokio::spawn(
                        logging::forward_logs(
                            GazetteLogWriter::new(app.clone()),
                            serve(
                                Session::new(
                                    app.clone(),
                                    cli.encryption_secret.to_owned(),
                                    upstream_kafka_urls.clone(),
                                    broker_username.to_string(),
                                    broker_password.to_string(),
                                    legacy_mode_kafka_urls.clone(),
                                    legacy_broker_username.to_string(),
                                    legacy_broker_password.to_string()
                                ),
                                socket,
                                addr,
                                cli.idle_session_timeout,
                                stop.clone()
                            )
                        )
                    );
                }
                _ = &mut stop => break,
            }
        }
    };

    Ok(())
}

#[tracing::instrument(level = "info", err(Debug, level = "warn"), skip(session, socket, _stop), fields(?addr))]
async fn serve<S>(
    mut session: Session,
    socket: S,
    addr: std::net::SocketAddr,
    idle_timeout: std::time::Duration,
    _stop: impl futures::Future<Output = ()>, // TODO(johnny): stop.
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    tracing::info!("accepted client connection");

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
            let Some(frame) = tokio::time::timeout(idle_timeout, r.try_next())
                .await
                .context("timeout waiting for next session request")?
                .context("failed to read next session request")?
            else {
                return Ok(());
            };

            dekaf::dispatch_request_frame(&mut session, &mut raw_sasl_auth, frame, &mut out)
                .await?;

            () = w.write_all(&mut out).await?;
            out.clear();
        }
    }
    .await;

    metrics::gauge!("dekaf_total_connections").decrement(1);

    w.shutdown().await?;
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
