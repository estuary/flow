// Links in the allocator crate, which sets the global allocator to jemalloc
extern crate allocator;

use anyhow::Context;
use clap::Parser;
use control_plane_api::{
    discovers::DiscoverHandler, proxy_connectors::DataPlaneConnectors, publications::Publisher,
};
use derivative::Derivative;
use futures::FutureExt;
use rand::Rng;
use sqlx::{ConnectOptions, Connection};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Agent is a daemon which runs server-side tasks of the Flow control-plane.
#[derive(Derivative, Parser)]
#[derivative(Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// URL of the postgres database.
    #[derivative(Debug = "ignore")]
    #[clap(
        long = "database",
        env = "DATABASE_URL",
        default_value = "postgres://flow:flow@127.0.0.1:5432/control_development"
    )]
    database_url: url::Url,
    /// Path to CA certificate of the database.
    #[clap(long = "database-ca", env = "DATABASE_CA")]
    database_ca: Option<String>,
    /// URL endpoint into which build database are placed.
    #[clap(long = "builds-root", env = "BUILDS_ROOT")]
    builds_root: url::Url,
    /// Docker network for connector invocations.
    #[clap(long = "connector-network", default_value = "bridge")]
    connector_network: String,
    /// Path to binaries like `flowctl`.
    #[clap(long = "bin-dir", env = "BIN_DIR")]
    bindir: String,
    /// Email address of user which provisions and maintains tenant accounts.
    #[clap(long = "accounts-email", default_value = "support@estuary.dev")]
    accounts_email: String,
    /// Allow local connectors. True for local stacks, and false otherwise.
    #[clap(long = "allow-local")]
    allow_local: bool,
    /// The port to listen on for API requests.
    #[clap(long, default_value = "8080", env = "API_PORT")]
    api_port: u16,
    /// Whether to serve job handlers within this agent instance.
    #[clap(long = "serve-handlers", env = "SERVE_HANDLERS")]
    serve_handlers: bool,
    /// Origin to allow in CORS contexts. May be specified multiple times.
    #[clap(long = "allow-origin")]
    allow_origin: Vec<String>,
    /// Maximum number of concurrent controller automations to run.
    /// The default value of 0 disables running controller automations.
    #[clap(
        long = "controller-max-jobs",
        env = "CONTROLLER_MAX_JOBS",
        default_value = "0"
    )]
    max_automations: u32,
    /// Interval between polls for dequeue-able tasks when otherwise idle.
    #[clap(
        long = "dequeue-interval",
        env = "CONTROLLER_DEQUEUE_INTERVAL",
        default_value = "5s"
    )]
    #[arg(value_parser = humantime::parse_duration)]
    dequeue_interval: std::time::Duration,
    /// Interval before a running task poll is presumed to have failed.
    /// Tasks updated their heartbeats every half of this interval.
    #[clap(
        long = "heartbeat-timeout",
        env = "CONTROLLER_HEARTBEAT_TIMEOUT",
        default_value = "300s"
    )]
    #[arg(value_parser = humantime::parse_duration)]
    heartbeat_timeout: std::time::Duration,

    #[clap(long = "log-format", env = "LOG_FORMAT", default_value = "json")]
    log_format: LogFormat,

    /// Probability of running an auto-discover when one is due, expressed as a
    /// decimal value between 0 and 1. 1 means a 100% chance of running an
    /// auto-discover when one is due, and 0 disables auto-discovers completely.
    /// This is intended to allow globally throttling down our overall rate of
    /// auto-discover tasks, or to disable them entirely.
    #[clap(
        long = "auto-discover-probability",
        env = "AUTO_DISCOVER_PROBABILITY",
        default_value = "1.0"
    )]
    auto_discover_probability: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

fn main() -> Result<(), anyhow::Error> {
    // Required in order for libraries to use `rustls` for TLS.
    // See: https://docs.rs/rustls/latest/rustls/crypto/struct.CryptoProvider.html
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    let args = Args::parse();

    // Use reasonable defaults for printing structured logs to stderr.
    let builder = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env());
    if args.log_format == LogFormat::Json {
        tracing::subscriber::set_global_default(builder.json().finish())
            .expect("setting tracing default failed");
    } else {
        let no_color = matches!(std::env::var("NO_COLOR"), Ok(v) if v == "1");
        tracing::subscriber::set_global_default(builder.with_ansi(!no_color).finish())
            .expect("setting tracing default failed");
    };
    tracing::info!(?args, "started!");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let task = runtime.spawn(async move { async_main(args).await });
    let result = runtime.block_on(task);

    tracing::info!(?result, "main function completed, shutting down runtime");
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    result?
}

async fn async_main(args: Args) -> Result<(), anyhow::Error> {
    // Bind early in the application lifecycle, to not fail requests which may dispatch
    // as soon as the process is up (for example, Tilt on local stacks).
    let api_listener = tokio::net::TcpListener::bind(format!("[::]:{}", args.api_port))
        .await
        .context("failed to bind server port")?;

    let bindir = std::fs::canonicalize(args.bindir)
        .context("canonicalize --bin-dir")?
        .into_os_string()
        .into_string()
        .expect("os path must be utf8");

    // The HOSTNAME variable will be set to the name of the pod in k8s
    let application_name = std::env::var("HOSTNAME").unwrap_or_else(|_| "agent".to_string());
    let mut pg_options = args
        .database_url
        .as_str()
        .parse::<sqlx::postgres::PgConnectOptions>()
        .context("parsing database URL")?
        .application_name(&application_name)
        .log_slow_statements(
            tracing::log::LevelFilter::Warn,
            std::time::Duration::from_secs(10),
        );

    // If a database CA was provided, require that we use TLS with full cert verification.
    if let Some(ca) = &args.database_ca {
        pg_options = pg_options
            .ssl_mode(sqlx::postgres::PgSslMode::VerifyFull)
            .ssl_root_cert(ca);
    } else {
        // Otherwise, prefer TLS but don't require it.
        pg_options = pg_options.ssl_mode(sqlx::postgres::PgSslMode::Prefer);
    }

    let pg_pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_secs(5))
            .after_release(|conn, meta| {
                let fut = async move {
                    let r =tokio::time::timeout(std::time::Duration::from_secs(5), async {
                                        conn.ping()
                                    });
                    if let Err(err) = r.await {
                        tracing::warn!(error = ?err, conn_meta = ?meta, "connection was put back in a bad state, removing from the pool");
                        Ok(false)
                    } else {
                        Ok(true) // connection is good
                    }
                };
                fut.boxed()
            })
            .connect_with(pg_options)
        .await
        .context("connecting to database")?;

    // Periodically log information about the connection pool to aid in debugging.
    let pool_copy = pg_pool.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(120));
        loop {
            interval.tick().await;
            let total_connections = pool_copy.size();
            let idle_connections = pool_copy.num_idle();
            tracing::info!(
                total_connections,
                idle_connections,
                "db connection pool stats"
            );
        }
    });

    let system_user_id = control_plane_api::get_user_id_for_email(&args.accounts_email, &pg_pool)
        .await
        .context("querying for agent user id")?;
    let jwt_secret: String =
        std::env::var("CONTROL_PLANE_JWT_SECRET").context("missing CONTROL_PLANE_JWT_SECRET")?;

    if args.builds_root.scheme() == "file" {
        std::fs::create_dir_all(args.builds_root.path())
            .context("failed to create builds-root directory")?;
    }

    // Start a logs sink into which agent loops may stream logs.
    let (logs_tx, logs_rx) = tokio::sync::mpsc::channel(8192);
    let logs_sink = control_plane_api::logs::serve_sink(pg_pool.clone(), logs_rx);
    let logs_sink = async move { anyhow::Result::Ok(logs_sink.await?) };
    let connectors = DataPlaneConnectors::new(logs_tx.clone());
    let discover_handler = DiscoverHandler::new(connectors.clone());

    // Generate a random shard ID to use for generating unique IDs.
    // Range starts at 1 because 0 is always used for ids generated in postgres.
    let id_gen_shard = rand::rng().random_range(1u16..1024u16);
    let id_gen = models::IdGenerator::new(id_gen_shard);
    let publisher = Publisher::new(
        &bindir,
        &args.builds_root,
        &args.connector_network,
        &logs_tx,
        pg_pool.clone(),
        id_gen.clone(),
        connectors,
    );

    let decrypted_hmac_keys = Arc::new(RwLock::new(HashMap::new()));

    tokio::spawn(refresh_decrypted_hmac_keys(
        pg_pool.clone(),
        decrypted_hmac_keys.clone(),
    ));

    let control_plane = agent::PGControlPlane::new(
        pg_pool.clone(),
        system_user_id,
        publisher.clone(),
        id_gen.clone(),
        discover_handler.clone(),
        logs_tx.clone(),
        decrypted_hmac_keys,
        args.auto_discover_probability,
    );
    let connector_tags_executor =
        agent::TagExecutor::new(&args.connector_network, &logs_tx, args.allow_local);

    // Share-able future which completes when the agent should exit.
    let shutdown = tokio::signal::ctrl_c().map(|_| ()).shared();

    // Wire up the agent's API server.
    let api_router = control_plane_api::build_router(
        id_gen.clone(),
        jwt_secret.into_bytes(),
        pg_pool.clone(),
        publisher.clone(),
        &args.allow_origin,
    )?;
    let api_server = axum::serve(api_listener, api_router).with_graceful_shutdown(shutdown.clone());
    let api_server = async move { anyhow::Result::Ok(api_server.await?) };
    let directive_executor = agent::DirectiveHandler::new(args.accounts_email, &logs_tx);

    let automations_fut = if args.max_automations > 0 {
        automations::Server::new()
            .register(agent::controllers::LiveSpecControllerExecutor::new(
                control_plane,
            ))
            .register(agent::publications::PublicationsExecutor {
                publisher,
                pg_pool: pg_pool.clone(),
            })
            .register(agent::DiscoverExecutor {
                handler: discover_handler,
            })
            .register(directive_executor)
            .register(connector_tags_executor)
            .register(migrate::automation::MigrationExecutor)
            .serve(
                args.max_automations,
                pg_pool.clone(),
                args.dequeue_interval,
                args.heartbeat_timeout,
                shutdown.clone(),
            )
            .map(|()| anyhow::Result::<()>::Ok(()))
            .boxed()
    } else {
        futures::future::ready(Ok(())).boxed()
    };

    std::mem::drop(logs_tx);
    let ((), (), ()) = tokio::try_join!(api_server, logs_sink, automations_fut)?;

    Ok(())
}

async fn refresh_decrypted_hmac_keys(
    pg_pool: sqlx::PgPool,
    decrypted_hmac_keys: Arc<RwLock<HashMap<String, Vec<String>>>>,
) -> anyhow::Result<()> {
    const REFRESH_INTERVAL: chrono::TimeDelta = chrono::TimeDelta::seconds(60);

    loop {
        let mut data_planes: Vec<_> =
            control_plane_api::data_plane::fetch_all_data_planes(&pg_pool)
                .await?
                .into_iter()
                .filter(|dp| {
                    !decrypted_hmac_keys
                        .read()
                        .unwrap()
                        .contains_key(&dp.data_plane_name)
                })
                .filter(|dp| {
                    !dp.encrypted_hmac_keys
                        .to_value()
                        .as_object()
                        .unwrap()
                        .is_empty()
                })
                .collect();

        futures::future::try_join_all(
            data_planes
                .iter_mut()
                .map(|dp| agent::decrypt_hmac_keys(dp)),
        )
        .await?;

        {
            let mut writable = decrypted_hmac_keys.write().unwrap();

            data_planes.iter().for_each(|dp| {
                writable.insert(dp.data_plane_name.clone(), dp.hmac_keys.clone());
            });
        }

        tokio::time::sleep(REFRESH_INTERVAL.to_std().unwrap()).await;
    }
}
