// Links in the allocator crate, which sets the global allocator to jemalloc
extern crate allocator;

use anyhow::Context;
use axum::http;
use clap::Parser;
use control_plane_api::{
    App, discovers::DiscoverHandler, proxy_connectors::DataPlaneConnectors, publications::Publisher,
};
use derivative::Derivative;
use futures::FutureExt;
use sqlx::{ConnectOptions, Connection};
use std::sync::Arc;

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
    /// Email address of user which provisions and maintains tenant accounts.
    #[clap(long = "accounts-email", default_value = "support@estuary.dev")]
    accounts_email: String,
    /// The port to listen on for API requests.
    #[clap(long, default_value = "8080", env = "API_PORT")]
    api_port: u16,
    /// Whether to serve job handlers within this agent instance.
    #[clap(long = "serve-handlers", env = "SERVE_HANDLERS")]
    serve_handlers: bool,
    /// Skip checking that connector images exist in the connectors table during publication.
    // TODO(johnny): This flag is temporary, while we release data-plane enforcement
    // of connector restrictions. Once released, we should remove this flag and remove
    // all enforcement within the agent.
    #[clap(
        long = "skip-connector-table-check",
        default_value = "false",
        env = "SKIP_CONNECTOR_TABLE_CHECK"
    )]
    skip_connector_table_check: bool,
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

    /// Minimum duration between two controller-initiated publications of
    /// the same live spec. This is used to maintain overall system performance
    /// in the face of bursty demands that get placed on our automation.
    #[clap(
        long = "controller-publication-cooldown",
        env = "CONTROLLER_PUBLICATION_COOLDOWN",
        default_value = "0s"
    )]
    #[arg(value_parser = humantime::parse_duration)]
    controller_publication_cooldown: std::time::Duration,

    /// Whether to serve the alert notifcations handler. Set to false (default)
    /// to disable the automations job that sends alert notifications. Note that
    /// there's a separate way to disable sending emails, by setting
    /// `RESEND_API_KEY=''`. Disabling via this flag will prevent the alert
    /// notifications handlers from ever running, giving us a means of pausing
    /// the sending of alert notifications and resuming later.
    #[clap(long, env, default_value = "false")]
    serve_alert_notifications: bool,

    /// Optional api key for sending alert notification emails via resend. If
    /// not provided, then sending alert emails will be disabled, and any alert
    /// emails that would be sent will instead be logged as warnings.
    #[clap(
        long,
        env,
        requires = "email_from_address",
        requires = "email_reply_to_address"
    )]
    resend_api_key: Option<String>,

    /// Sender address for any emails that we send
    #[clap(long, env)]
    email_from_address: Option<String>,

    /// Reply-to address for any emails that we send
    #[clap(long, env)]
    email_reply_to_address: Option<String>,

    /// The URL of the dashboard UI, which is used when rendering links
    #[clap(long, env, default_value = "https://dashboard.estuary.dev/")]
    dashboard_base_url: String,

    /// How frequently to evaluate tenant-related alert conditions.
    #[clap(long, env, default_value = "1h")]
    #[arg(value_parser = humantime::parse_duration)]
    tenant_alert_interval: std::time::Duration,

    /// How frequently to evaluate `data_movement_stalled` alert conditions
    #[clap(long, env, default_value = "10m")]
    #[arg(value_parser = humantime::parse_duration)]
    data_movement_alert_interval: std::time::Duration,
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

    let flowctl_go = locate_bin::locate("flowctl-go")?;

    // The HOSTNAME variable will be set to the name of the pod in k8s
    let application_name = std::env::var("HOSTNAME").unwrap_or_else(|_| "agent".to_string());
    let pg_options = args
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
    let pg_options = if let Some(ca) = &args.database_ca {
        pg_options
            .ssl_mode(sqlx::postgres::PgSslMode::VerifyFull)
            .ssl_root_cert(ca)
    } else {
        // Otherwise, prefer TLS but don't require it.
        pg_options.ssl_mode(sqlx::postgres::PgSslMode::Prefer)
    };

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

    let builder = control_plane_api::publications::builds::new_builder(connectors);
    let mut publisher = Publisher::new(
        flowctl_go,
        &args.builds_root,
        &args.connector_network,
        &logs_tx,
        pg_pool.clone(),
        agent::id_generator::with_random_shard(),
        builder,
    );
    if args.skip_connector_table_check {
        publisher = publisher.with_skip_connector_table_check();
    }

    // Share-able future which completes when the agent should exit.
    let shutdown = tokio::signal::ctrl_c().map(|_| ()).shared();

    // Create the snapshot source and start the refresh loop.
    let snapshot_source = control_plane_api::snapshot::PgSnapshotSource::new(pg_pool.clone());
    let snapshot_watch = tokens::watch(snapshot_source).ready_owned().await;

    let controller_publication_cooldown =
        chrono::Duration::from_std(args.controller_publication_cooldown)?;
    let control_plane = agent::PGControlPlane::new(
        pg_pool.clone(),
        system_user_id,
        publisher.clone(),
        discover_handler.clone(),
        logs_tx.clone(),
        snapshot_watch.clone(),
        args.auto_discover_probability,
        controller_publication_cooldown,
    );

    // Wire up the agent's API Application and server.
    let api_app = Arc::new(App::new(
        agent::id_generator::with_random_shard(),
        jwt_secret.as_bytes(),
        pg_pool.clone(),
        publisher.clone(),
        snapshot_watch,
    ));
    let api_router = control_plane_api::build_router(api_app.clone(), &args.allow_origin)?;
    let api_server = axum::serve(api_listener, api_router).with_graceful_shutdown(shutdown.clone());
    let api_server = async move { anyhow::Result::Ok(api_server.await?) };

    let automations_fut = if args.max_automations > 0 {
        let directive_executor = agent::DirectiveHandler::new(args.accounts_email, &logs_tx);
        let connector_tags_executor = agent::TagExecutor::new(&args.connector_network, &logs_tx);
        let mut automations_server = automations::Server::new()
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
            .register(agent::alerts::new_tenant_alerts_executor(
                args.tenant_alert_interval,
            ))
            .register(agent::alerts::new_data_movement_alerts_executor(
                args.data_movement_alert_interval,
            ));

        if args.serve_alert_notifications {
            let sender = if let Some(api_key) = &args.resend_api_key {
                // These two are required if api-key is provided, so clap should have ensured they are present
                let from_email = args
                    .email_from_address
                    .clone()
                    .expect("missing email-from-address");
                let reply_to_email = args
                    .email_reply_to_address
                    .clone()
                    .expect("missing email-reply-to-address");
                tracing::info!(%from_email, %reply_to_email, "Sending of alert emails is enabled");
                agent::alerts::Sender::resend(
                    api_key,
                    from_email,
                    reply_to_email,
                    new_http_client()?,
                )
            } else {
                // Hopefully this is a local env
                tracing::warn!("Sending of alert emails is disabled");
                agent::alerts::Sender::Disabled
            };
            let alert_notifications =
                agent::alerts::AlertNotifications::new(&args.dashboard_base_url, sender)?;
            automations_server = automations_server.register(alert_notifications);
        }

        automations_server
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

/// Creates a new http client. We probably ought to use this same client in all/most
/// places in the app, but for now it's only use is just for sending alert notifications.
fn new_http_client() -> anyhow::Result<reqwest::Client> {
    let mut map = http::HeaderMap::new();
    let version = env!("CARGO_PKG_VERSION");
    let user_agent = format!("estuary-control-plane-agent:{version}");
    map.append(
        http::header::USER_AGENT,
        http::HeaderValue::from_str(&user_agent)?,
    );

    let c = reqwest::Client::builder().default_headers(map).build()?;
    Ok(c)
}
