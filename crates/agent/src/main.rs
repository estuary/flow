// Links in the allocator crate, which sets the global allocator to jemalloc
extern crate allocator;

use agent::publications::Publisher;
use anyhow::Context;
use clap::Parser;
use derivative::Derivative;
use futures::FutureExt;
use rand::Rng;
use sqlx::{ConnectOptions, Connection};

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
}

fn main() -> Result<(), anyhow::Error> {
    // Required in order for libraries to use `rustls` for TLS.
    // See: https://docs.rs/rustls/latest/rustls/crypto/struct.CryptoProvider.html
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    // Use reasonable defaults for printing structured logs to stderr.
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(if matches!(std::env::var("NO_COLOR"), Ok(v) if v == "1") {
            false
        } else {
            true
        })
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting tracing default failed");

    let args = Args::parse();
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
        .application_name(&application_name);
    pg_options.log_slow_statements(log::LevelFilter::Warn, std::time::Duration::from_secs(10));

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

    let system_user_id = agent_sql::get_user_id_for_email(&args.accounts_email, &pg_pool)
        .await
        .context("querying for agent user id")?;
    let jwt_secret: String = sqlx::query_scalar(r#"show app.settings.jwt_secret;"#)
        .fetch_one(&pg_pool)
        .await?;

    if args.builds_root.scheme() == "file" {
        std::fs::create_dir_all(args.builds_root.path())
            .context("failed to create builds-root directory")?;
    }

    // Start a logs sink into which agent loops may stream logs.
    let (logs_tx, logs_rx) = tokio::sync::mpsc::channel(8192);
    let logs_sink = agent::logs::serve_sink(pg_pool.clone(), logs_rx);
    let logs_sink = async move { anyhow::Result::Ok(logs_sink.await?) };

    // Generate a random shard ID to use for generating unique IDs.
    // Range starts at 1 because 0 is always used for ids generated in postgres.
    let id_gen_shard = rand::thread_rng().gen_range(1u16..1024u16);
    let id_gen = models::IdGenerator::new(id_gen_shard);
    let publisher = Publisher::new(
        &bindir,
        &args.builds_root,
        &args.connector_network,
        &logs_tx,
        pg_pool.clone(),
        id_gen.clone(),
    );
    let control_plane = agent::PGControlPlane::new(
        pg_pool.clone(),
        system_user_id,
        publisher.clone(),
        id_gen.clone(),
    );

    // Share-able future which completes when the agent should exit.
    let shutdown = tokio::signal::ctrl_c().map(|_| ()).shared();

    // Wire up the agent's API server.
    let api_router = agent::api::build_router(
        id_gen.clone(),
        jwt_secret.into_bytes(),
        pg_pool.clone(),
        publisher.clone(),
        &args.allow_origin,
    )?;
    let api_server = axum::serve(api_listener, api_router).with_graceful_shutdown(shutdown.clone());
    let api_server = async move { anyhow::Result::Ok(api_server.await?) };

    // Wire up the agent's job execution loop.
    let serve_fut = if args.serve_handlers {
        agent::serve(
            vec![
                Box::new(publisher),
                Box::new(agent::TagHandler::new(
                    &args.connector_network,
                    &logs_tx,
                    args.allow_local,
                )),
                Box::new(agent::DiscoverHandler::new(&logs_tx)),
                Box::new(agent::DirectiveHandler::new(args.accounts_email, &logs_tx)),
                Box::new(agent::EvolutionHandler),
                Box::new(agent::controllers::ControllerHandler::new(control_plane)),
            ],
            pg_pool.clone(),
            shutdown,
        )
        .boxed()
    } else {
        futures::future::ready(Ok(())).boxed()
    };

    std::mem::drop(logs_tx);
    let ((), (), ()) = tokio::try_join!(serve_fut, api_server, logs_sink)?;

    Ok(())
}
