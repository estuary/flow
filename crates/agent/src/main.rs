// Links in the allocator crate, which sets the global allocator to jemalloc
extern crate allocator;

use agent::publications::Publisher;
use anyhow::Context;
use clap::Parser;
use derivative::Derivative;
use futures::{FutureExt, TryFutureExt};
use rand::Rng;
use serde::Deserialize;

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
    /// URL of the data-plane Gazette broker.
    /// TODO(johnny): Deprecated and should be removed with federated data-planes.
    #[clap(
        long = "broker-address",
        env = "BROKER_ADDRESS",
        default_value = "http://localhost:8080"
    )]
    broker_address: url::Url,
    /// URL of the data-plane Flow consumer.
    /// TODO(johnny): Deprecated and should be removed with federated data-planes.
    #[clap(
        long = "consumer-address",
        env = "CONSUMER_ADDRESS",
        default_value = "http://localhost:9000"
    )]
    consumer_address: url::Url,
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
    #[clap(long, default_value = "8675", env = "API_PORT")]
    api_port: u16,
}

fn main() -> Result<(), anyhow::Error> {
    // Use reasonable defaults for printing structured logs to stderr.
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
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
    let bindir = std::fs::canonicalize(args.bindir)
        .context("canonicalize --bin-dir")?
        .into_os_string()
        .into_string()
        .expect("os path must be utf8");

    let mut pg_options = args
        .database_url
        .as_str()
        .parse::<sqlx::postgres::PgConnectOptions>()
        .context("parsing database URL")?
        .application_name("agent");

    // If a database CA was provided, require that we use TLS with full cert verification.
    if let Some(ca) = &args.database_ca {
        pg_options = pg_options
            .ssl_mode(sqlx::postgres::PgSslMode::VerifyFull)
            .ssl_root_cert(ca);
    } else {
        // Otherwise, prefer TLS but don't require it.
        pg_options = pg_options.ssl_mode(sqlx::postgres::PgSslMode::Prefer);
    }

    let pg_pool = sqlx::postgres::PgPool::connect_with(pg_options)
        .await
        .context("connecting to database")?;

    let system_user_id = agent_sql::get_user_id_for_email(&args.accounts_email, &pg_pool)
        .await
        .context("querying for agent user id")?;

    let builds_root = resolve_builds_root(&args.consumer_address)
        .await
        .context("resolving builds root")?;
    tracing::info!(%builds_root, "resolved builds root");

    // Start a logs sink into which agent loops may stream logs.
    let (logs_tx, logs_rx) = tokio::sync::mpsc::channel(8192);
    let logs_sink = agent::logs::serve_sink(pg_pool.clone(), logs_rx);

    // Generate a random shard ID to use for generating unique IDs.
    // Range starts at 1 because 0 is always used for ids generated in postgres.
    let id_gen_shard = rand::thread_rng().gen_range(1u16..1024u16);
    let id_gen = models::IdGenerator::new(id_gen_shard);
    let publisher = Publisher::new(
        args.allow_local,
        &bindir,
        &builds_root,
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
    let api_app = agent::api::App {
        pool: pg_pool.clone(),
    };
    let api_listener = std::net::TcpListener::bind(format!("[::]:{}", args.api_port))
        .context("failed to bind server port")?;
    let api_server = axum::Server::from_tcp(api_listener)
        .unwrap()
        .serve(agent::api::build_router(api_app.into()).into_make_service())
        .with_graceful_shutdown(shutdown.clone());

    // Wire up the agent's job execution loop.
    let serve_fut = agent::serve(
        vec![
            Box::new(publisher),
            Box::new(agent::TagHandler::new(
                &args.connector_network,
                &logs_tx,
                args.allow_local,
            )),
            Box::new(agent::DiscoverHandler::new(
                &args.connector_network,
                &bindir,
                &logs_tx,
                args.allow_local,
            )),
            Box::new(agent::DirectiveHandler::new(args.accounts_email, &logs_tx)),
            Box::new(agent::EvolutionHandler),
            Box::new(agent::controllers::ControllerHandler::new(control_plane)),
        ],
        pg_pool.clone(),
        shutdown,
    );

    std::mem::drop(logs_tx);
    let ((), (), ()) = tokio::try_join!(
        serve_fut,
        api_server.map_err(|err| anyhow::anyhow!(err)),
        logs_sink.map_err(Into::into)
    )?;

    Ok(())
}

async fn resolve_builds_root(consumer: &url::Url) -> anyhow::Result<url::Url> {
    #[derive(Deserialize)]
    struct Response {
        cmdline: Vec<String>,
    }
    let Response { cmdline } = reqwest::get(consumer.join("/debug/vars")?)
        .await?
        .error_for_status()?
        .json()
        .await?;

    tracing::debug!(?cmdline, "fetched Flow consumer cmdline");

    for window in cmdline.windows(2) {
        if window[0] == "--flow.builds-root" {
            return Ok(url::Url::parse(&window[1]).context("parsing builds-root")?);
        }
    }
    anyhow::bail!("didn't find --flow.builds-root flag")
}
