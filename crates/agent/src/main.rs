use clap::Parser;
use futures::{FutureExt, TryFutureExt};
use tracing::info;

/// Agent is a daemon which runs server-side tasks of the Flow control-plane.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// URL of the postgres database.
    #[clap(
        short,
        long,
        env = "DATABASE_URL",
        default_value = "postgres://flow:flow@127.0.0.1:5432/control_development"
    )]
    database: url::Url,
    /// URL of the builds root.
    #[clap(short, long, env = "BUILDS_URL", default_value = "file:///var/tmp/")]
    builds_root: url::Url,
    /// Docker network for connector invocations.
    #[clap(short, long, default_value = "host")]
    connector_network: String,
    /// Path to the `flowctl` binary.
    #[clap(short, long, default_value = "flowctl")]
    flowctl: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    info!(?args, "started!");

    // Use reasonable defaults for printing structured logs to stderr.
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting tracing default failed");

    // Start a logs sink into which agent loops may stream logs.
    let (logs_tx, logs_rx) = tokio::sync::mpsc::channel(8192);
    let logs_sink = tokio::spawn(agent::logs::serve_sink(
        agent::build_pg_client(&args.database).await?,
        logs_rx,
    ));

    {
        // Build a BuildHandler.
        let pg_conn = agent::build_pg_client(&args.database).await?;
        let draft_handler = agent::DraftHandler::new(
            &args.connector_network,
            &args.flowctl,
            &logs_tx,
            &args.builds_root,
        );
        let draft_handler =
            agent::todo_serve(draft_handler, pg_conn, tokio::signal::ctrl_c().map(|_| ()))
                .map_err(|e| anyhow::anyhow!(e));

        // Build a TagHandler.
        let pg_conn = agent::build_pg_client(&args.database).await?;
        let tag_handler = agent::TagHandler::new(&args.connector_network, &args.flowctl, &logs_tx);
        let tag_handler =
            agent::todo_serve(tag_handler, pg_conn, tokio::signal::ctrl_c().map(|_| ()))
                .map_err(|e| anyhow::anyhow!(e));

        // Build a DiscoverHandler.
        let pg_conn = agent::build_pg_client(&args.database).await?;
        let discover_handler =
            agent::DiscoverHandler::new(&args.connector_network, &args.flowctl, &logs_tx);
        let discover_handler = agent::todo_serve(
            discover_handler,
            pg_conn,
            tokio::signal::ctrl_c().map(|_| ()),
        )
        .map_err(|e| anyhow::anyhow!(e));

        let _ = futures::try_join!(discover_handler, draft_handler, tag_handler)?;
    }

    std::mem::drop(logs_tx);
    logs_sink.await??;

    Ok(())
}
