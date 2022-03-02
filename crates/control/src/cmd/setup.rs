//! Setup a Control Plane and its accompanying database.
use sqlx::migrate::MigrateDatabase;
use sqlx::{Connection, PgConnection, Postgres};

use crate::cmd::{async_runtime, ConfigArgs};
use crate::config::{self};

#[derive(clap::Args, Debug)]
pub struct Args {
    #[clap(flatten)]
    config: ConfigArgs,
}

// TODO: Add a subcommand for *generating* a valid config file based on a provided database url.
pub fn run(args: Args) -> anyhow::Result<()> {
    let settings = config::load_settings(args.config.config_path)?;
    let runtime = async_runtime()?;

    runtime.block_on(async move { setup_database(&settings.database.url()).await })
}

async fn setup_database(url: &str) -> anyhow::Result<()> {
    // Create a new database if necessary.
    if !Postgres::database_exists(url).await? {
        Postgres::create_database(url).await?;
    }

    let mut conn = PgConnection::connect(url).await?;

    // Uses sqlx's `migrate` macro to embed the migration source files in the
    // binary. This allows us to migrate *any* database we are given a connection
    // to without needing to manually specify a migration source directory at
    // runtime.
    sqlx::migrate!("./migrations").run(&mut conn).await?;

    Ok(())
}
