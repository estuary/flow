use sqlx::migrate::MigrateDatabase;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgConnectOptions;
use sqlx::ConnectOptions;
use sqlx::PgConnection;
use sqlx::PgPool;
use sqlx::Postgres;

use control::config;
use control::config::DatabaseSettings;
use control::startup;

/// Creates a copy of the test database for this specific test. This provides an
/// isolated test database shared by the test code and the server under test.
///
/// Each test database is dropped at the beginning of the test before it is
/// created. This leaves the test database around as an artifact for inspection
/// and debugging after a test run.
///
/// **Important**: This should only be invoked at the beginning of a test, from
/// the top level. Invoking it from elsewhere will generate a test database with
/// an unexpected name.
pub async fn test_db_pool(test_db_name: &str) -> anyhow::Result<PgPool> {
    let test_db_settings = create_test_db(&config::settings().database, &test_db_name).await?;
    let test_db = startup::connect_to_postgres(&test_db_settings).await;

    Ok(test_db)
}

/// Creates a sandboxed test database for this specific test by using the
/// primary "control_development" database as a template. Returns new
/// `DatabaseSettings` configured to connect to this new database.
///
/// The new database name includes the test name to support identification,
/// connection, and inspection of the correct test database.
///
/// To help prevent a proliferation of extra databases, the target test database
/// is dropped before it is copied from the template. This ensures all the tests
/// run against a current version of the schema.
async fn create_test_db(
    db_settings: &DatabaseSettings,
    new_test_db: &str,
) -> anyhow::Result<DatabaseSettings> {
    // Sanitize the name. We're using the full module path of the test name as
    // the test database name, which includes `:`.
    let new_test_db = new_test_db.replace("::", "__");

    // Make one connection. We explicitly do not connect to the template
    // database, as it prevents copying it as a template.
    let mut conn = maintenance_connection(&db_settings).await?;

    // Database names can't be parameterized like a normal query.
    sqlx::query(&format!("DROP DATABASE IF EXISTS {}", new_test_db))
        .execute(&mut conn)
        .await?;

    sqlx::query(&format!(
        "CREATE DATABASE {} WITH TEMPLATE {} OWNER {}",
        new_test_db, TEMPLATE_DATABASE, &db_settings.username
    ))
    .execute(&mut conn)
    .await?;

    Ok(DatabaseSettings {
        db_name: new_test_db,
        ..db_settings.clone()
    })
}

static TEMPLATE_DATABASE: &'static str = "control_test";

/// Sets up a blank "control_test" database with the latest schema. Subsequent
/// tests will clone this database schema.
pub fn setup(mut db_settings: DatabaseSettings) -> anyhow::Result<()> {
    db_settings.db_name = TEMPLATE_DATABASE.to_owned();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();

    runtime.block_on(async move { reset(&mut db_settings).await })?;

    Ok(())
}

async fn reset(db_settings: &DatabaseSettings) -> anyhow::Result<()> {
    // A workspace package's tests always run from the package root
    // (flow/crates/control). We can use this to find our migrations.
    let root_dir = std::env::current_dir()?;

    let url = db_settings.url();

    // Remove old database in case the schema has changed.
    if Postgres::database_exists(&url).await? {
        Postgres::drop_database(&url).await?;
    }

    // Create a new copy.
    Postgres::create_database(&url).await?;

    // Now that we've created the database, we can connect to "control_test" to run the migrations.
    let mut conn = maintenance_connection(&db_settings).await?;

    // Run all the migrations.
    Migrator::new(root_dir.join("./migrations"))
        .await?
        .run(&mut conn)
        .await?;

    Ok(())
}

/// Connects to Postgres outside of a pool.
async fn maintenance_connection(
    db_settings: &DatabaseSettings,
) -> Result<PgConnection, sqlx::Error> {
    PgConnectOptions::new()
        .host(&db_settings.host)
        .port(db_settings.port)
        .username(&db_settings.username)
        .password(&db_settings.password)
        .database(&db_settings.db_name)
        .connect()
        .await
}
