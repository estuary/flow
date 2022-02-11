use std::io::Error as IoError;
use std::net::TcpListener;
use std::process::{Command, Output as ProcessOutput};

use serde::Serialize;
use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, PgPool};

use control::config::{self, DatabaseSettings};
use control::startup;

pub mod factory;
pub mod redactor;

/// Creates a `TestContext` with the appropriate test name prefilled.
macro_rules! test_context {
    () => {
        crate::support::TestContext::new(support::function_name!()).await
    };
}
pub(crate) use test_context;

pub struct TestContext {
    pub test_name: &'static str,
    server_address: String,
    db: PgPool,
    http: reqwest::Client,
}

impl TestContext {
    pub async fn new(test_name: &'static str) -> Self {
        let db = test_db_pool(test_name)
            .await
            .expect("Failed to acquire a database connection");
        let server_address = spawn_app(db.clone())
            .await
            .expect("Failed to spawn our app.");
        let http = reqwest::Client::new();

        Self {
            test_name,
            server_address,
            db,
            http,
        }
    }

    pub async fn get(&self, path: &str) -> reqwest::Response {
        self.http
            .get(format!("http://{}{}", &self.server_address, &path))
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn post<P>(&self, path: &str, payload: &P) -> reqwest::Response
    where
        P: Serialize + ?Sized,
    {
        self.http
            .post(format!("http://{}{}", &self.server_address, &path))
            .json(payload)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub fn db(&self) -> &PgPool {
        &self.db
    }
}

/// Returns the full name of the function where it is invoked. This includes the module path to the function.
///
/// Ex. `"acme::anvils::drop_from_a_great_height"`
// Directly pulled from https://github.com/mitsuhiko/insta/blob/e8f3f2782e24b4eb5f256f94bbd98048d4a716ba/src/macros.rs#L1-L17
// Apache Licensed from https://github.com/mitsuhiko/insta/blob/e8f3f2782e24b4eb5f256f94bbd98048d4a716ba/LICENSE
macro_rules! function_name {
    () => {{
        fn f() {}
        fn type_name_of_val<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let mut name = type_name_of_val(f).strip_suffix("::f").unwrap_or("");
        while let Some(rest) = name.strip_suffix("::{{closure}}") {
            name = rest;
        }
        name
    }};
}
pub(crate) use function_name;

/// Spawns a web server listening on localhost and returns its port. This server
/// is scheduled on the test-local executor and will be shut down when the test
/// exits.
pub async fn spawn_app(db: PgPool) -> anyhow::Result<String> {
    // Binding to port 0 will automatically assign a free random port.
    let listener = TcpListener::bind("127.0.0.1:0").expect("No random port available");
    let addr = listener.local_addr()?.to_string();

    // Tokio runs an executor for each test, so this server will shut down at the end of the test.
    let server = startup::run(listener, db)?;
    let _ = tokio::spawn(server);

    Ok(addr)
}

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
/// primary "control_test" database as a template. Returns new
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
    let mut conn = PgConnectOptions::new()
        .host(&db_settings.host)
        .port(db_settings.port)
        .username(&db_settings.username)
        .password(&db_settings.password)
        .database("postgres")
        .connect()
        .await?;

    // Database names can't be parameterized like a normal query.
    sqlx::query(&format!("DROP DATABASE IF EXISTS {}", new_test_db))
        .execute(&mut conn)
        .await?;

    sqlx::query(&format!(
        "CREATE DATABASE {} WITH TEMPLATE {} OWNER {}",
        new_test_db, &db_settings.db_name, &db_settings.username
    ))
    .execute(&mut conn)
    .await?;

    Ok(DatabaseSettings {
        db_name: new_test_db,
        ..db_settings.clone()
    })
}

/// Easily invoke sqlx cli commands to help managed the test database.
pub(crate) struct TestDatabase {
    url: String,
}

impl TestDatabase {
    pub(crate) fn new() -> Self {
        TestDatabase {
            url: config::settings().database.url(),
        }
    }

    pub(crate) fn drop(&self) -> Result<ProcessOutput, IoError> {
        self.run_sqlx(&["database", "drop", "-y"])
    }

    pub(crate) fn setup(&self) -> Result<ProcessOutput, IoError> {
        self.run_sqlx(&["database", "setup"])
    }

    fn run_sqlx(&self, args: &[&str]) -> Result<ProcessOutput, IoError> {
        let cmd_args = [args, &["--database-url", &self.url]].concat();
        Command::new("sqlx").args(cmd_args).output()
    }
}
