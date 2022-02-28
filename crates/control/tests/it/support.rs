use std::net::TcpListener;

use serde::Serialize;
use sqlx::PgPool;

use control::{config, startup};

pub mod factory;
pub mod redactor;
pub mod test_database;

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
        let db = test_database::test_db_pool(test_name)
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

    let builds_root_uri = url::Url::parse(&format!("file://{}/", std::env::temp_dir().display()))?;
    let (put_builds, fetch_builds) = startup::init_builds_root(&config::BuildsRootSettings {
        uri: builds_root_uri,
    })?;

    // Tokio runs an executor for each test, so this server will shut down at the end of the test.
    let server = startup::run(listener, db, put_builds, fetch_builds)?;
    let _ = tokio::spawn(server);

    Ok(addr)
}
