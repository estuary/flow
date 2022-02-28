use std::net::TcpListener;

use axum::body::Body;
use axum::http::{header, Request};
use axum::response::Response;
use axum::Router;
use serde::Serialize;
use sqlx::PgPool;
use tower::ServiceExt;

use control::config;
use control::context::AppContext;
use control::services::builds_root::init_builds_root;
use control::startup::{self, FetchBuilds, PutBuilds};

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
    db: PgPool,
    app: Router,
}

impl TestContext {
    pub async fn new(test_name: &'static str) -> Self {
        let db = test_database::test_db_pool(test_name)
            .await
            .expect("Failed to acquire a database connection");
        let (put_builds, fetch_builds) = test_builds_root();
        let app_context = AppContext::new(db.clone(), put_builds, fetch_builds);
        let app = startup::app(app_context.clone());

        Self { test_name, db, app }
    }

    pub async fn get(&self, path: &str) -> Response {
        let req = Request::builder()
            .method(axum::http::Method::GET)
            .uri(path)
            .body(Body::empty())
            .expect("to build GET request");

        self.app()
            .oneshot(req)
            .await
            .expect("axum to always respond")
    }

    pub async fn post<P>(&self, path: &str, payload: &P) -> Response
    where
        P: Serialize + ?Sized,
    {
        let req = Request::builder()
            .method(axum::http::Method::POST)
            .uri(path)
            .header(header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
            .body(Body::from(
                serde_json::to_vec(payload).expect("to serialize request body"),
            ))
            .expect("to build POST request");

        self.app()
            .oneshot(req)
            .await
            .expect("axum to always respond")
    }

    pub fn db(&self) -> &PgPool {
        &self.db
    }

    pub fn app(&self) -> Router {
        self.app.clone()
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

    let (put_builds, fetch_builds) = test_builds_root();
    let ctx = AppContext::new(db, put_builds, fetch_builds);

    // Tokio runs an executor for each test, so this server will shut down at the end of the test.
    let server = startup::run(listener, ctx)?;
    let _ = tokio::spawn(server);

    Ok(addr)
}

pub fn test_builds_root() -> (PutBuilds, FetchBuilds) {
    let builds_root_uri = url::Url::parse(&format!("file://{}/", std::env::temp_dir().display()))
        .expect("to parse tempdir path");
    init_builds_root(&config::BuildsRootSettings {
        uri: builds_root_uri,
    })
    .expect("to initialize builds root")
}
