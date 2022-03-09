use std::net::TcpListener;

use axum::body::Body;
use axum::extract::Extension;
use axum::http::{header, Request};
use axum::response::Response;
use axum::Router;
use serde::Serialize;
use sqlx::PgPool;
use tower::util::ServiceExt;

use control::config::BuildsRootSettings;
use control::context::AppContext;
use control::middleware::sessions::CurrentAccount;
use control::models::accounts::Account;
use control::services::builds_root;
use control::startup::{self, FetchBuilds, PutBuilds};

use crate::support::test_database;

/// Creates a `TestContext` with the appropriate test name prefilled.
macro_rules! test_context {
    () => {
        crate::support::context::TestContext::new(
            crate::support::test_introspection::function_name!(),
        )
        .await
    };
}

pub(crate) use test_context;

pub struct TestContext {
    pub test_name: &'static str,
    pub db: PgPool,
    pub app: Router,
    pub auth: Option<Extension<CurrentAccount>>,
}

impl TestContext {
    pub async fn new(test_name: &'static str) -> Self {
        let db = test_database::test_db_pool(test_name)
            .await
            .expect("Failed to acquire a database connection");
        let (put_builds, fetch_builds) = test_builds_root();
        let app_context = AppContext::new(db.clone(), put_builds, fetch_builds);
        let app = startup::app(app_context.clone());

        Self {
            test_name,
            db,
            app,
            auth: None,
        }
    }

    pub fn login(&mut self, account: Account) {
        self.auth = Some(Extension(CurrentAccount(account)));
    }

    // pub fn logout(&mut self) {
    //     self.auth = None;
    // }

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
        if let Some(auth) = &self.auth {
            self.app.clone().layer(auth)
        } else {
            self.app.clone()
        }
    }
}

/// Spawns a web server listening on localhost and returns its port. This server
/// is scheduled on the test-local executor and will be shut down when the test
/// exits.
///
/// If in doubt, use the `test_context!` macro above to setup an application.
/// This is only necessary for tests which require a real http server (most
/// do not) to function correctly.
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

/// Initializes a local builds_root service-pair in a tmp_dir.
pub fn test_builds_root() -> (PutBuilds, FetchBuilds) {
    let builds_root_uri = url::Url::parse(&format!("file://{}/", std::env::temp_dir().display()))
        .expect("to parse tempdir path");
    builds_root::init_builds_root(&BuildsRootSettings {
        uri: builds_root_uri,
    })
    .expect("to initialize builds root")
}
