use std::io::Error as IoError;
use std::net::TcpListener;
use std::process::{Command, Output as ProcessOutput};

use control::config;
use control::startup;

pub async fn spawn_app() -> anyhow::Result<String> {
    // Binding to port 0 will automatically assign a free random port.
    let listener = TcpListener::bind("127.0.0.1:0").expect("No random port available");
    let addr = listener.local_addr()?.to_string();

    let db = startup::connect_to_postgres().await;

    // Tokio runs an executor for each test, so this server will shut down at the end of the test.
    let server = startup::run(listener, db)?;
    let _ = tokio::spawn(server);

    Ok(addr)
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
        self.run_sqlx(&["database", "drop"])
    }

    pub(crate) fn setup(&self) -> Result<ProcessOutput, IoError> {
        self.run_sqlx(&["database", "setup"])
    }

    fn run_sqlx(&self, args: &[&str]) -> Result<ProcessOutput, IoError> {
        let cmd_args = [args, &["--database-url", &self.url]].concat();
        Command::new("sqlx").args(cmd_args).output()
    }
}
