use control_plane_api::Snapshot;
use std::sync::Arc;

pub fn init() -> tracing::subscriber::DefaultGuard {
    // Enable tracing for the test server.
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing::level_filters::LevelFilter::DEBUG.into())
                .from_env_lossy(),
        )
        .finish();

    tracing::subscriber::set_default(subscriber)
}

pub struct GatedSnapshot {
    gate: bool,
    actual: Option<Snapshot>,
}

impl tokens::Source for GatedSnapshot {
    type Token = Snapshot;
    type Revoke = tokens::WaitForCancellationFutureOwned;

    async fn refresh(
        &mut self,
        _started: tokens::DateTime,
    ) -> tonic::Result<Result<(Self::Token, chrono::TimeDelta, Self::Revoke), chrono::TimeDelta>>
    {
        let snapshot = if self.gate {
            self.gate = false;
            Snapshot::empty()
        } else {
            self.actual
                .take()
                .expect("not refreshed again after actual snapshot")
        };

        let revoked = snapshot.revoke.clone().cancelled_owned();
        Ok(Ok((snapshot, chrono::TimeDelta::MAX, revoked)))
    }
}

pub async fn snapshot(pg_pool: sqlx::PgPool, gate: bool) -> Arc<dyn tokens::Watch<Snapshot>> {
    use tokens::Source;

    let mut actual = control_plane_api::snapshot::PgSnapshotSource::new(pg_pool);
    let (mut snapshot, _valid_for, _revoke) = actual
        .refresh(tokens::DateTime::UNIX_EPOCH)
        .await
        .unwrap()
        .unwrap();

    // Shift forward artificially so it's definitively "after" any following requests.
    snapshot.taken += chrono::TimeDelta::seconds(2);

    let source = GatedSnapshot {
        gate,
        actual: Some(snapshot),
    };
    tokens::watch(source).ready_owned().await
}

pub struct TestServer {
    pub addr: std::net::SocketAddr,
    pub encoding_key: tokens::jwt::EncodingKey,
    _shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl TestServer {
    pub async fn start(pg_pool: sqlx::PgPool, snapshot: Arc<dyn tokens::Watch<Snapshot>>) -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        // TODO(johnny): Aggregate into a sink?
        let (logs_tx, _logs_rx) = tokio::sync::mpsc::channel(1);

        // Build an invalid Publisher that will blow up if used.
        let publisher = control_plane_api::publications::Publisher::new(
            std::path::PathBuf::from("/invalid"),
            &url::Url::parse("file:///invalid").unwrap(),
            &"invalid",
            &logs_tx,
            pg_pool.clone(),
            models::IdGenerator::new(0),
            Box::new(NoopBuilder),
        );

        let app = Arc::new(control_plane_api::App::new(
            models::IdGenerator::new(0),
            b"test-jwt-secret-for-integration-tests",
            pg_pool.clone(),
            publisher,
            snapshot,
        ));
        let encoding_key = app.control_plane_jwt_encode_key.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind test server");
        let addr = listener.local_addr().expect("failed to get local addr");

        let router = control_plane_api::server::build_router(app, &[addr.to_string()]).unwrap();

        tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    _ = shutdown_rx.await;
                })
                .await
                .expect("server error");
        });

        TestServer {
            addr,
            encoding_key,
            _shutdown_tx: shutdown_tx,
        }
    }

    /// Get the base URL for the test server.
    pub fn base_url(&self) -> url::Url {
        format!("http://{}", self.addr).parse().expect("valid URL")
    }

    /// Create a valid access token for a test user.
    /// The token includes all required claims for the server's JWT validation.
    pub fn make_access_token(&self, user_id: uuid::Uuid, email: Option<&str>) -> String {
        let now = tokens::now();
        let claims = models::authorizations::ControlClaims {
            iat: now.timestamp() as u64,
            exp: (now + chrono::Duration::hours(1)).timestamp() as u64,
            sub: user_id,
            role: "authenticated".to_string(),
            aud: "authenticated".to_string(),
            email: email.map(String::from),
        };

        jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &self.encoding_key,
        )
        .expect("failed to encode JWT")
    }

    /// Create a fixed user token PendingWatch (immutable).
    pub fn make_fixed_user_tokens(
        &self,
        user_id: uuid::Uuid,
        email: Option<&str>,
    ) -> tokens::PendingWatch<flow_client_next::user_auth::UserToken> {
        let access_token = self.make_access_token(user_id, email);
        tokens::fixed(Ok(flow_client_next::user_auth::UserToken {
            access_token: Some(access_token),
            refresh_token: None,
        }))
    }

    /// Create a rest::Client pointing at this server.
    pub fn rest_client(&self) -> flow_client_next::rest::Client {
        flow_client_next::rest::Client {
            base_url: self.base_url(),
            http_client: reqwest::Client::new(),
        }
    }

    /// Run a GraphQL request against this test server.
    pub async fn graphql<Request, Response>(
        &self,
        request: &Request,
        access_token: Option<&str>,
    ) -> Response
    where
        Request: serde::Serialize,
        Response: serde::de::DeserializeOwned,
    {
        self.rest_client()
            .post("/api/graphql", &request, access_token)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<Response>()
            .await
            .unwrap()
    }
}

/// A no-op Builder for integration testing.
/// This builder will panic if `build` is called.
#[derive(Debug)]
struct NoopBuilder;

#[async_trait::async_trait]
impl control_plane_api::publications::builds::Builder for NoopBuilder {
    async fn build(
        &self,
        _builds_root: &url::Url,
        _draft: tables::DraftCatalog,
        _live: tables::LiveCatalog,
        _pub_id: models::Id,
        _build_id: models::Id,
        _tmpdir: &std::path::Path,
        _logs_tx: control_plane_api::logs::Tx,
        _logs_token: sqlx::types::Uuid,
        _explicit_plane_name: Option<&str>,
    ) -> anyhow::Result<build::Output> {
        panic!("NoopBuilder::build called in test - this should not happen for authorization tests")
    }
}
