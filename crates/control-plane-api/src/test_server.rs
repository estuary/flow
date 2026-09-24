use crate::Snapshot;
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

    let mut actual = crate::snapshot::PgSnapshotSource::new(pg_pool);
    let (mut snapshot, _valid_for, _revoke) = actual
        .refresh(tokens::DateTime::UNIX_EPOCH)
        .await
        .unwrap()
        .unwrap();

    // Shift `taken` well past the test's wall-clock runtime. A denial is only
    // terminal while the snapshot is `taken_after` the request's start; a request
    // that begins after `taken` instead takes the provisional path, cancels the
    // snapshot, and awaits a refresh this single-shot gated source can't serve
    // (it panics rather than re-serve). An hour comfortably exceeds any test, so
    // every request stays inside the window and denials remain terminal.
    snapshot.taken += chrono::TimeDelta::hours(1);

    let source = GatedSnapshot {
        gate,
        actual: Some(snapshot),
    };
    tokens::watch(source).ready_owned().await
}

pub struct TestServer {
    pub addr: std::net::SocketAddr,
    pub encoding_key: tokens::jwt::EncodingKey,
    pub decoding_keys: Vec<tokens::jwt::DecodingKey>,
    _shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl TestServer {
    pub async fn start(pg_pool: sqlx::PgPool, snapshot: Arc<dyn tokens::Watch<Snapshot>>) -> Self {
        Self::start_with_config(
            pg_pool,
            snapshot,
            Some(Arc::new(crate::billing::InMemoryBillingProvider::new())),
            models::AlertConfig::default(),
        )
        .await
    }

    pub async fn start_with_alert_defaults(
        pg_pool: sqlx::PgPool,
        snapshot: Arc<dyn tokens::Watch<Snapshot>>,
        alert_config_defaults: models::AlertConfig,
    ) -> Self {
        Self::start_with_config(
            pg_pool,
            snapshot,
            Some(Arc::new(crate::billing::InMemoryBillingProvider::new())),
            alert_config_defaults,
        )
        .await
    }

    pub async fn start_with_config(
        pg_pool: sqlx::PgPool,
        snapshot: Arc<dyn tokens::Watch<Snapshot>>,
        billing_provider: Option<Arc<dyn crate::billing::BillingProvider>>,
        alert_config_defaults: models::AlertConfig,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        // TODO(johnny): Aggregate into a sink?
        let (logs_tx, _logs_rx) = tokio::sync::mpsc::channel(1);

        // Build an invalid Publisher that will blow up if used.
        let publisher = crate::publications::Publisher::new(
            &url::Url::parse("file:///invalid").unwrap(),
            &"invalid",
            &logs_tx,
            pg_pool.clone(),
            models::IdGenerator::new(0),
            Box::new(NoopBuilder),
        );

        let app = Arc::new(crate::App::new(
            models::IdGenerator::new(0),
            billing_provider,
            b"test-jwt-secret-for-integration-tests",
            pg_pool.clone(),
            publisher,
            snapshot,
            Some(crate::server::public::stripe_webhooks::tests::DEV_WEBHOOK_SECRET.to_string()),
        ));
        let encoding_key = app.control_plane_jwt_encode_key.clone();
        let decoding_keys = app.control_plane_jwt_decode_keys.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind test server");
        let addr = listener.local_addr().expect("failed to get local addr");

        let router =
            crate::server::build_router(app, &[addr.to_string()], alert_config_defaults).unwrap();

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
            decoding_keys,
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
        self.make_masked_access_token(user_id, email, None)
    }

    /// Like `make_access_token`, but with a `capability_mask` claim naming
    /// the given capability bundles, as a scoped token minted for CI would.
    pub fn make_masked_access_token(
        &self,
        user_id: uuid::Uuid,
        email: Option<&str>,
        capability_mask: Option<Vec<String>>,
    ) -> String {
        let now = tokens::now();
        let claims = models::authorizations::ControlClaims {
            iat: now.timestamp() as u64,
            exp: (now + chrono::Duration::hours(1)).timestamp() as u64,
            sub: user_id,
            role: "authenticated".to_string(),
            aud: "authenticated".to_string(),
            email: email.map(String::from),
            capability_mask,
        };

        jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &self.encoding_key,
        )
        .expect("failed to encode JWT")
    }

    /// Verify an access token the server minted, using the same keys the
    /// server verifies bearers with, and return its claims. Panics on a bad
    /// signature or expired token, since a test holding such a token has
    /// already failed.
    pub fn verify_access_token(&self, token: &str) -> crate::ControlClaims {
        tokens::jwt::verify::<crate::ControlClaims>(token.as_bytes(), 0, &self.decoding_keys)
            .expect("server-minted access token must verify")
            .claims()
            .clone()
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

    /// Run one GraphQL `request` as `user_id` under a token whose
    /// `capability_mask` claim names the given bundles. An empty slice is
    /// the empty mask (present but holding nothing), not an unmasked token.
    pub async fn graphql_capability_mask_request(
        &self,
        user_id: uuid::Uuid,
        email: Option<&str>,
        request: &serde_json::Value,
        capability_mask: &[&str],
    ) -> serde_json::Value {
        let token = self.make_masked_access_token(
            user_id,
            email,
            Some(capability_mask.iter().map(|b| b.to_string()).collect()),
        );
        let mut response: serde_json::Value = self.graphql(request, Some(&token)).await;

        // Error `locations` are line/column offsets into the query text,
        // so they change whenever a test reindents its query. Strip them
        // so snapshots pin the error's message and path, not formatting.
        if let Some(errors) = response["errors"].as_array_mut() {
            for error in errors {
                error.as_object_mut().map(|e| e.remove("locations"));
            }
        }
        response
    }
}

/// A no-op Builder for integration testing.
/// This builder will panic if `build` is called.
#[derive(Debug)]
struct NoopBuilder;

#[async_trait::async_trait]
impl crate::publications::builds::Builder for NoopBuilder {
    async fn build(
        &self,
        _builds_root: &url::Url,
        _draft: tables::DraftCatalog,
        _live: tables::LiveCatalog,
        _pub_id: models::Id,
        _build_id: models::Id,
        _tmpdir: &std::path::Path,
        _logs_tx: crate::logs::Tx,
        _logs_token: sqlx::types::Uuid,
        _explicit_plane_name: Option<&str>,
    ) -> anyhow::Result<build::Output> {
        panic!("NoopBuilder::build called in test - this should not happen for authorization tests")
    }
}

/// Build grant tables from compact (subject, object, capability) tuples,
/// shared by tests which pin grant-walk authorization semantics.
pub fn make_grants(
    user_grants: &[(uuid::Uuid, &str, models::Capability)],
    role_grants: &[(&str, &str, models::Capability)],
) -> (tables::UserGrants, tables::RoleGrants) {
    let user_grants =
        tables::UserGrants::from_iter(user_grants.iter().map(|(id, obj, cap)| tables::UserGrant {
            user_id: *id,
            object_role: models::Prefix::new(*obj),
            capability: *cap,
            bundles: Vec::new(),
        }));
    let role_grants = tables::RoleGrants::from_iter(role_grants.iter().map(|(sub, obj, cap)| {
        tables::RoleGrant {
            subject_role: models::Prefix::new(*sub),
            object_role: models::Prefix::new(*obj),
            capability: *cap,
            bundles: Vec::new(),
        }
    }));
    (user_grants, role_grants)
}

/// Build a Snapshot holding only the given user and role grants.
pub fn snapshot_of_grants(
    user_grants: &[(uuid::Uuid, &str, models::Capability)],
    role_grants: &[(&str, &str, models::Capability)],
) -> Snapshot {
    let (user_grants, role_grants) = make_grants(user_grants, role_grants);
    let mut snapshot = Snapshot::empty();
    snapshot.user_grants = user_grants;
    snapshot.role_grants = role_grants;
    snapshot
}
