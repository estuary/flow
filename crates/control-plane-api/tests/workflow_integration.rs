//! Integration tests for flow-client workflows against control-plane-api server.
//!
//! These tests spin up a real HTTP server with fixture data and exercise
//! the actual client workflow code against it.

use std::sync::Arc;
use tokens::Source;

/// TestServer manages a control-plane-api server instance for integration testing.
pub struct TestServer {
    pub app: Arc<control_plane_api::server::App>,
    pub addr: std::net::SocketAddr,
    pub jwt_secret: Vec<u8>,
    _shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl TestServer {
    /// Start a test server with fixture data.
    pub async fn start() -> Self {
        let jwt_secret = b"test-jwt-secret-for-integration-tests".to_vec();

        let snapshot_data = include_str!("../src/server/snapshot_fixture.json");
        let mut snapshot_data: control_plane_api::server::snapshot::SnapshotData =
            serde_json::from_str(snapshot_data).unwrap();

        // Use a time slightly in the future so requests appear "before" the snapshot.
        snapshot_data.taken = chrono::Utc::now() + chrono::Duration::seconds(5);

        let snapshot = control_plane_api::server::Snapshot::new(snapshot_data);

        let (logs_tx, _logs_rx) = tokio::sync::mpsc::channel(1);
        // Create a dummy pool that will fail if actually used.
        // The authorization endpoints only use the snapshot, not the pool.
        let pg_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1) // Zero triggers a panic.
            .connect_lazy("postgres://invalid:5432/invalid")
            .unwrap();

        let publisher = control_plane_api::publications::Publisher::new(
            std::path::PathBuf::from("/invalid"),
            &url::Url::parse("file:///invalid").unwrap(),
            &"invalid",
            &logs_tx,
            pg_pool.clone(),
            models::IdGenerator::new(0),
            Box::new(NoopBuilder),
        );
        let app = Arc::new(control_plane_api::server::App::new(
            models::IdGenerator::new(0),
            jwt_secret.clone(),
            pg_pool.clone(),
            publisher,
        ));
        *app.snapshot().write().unwrap() = snapshot;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind test server");
        let addr = listener.local_addr().expect("failed to get local addr");

        let router =
            control_plane_api::server::build_router(app.clone(), &[addr.to_string()]).unwrap();

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    _ = shutdown_rx.await;
                })
                .await
                .expect("server error");
        });

        TestServer {
            app,
            addr,
            jwt_secret,
            _shutdown_tx: shutdown_tx,
        }
    }

    /// Get the snapshot's taken timestamp.
    pub fn snapshot_taken(&self) -> chrono::DateTime<chrono::Utc> {
        self.app.snapshot().read().unwrap().taken
    }

    /// Update the snapshot's taken timestamp.
    pub fn set_snapshot_taken(&self, taken: chrono::DateTime<chrono::Utc>) {
        self.app.snapshot().write().unwrap().taken = taken;
    }

    /// Get a request start time that will produce terminal errors (before snapshot).
    pub fn request_time(&self) -> std::time::SystemTime {
        let taken = self.snapshot_taken();
        std::time::UNIX_EPOCH + std::time::Duration::from_secs((taken.timestamp() - 2) as u64)
    }

    /// Get a request start time that will produce retry responses (after snapshot).
    /// Use this to test retry behavior when authorization fails.
    pub fn request_time_for_retry(&self) -> std::time::SystemTime {
        let taken = self.snapshot_taken();
        std::time::UNIX_EPOCH + std::time::Duration::from_secs((taken.timestamp() + 2) as u64)
    }

    /// Get the base URL for the test server.
    pub fn base_url(&self) -> url::Url {
        format!("http://{}", self.addr).parse().expect("valid URL")
    }

    /// Create a valid access token for a test user.
    /// The token includes all required claims for the server's JWT validation.
    pub fn make_access_token(&self, user_id: uuid::Uuid, email: Option<&str>) -> String {
        // The server validates audience as "authenticated"
        #[derive(serde::Serialize)]
        struct TestClaims {
            iat: u64,
            exp: u64,
            sub: uuid::Uuid,
            role: String,
            aud: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            email: Option<String>,
        }

        let now = chrono::Utc::now();
        let claims = TestClaims {
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
            &jsonwebtoken::EncodingKey::from_secret(&self.jwt_secret),
        )
        .expect("failed to encode JWT")
    }

    /// Create a rest::Client pointing at this server.
    pub fn rest_client(&self) -> flow_client::rest::Client {
        flow_client::rest::Client {
            base_url: self.base_url(),
            http_client: reqwest::Client::new(),
        }
    }

    /// Create a user token Watch that can be updated during tests.
    /// Returns the Watch and an update function to change the token.
    pub async fn make_user_token_watch(
        &self,
        user_id: uuid::Uuid,
        email: Option<&str>,
    ) -> (
        Arc<dyn tokens::Watch<flow_client::user_auth::UserToken>>,
        impl Fn(tonic::Result<flow_client::user_auth::UserToken>) -> Option<tokens::CancellationToken>,
    ) {
        let access_token = self.make_access_token(user_id, email);
        let (pending, update) = tokens::manual();
        update(Ok(flow_client::user_auth::UserToken {
            access_token: Some(access_token),
            refresh_token: None,
        }));
        (pending.ready_owned().await, update)
    }

    /// Create a fixed user token Watch (immutable).
    pub async fn make_fixed_user_tokens(
        &self,
        user_id: uuid::Uuid,
        email: Option<&str>,
    ) -> Arc<dyn tokens::Watch<flow_client::user_auth::UserToken>> {
        let access_token = self.make_access_token(user_id, email);
        tokens::fixed(Ok(flow_client::user_auth::UserToken {
            access_token: Some(access_token),
            refresh_token: None,
        }))
        .await
    }
}

/// A no-op Builder for integration testing.
/// This builder will panic if `build` is called.
#[derive(Debug)]
pub struct NoopBuilder;

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

/// User IDs from the Snapshot fixture:
/// - bob@bob: UUID [32; 16] - has write access to bobCo/
/// - alice@alice: UUID [64; 16] - has admin access to aliceCo/ and estuary_support/
#[allow(dead_code)]
mod fixture_users {
    pub fn bob() -> uuid::Uuid {
        uuid::Uuid::from_bytes([32; 16])
    }

    pub fn alice() -> uuid::Uuid {
        uuid::Uuid::from_bytes([64; 16])
    }
}

// =============================================================================
// UserCollectionAuth Tests
// =============================================================================

mod user_collection_auth {
    use super::*;
    use flow_client::workflows::UserCollectionAuth;

    #[tokio::test]
    async fn test_success_write_access() {
        let server = TestServer::start().await;

        let mut workflow = UserCollectionAuth {
            client: server.rest_client(),
            user_tokens: server
                .make_fixed_user_tokens(fixture_users::bob(), Some("bob@bob"))
                .await,
            collection: models::Collection::new("bobCo/anvils/peaches"),
            capability: models::Capability::Write,
        };

        let result = workflow.refresh(server.request_time()).await;

        let (token, valid_for, _revoke) = result
            .expect("request should succeed")
            .expect("should not be a retry");

        assert!(!token.broker_address.is_empty());
        assert!(!token.broker_token.is_empty());
        assert!(
            token
                .journal_name_prefix
                .starts_with("bobCo/anvils/peaches/")
        );
        assert!(valid_for > std::time::Duration::from_secs(60));
    }

    #[tokio::test]
    async fn test_forbidden_no_access() {
        let server = TestServer::start().await;

        let mut workflow = UserCollectionAuth {
            client: server.rest_client(),
            user_tokens: server
                .make_fixed_user_tokens(fixture_users::bob(), Some("bob@bob"))
                .await,
            collection: models::Collection::new("acmeCo/pineapples"),
            capability: models::Capability::Read,
        };

        let result = workflow.refresh(server.request_time()).await;
        let err = result.expect_err("request should fail");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn test_not_found_collection() {
        let server = TestServer::start().await;

        let mut workflow = UserCollectionAuth {
            client: server.rest_client(),
            user_tokens: server
                .make_fixed_user_tokens(fixture_users::bob(), Some("bob@bob"))
                .await,
            collection: models::Collection::new("bobCo/nonexistent/collection"),
            capability: models::Capability::Read,
        };

        let result = workflow.refresh(server.request_time()).await;
        let err = result.expect_err("request should fail");
        assert!(
            err.code() == tonic::Code::NotFound || err.code() == tonic::Code::InvalidArgument,
            "expected NotFound or InvalidArgument, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_retry_response() {
        let server = TestServer::start().await;

        // bob trying to access acmeCo (no permission), but using a timestamp
        // after the snapshot so the server returns retry instead of terminal error.
        let mut workflow = UserCollectionAuth {
            client: server.rest_client(),
            user_tokens: server
                .make_fixed_user_tokens(fixture_users::bob(), Some("bob@bob"))
                .await,
            collection: models::Collection::new("acmeCo/pineapples"),
            capability: models::Capability::Read,
        };

        let result = workflow.refresh(server.request_time_for_retry()).await;

        let retry_after = result
            .expect("should not be terminal error")
            .expect_err("should be retry directive");

        assert!(retry_after > std::time::Duration::ZERO);
    }
}

// =============================================================================
// UserPrefixAuth Tests
// =============================================================================

mod user_prefix_auth {
    use super::*;
    use flow_client::workflows::UserPrefixAuth;

    #[tokio::test]
    async fn test_success_prefix_access() {
        let server = TestServer::start().await;

        let mut workflow = UserPrefixAuth {
            client: server.rest_client(),
            user_tokens: server
                .make_fixed_user_tokens(fixture_users::bob(), Some("bob@bob"))
                .await,
            prefix: models::Prefix::new("bobCo/"),
            data_plane: models::Name::new("ops/dp/public/plane-two"),
            capability: models::Capability::Write,
        };

        let result = workflow.refresh(server.request_time()).await;

        let (token, valid_for, _revoke) = result
            .expect("request should succeed")
            .expect("should not be a retry");

        assert!(!token.broker_address.is_empty());
        assert!(!token.broker_token.is_empty());
        assert!(!token.reactor_address.is_empty());
        assert!(!token.reactor_token.is_empty());
        assert!(valid_for > std::time::Duration::from_secs(60));
    }

    #[tokio::test]
    async fn test_forbidden_no_prefix_access() {
        let server = TestServer::start().await;

        let mut workflow = UserPrefixAuth {
            client: server.rest_client(),
            user_tokens: server
                .make_fixed_user_tokens(fixture_users::bob(), Some("bob@bob"))
                .await,
            prefix: models::Prefix::new("acmeCo/"),
            data_plane: models::Name::new("ops/dp/public/plane-one"),
            capability: models::Capability::Read,
        };

        let result = workflow.refresh(server.request_time()).await;
        let err = result.expect_err("request should fail");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }
}

// =============================================================================
// tokens::watch() Integration Tests
// =============================================================================
// These tests demonstrate the realistic client pattern: creating a workflow
// (which implements Source via RestSource), passing it to tokens::watch(),
// and using the resulting Watch as a real client would.

mod watch_integration {
    use super::*;
    use flow_client::workflows::UserCollectionAuth;

    #[tokio::test]
    async fn test_watch_provides_refreshed_tokens() {
        let server = TestServer::start().await;

        // Create a workflow using the realistic client pattern.
        let workflow = UserCollectionAuth {
            client: server.rest_client(),
            user_tokens: server
                .make_fixed_user_tokens(fixture_users::bob(), Some("bob@bob"))
                .await,
            collection: models::Collection::new("bobCo/anvils/peaches"),
            capability: models::Capability::Write,
        };

        // Use tokens::watch() to get a Watch - this is how real clients would use it.
        // The watch spawns a background task that refreshes tokens proactively.
        let watch = tokens::inner(workflow).await;

        // Verify we got a valid token.
        let token = watch.token();
        let auth = token.result().expect("initial fetch should succeed");

        assert!(!auth.broker_address.is_empty());
        assert!(!auth.broker_token.is_empty());
        assert!(
            auth.journal_name_prefix
                .starts_with("bobCo/anvils/peaches/")
        );
    }

    #[tokio::test]
    async fn test_watch_with_permission_error() {
        let server = TestServer::start().await;

        // Create a workflow for a collection bob doesn't have access to.
        let workflow = UserCollectionAuth {
            client: server.rest_client(),
            user_tokens: server
                .make_fixed_user_tokens(fixture_users::bob(), Some("bob@bob"))
                .await,
            collection: models::Collection::new("acmeCo/pineapples"),
            capability: models::Capability::Read,
        };

        let watch = tokens::inner(workflow).await;

        // The watch should have a permission denied error.
        let token = watch.token();
        let err = token.result().expect_err("should have permission error");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn test_watch_with_dynamic_user_tokens() {
        let server = TestServer::start().await;

        // Create a manual user token watch so we can update it during the test.
        let (user_tokens, _update_user_tokens) = server
            .make_user_token_watch(fixture_users::bob(), Some("bob@bob"))
            .await;

        let workflow = UserCollectionAuth {
            client: server.rest_client(),
            user_tokens,
            collection: models::Collection::new("bobCo/anvils/peaches"),
            capability: models::Capability::Write,
        };

        let watch = tokens::inner(workflow).await;

        // Initial token should be valid.
        let token = watch.token();
        assert!(token.result().is_ok());

        // The update function could be used to simulate credential changes,
        // but for this test we just verify the watch works with manual tokens.
    }
}
