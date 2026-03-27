pub mod mock_connectors;
pub mod publish_utils;
mod server;
mod snapshot;

use models::Capability;
use uuid::Uuid;

use crate::App;
use crate::publications::{
    DefaultRetryPolicy, DraftPublication, NoopInitialize, NoopWithCommit, PublicationResult,
    Publisher,
};
use crate::testing::mock_connectors::MockDiscoverConnectors;

pub use self::server::TestServer;
pub use self::snapshot::new_snapshot;

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

#[allow(unused)]
pub struct TestApp {
    build_failures: publish_utils::InjectBuildFailures,
    pg_pool: sqlx::PgPool,
    /// Used for build databases, will be deleted automatically when TestApp is dropped.
    tempdir: tempfile::TempDir,
    /// Allows mocking interactions with connectors during discovers and publications
    connectors: MockDiscoverConnectors,
}

impl TestApp {
    pub fn new(pg_pool: sqlx::PgPool) -> TestApp {
        let connectors = MockDiscoverConnectors::default();

        // Used to store build databases from publications
        let tempdir = tempfile::tempdir().expect("Failed to create tempdir");

        TestApp {
            pg_pool,
            tempdir,
            connectors,
        }
    }

    async fn app(&self) -> App {
        const JWT_SECRET: [u8; 32] = [0u8; 32]; // Test JWT secret

        // Initialize a fresh snapshot each time, in case authZ roles have changed.
        // Note that some tests might prefer an api that lets them manually control
        // when snapshots are refreshed, but for now we're just keeping it simple.
        let snapshot_data = crate::snapshot::try_fetch(&self.pg_pool, &mut Default::default())
            .await
            .expect("failed to fetch Snapshot");
        let snapshot = crate::Snapshot::new(tokens::now(), snapshot_data);
        let snapshot_watch = tokens::fixed(Ok(snapshot)).ready_owned().await;

        let builder = crate::publications::builds::new_builder(self.connectors.clone());
        let (logs_tx, logs_rx) = tokio::sync::mpsc::channel::<crate::logs::Line>(64);
        tokio::spawn(async move {
            let mut logs_rx = logs_rx;
            while let Some(log) = logs_rx.recv().await {
                tracing::debug!(token = %log.token, stream = %log.stream, line = %log.line, "publication log");
            }
            tracing::debug!("end of publication log");
        });
        let publisher = Publisher::new(
            "/not/a/real/flowctl-go".into(),
            &url::Url::from_directory_path(self.tempdir.path()).unwrap(),
            "some-connector-network",
            &logs_tx,
            self.pg_pool.clone(),
            models::IdGenerator::new(1),
            builder,
        )
        .with_skip_all_tests();

        crate::App::new(
            models::IdGenerator::new(2),
            &JWT_SECRET,
            self.pg_pool.clone(),
            publisher,
            snapshot_watch,
        )
    }

    /// Setup a new tenant with the given name, and return the id of the user
    /// who has `admin` capabilities to it. Performs essentially the same setup
    /// as the beta onboarding directive, so the user_grants, role_grants,
    /// storage_mappings, and tenants tables should all look just like they
    /// would in production.
    pub async fn setup_tenant(&self, tenant: &str) -> sqlx::types::Uuid {
        let user_id = sqlx::types::Uuid::new_v4();
        let email = format!("{tenant}@testing.test");

        let meta = serde_json::json!({
            "picture": format!("http://{tenant}.test/avatar"),
            "full_name": format!("Full ({tenant}) Name"),
        });

        let mut txn = self.pg_pool.begin().await.unwrap();
        sqlx::query!(
            r#"insert into auth.users(id, email, raw_user_meta_data) values ($1, $2, $3)"#,
            user_id,
            email.as_str(),
            meta
        )
        .execute(&mut *txn)
        .await
        .expect("failed to create user");

        crate::directives::beta_onboard::provision_tenant(
            "support@estuary.dev",
            None,
            tenant,
            user_id,
            &mut txn,
        )
        .await
        .expect("failed to provision tenant");

        // Remove the estuary_support/ role grant, which gets automatically
        // added by a trigger whenever we create a new tenant. Removing it here
        // ensures that things still work correctly without it.
        sqlx::query!(r#"delete from role_grants where subject_role = 'estuary_support/';"#)
            .execute(&mut *txn)
            .await
            .expect("failed to remove estuary_support/ role");

        txn.commit().await.expect("failed to commit transaction");
        user_id
    }

    pub async fn add_role_grant(&mut self, subject: &str, object: &str, capability: Capability) {
        sqlx::query!(
            r#"
                insert into role_grants (subject_role, object_role, capability)
                values ($1, $2, $3)
            "#,
            subject as &str,
            object as &str,
            capability as Capability,
        )
        .execute(&self.pg_pool)
        .await
        .unwrap();
    }

    pub async fn add_user_grant(&mut self, user_id: Uuid, role: &str, capability: Capability) {
        let mut txn = self.pg_pool.begin().await.unwrap();
        crate::directives::grant::upsert_user_grant(
            user_id,
            role,
            capability,
            Some("test grant".to_string()),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
    }

    pub async fn execute_graphql_query<T>(&self, user_id: Uuid, query_str: &str, variables: &serde_json::Value) -> anyhow::Result<T>
    where T: serde::de::DeserializeOwned {
        let app = self.app().await;
        // Create control claims for the user
        let req_start = chrono::Utc::now();
        let claims = models::authorizations::ControlClaims {
            aud: "authenticated".to_string(),
            sub: user_id,
            iat: req_start.timestamp() as u64,
            exp: (req_start + chrono::Duration::hours(1)).timestamp() as u64,
            role: "authenticated".to_string(),
            email: Some("user@example.com".to_string()),
        };

        let token = tokens::jwt::sign(&claims, &app.control_plane_jwt_encode_key)
            .expect("failed to sign test JWT");
        let verified = tokens::jwt::verify(token.as_bytes(), 0, &app.control_plane_jwt_decode_keys)
            .expect("failed to verify test JWT");

        let envelope = control_plane_api::Envelope {
            maybe_claims: control_plane_api::MaybeControlClaims::with_verified(verified),
            original_uri: axum::http::Uri::from_static("/graphql"),
            pg_pool: self.pool.clone(),
            refresh: app.snapshot.token(),
            retry_after: tokens::DateTime::UNIX_EPOCH,
            started: tokens::now(),
            locale: control_plane_api::Locale::EnUS,
        };

        // Create GraphQL schema
        let schema = control_plane_api::server::public::graphql::create_schema();

        // Create GraphQL request
        let request = async_graphql::Request::new(query)
            .variables(async_graphql::Variables::from_json(variables.clone()))
            .data(envelope)
            .data(async_graphql::dataloader::DataLoader::new(
                control_plane_api::server::public::graphql::PgDataLoader(self.pool.clone()),
                tokio::spawn,
            ));

        // Execute the query using a pretty short timeout. This is necessary because the
        // server will try to wait for a Snapshot refresh when an authZ check fails, but
        // snapshots are never automatically refreshed during integration tests.
        let response =
            tokio::time::timeout(std::time::Duration::from_secs(1), schema.execute(request))
                .await
                .context("graphql query timed out (note: authorization failures could be a common cause of this if the query omitted a startedAt parameter)")?;

        // Check for errors
        if !response.errors.is_empty() {
            let mut errors = response.errors;
            // If any errors include the `retryAfter` extension, then assert
            // that the value is in the near future and then set it to a const
            // that works for snapshots.
            for err in errors.iter_mut() {
                if let Some(ext) = err.extensions.as_mut()
                    && ext.get("retryAfter").is_some()
                {
                    let prev = ext.get("retryAfter").unwrap().clone().into_json().unwrap();
                    let retry_after: chrono::DateTime<chrono::Utc> =
                        serde_json::from_value(prev).expect("retryAfter must be a UTC timestamp");
                    assert!(
                        retry_after > req_start,
                        "expected error retryAfter to be greater than {req_start}, actual: {retry_after}"
                    );
                    let diff = retry_after - req_start;
                    assert!(
                        diff.num_seconds() <= 30,
                        "expected retryAfter to be at most 30s in the future, got: {retry_after}"
                    );

                    ext.set("retryAfter", "const-retryAfter-for-tests");
                }
            }
            return Err(anyhow::anyhow!("GraphQL errors: {:?}", errors));
    }

    /// Publish the given draft. Ideally, this would be treated as a user-initiated publication,
    /// but for now, it's just a generic system-initiated publication, which happens to be performed
    /// as the given `user_id` (with authZ checks).
    pub async fn publish(
        &self,
        user_id: Uuid,
        detail: Option<String>,
        draft: tables::DraftCatalog,
        // Should we instead try to determine a suitable data plane name automatically?
        data_plane_name: Option<String>,
    ) -> anyhow::Result<PublicationResult> {
        let finalize = self.build_failures.clone();
        let logs_token = Uuid::new_v4();
        let app = self.app().await;
        let publication = DraftPublication {
            user_id,
            detail,
            draft,
            logs_token,
            dry_run: false,
            default_data_plane_name: data_plane_name,
            verify_user_authz: true,
            initialize: NoopInitialize,
            finalize,
            retry: DefaultRetryPolicy,
            with_commit: NoopWithCommit,
        };
        app.publisher.publish(publication).await
    }
}
