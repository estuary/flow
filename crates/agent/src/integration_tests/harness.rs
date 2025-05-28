pub mod connectors;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use crate::publications::{NoopInitialize, NoopWithCommit};
use crate::{
    controllers::ControllerState,
    controlplane::ConnectorSpec,
    discovers::{self, DiscoverHandler, DiscoverOutput},
    evolution,
    publications::{
        self, DefaultRetryPolicy, DraftPublication, PublicationResult, Publisher, UncommittedBuild,
    },
    ControlPlane, PGControlPlane,
};
use agent_sql::{Capability, TextJson};
use anyhow::Context;
use chrono::{DateTime, Utc};
use gazette::consumer::ReplicaStatus;
use models::status::activation::ShardFailure;
use models::status::connector::ConfigUpdate;
use models::{CatalogType, Id};
use proto_flow::AnyBuiltSpec;
use proto_gazette::consumer::replica_status;
use serde::Deserialize;
use serde_json::{value::RawValue, Value};
use sqlx::types::Uuid;
use tables::DraftRow;
use tempfile::tempdir;
use tokio::sync::Semaphore;

use self::connectors::MockDiscoverConnectors;

const FIXED_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

pub fn set_of(names: &[&str]) -> BTreeSet<String> {
    names.into_iter().map(|n| n.to_string()).collect()
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Suppress lints for fields used only in test snapshots
pub struct LiveSpec {
    pub catalog_name: String,
    pub connector_image_name: Option<String>,
    pub connector_image_tag: Option<String>,
    pub reads_from: Option<Vec<String>>,
    pub writes_to: Option<Vec<String>>,
    pub spec: Option<Value>,
    pub spec_type: Option<String>,
}

#[derive(Debug)]
pub struct ScenarioResult {
    pub publication_row_id: Id,
    /// The pub_id of a successfully committed publication, which will match with
    /// the `live_specs.last_pub_id` and `publication_specs.pub_id`. Will be None if
    /// the publication failed.
    pub pub_id: Option<Id>,
    pub status: publications::JobStatus,
    pub errors: Vec<(String, String)>,
    pub live_specs: Vec<LiveSpec>,
}

pub struct UserDiscoverResult {
    pub job_status: discovers::executor::JobStatus,
    pub draft: tables::DraftCatalog,
    pub errors: Vec<(String, String)>,
}

impl UserDiscoverResult {
    async fn load(discover_id: Id, db: &sqlx::PgPool) -> UserDiscoverResult {
        let discover = sqlx::query!(
            r#"select
                draft_id as "draft_id: Id",
                job_status as "job_status: TextJson<discovers::executor::JobStatus>"
            from discovers
            where id = $1;"#,
            discover_id as Id,
        )
        .fetch_one(db)
        .await
        .expect("failed to query discover");

        let draft = crate::draft::load_draft(discover.draft_id, db)
            .await
            .unwrap();

        let errors = load_draft_errors(discover.draft_id, db).await;

        UserDiscoverResult {
            job_status: discover.job_status.0,
            draft,
            errors,
        }
    }
}

async fn load_draft_errors(draft_id: Id, db: &sqlx::PgPool) -> Vec<(String, String)> {
    sqlx::query!(
        r#"select scope, detail from draft_errors where draft_id = $1;"#,
        draft_id as Id
    )
    .fetch_all(db)
    .await
    .unwrap()
    .into_iter()
    .map(|de| (de.scope, de.detail))
    .collect::<Vec<(String, String)>>()
}

/// Facilitates writing integration tests.
/// **Note:** integration tests require exclusive access to the database,
/// so it's required to use the attribute: `#[serial_test::serial]` on every
/// test that uses a `TestHarness`. Initializing a new harness will clear out
/// (nearly) all data in the database, to ensure each test run starts with a
/// clean slate.
pub struct TestHarness {
    pub control_plane: TestControlPlane,
    pub test_name: String,
    pub pool: sqlx::PgPool,
    pub publisher: Publisher<MockDiscoverConnectors>,
    #[allow(dead_code)] // only here so we don't drop it until the harness is dropped
    pub builds_root: tempfile::TempDir,
    pub discover_handler: DiscoverHandler<connectors::MockDiscoverConnectors>,
    pub controller_exec: crate::controllers::executor::LiveSpecControllerExecutor<TestControlPlane>,
    pub directive_exec: crate::directives::DirectiveHandler,
}

impl TestHarness {
    /// Initializes a new harness, and clears out all existing data in the database.
    pub async fn init(test_name: &str) -> Self {
        // Setup tracing so we can see logs
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);

        let pool = sqlx::PgPool::connect(FIXED_DATABASE_URL)
            .await
            .expect("Failed to connect to database");

        let builds_root = tempdir().expect("Failed to create tempdir");

        // System user id is set in `seed.sql`, so we could technically hard code it here.
        // This is a bit more robust, though, as it ensures that the seed has been run.
        let system_user_id = agent_sql::get_user_id_for_email("support@estuary.dev", &pool)
            .await
            .expect("querying for agent user id");

        let (logs_tx, logs_rx) = tokio::sync::mpsc::channel(8192);
        tokio::spawn(async move {
            let mut logs_rx = logs_rx;
            while let Some(log) = logs_rx.recv().await {
                eprintln!("PUB-LOG: {:?}", log);
            }
            eprintln!("end of PUB-LOG");
        });

        let id_gen = models::IdGenerator::new(1);
        let mock_connectors = connectors::MockDiscoverConnectors::default();
        let discover_handler = DiscoverHandler::new(mock_connectors.clone());

        let publisher = Publisher::new(
            "/not/a/real/bin/dir",
            &url::Url::from_directory_path(builds_root.path()).unwrap(),
            "some-connector-network",
            &logs_tx,
            pool.clone(),
            id_gen.clone(),
            mock_connectors,
        );

        let control_plane = TestControlPlane::new(PGControlPlane::new(
            pool.clone(),
            system_user_id,
            publisher.clone(),
            id_gen.clone(),
            discover_handler.clone(),
            logs_tx.clone(),
        ));

        let controller_exec =
            crate::controllers::executor::LiveSpecControllerExecutor::new(control_plane.clone());
        let directive_exec =
            crate::directives::DirectiveHandler::new("support@estuary.test".to_string(), &logs_tx);

        let mut harness = Self {
            test_name: test_name.to_string(),
            pool,
            publisher,
            builds_root,
            discover_handler,
            control_plane,
            controller_exec,
            directive_exec,
        };
        harness.truncate_tables().await;
        harness.setup_test_connectors().await;

        harness
    }

    async fn setup_test_connectors(&mut self) {
        sqlx::query!(r##"
            with source_image as (
                insert into connectors (external_url, image_name, title, short_description, logo_url, recommended)
                values ('http://test.test/', 'source/test', '{"en-US": "test"}', '{"en-US": "test"}', '{"en-US": "http://test.test/"}', false)
                on conflict(image_name) do update set title = excluded.title
                returning id
            ),
            materialize_image as (
                insert into connectors (external_url, image_name, title, short_description, logo_url, recommended)
                values ('http://test.test/', 'materialize/test', '{"en-US": "test"}', '{"en-US": "test"}', '{"en-US": "http://test.test/"}', false)
                on conflict(image_name) do update set title = excluded.title
                returning id
            ),
            source_tag as (
                insert into connector_tags (
                    connector_id,
                    image_tag,
                    protocol,
                    documentation_url,
                    endpoint_spec_schema,
                    resource_spec_schema,
                    resource_path_pointers,
                    job_status
                ) values (
                    (select id from source_image),
                    ':test',
                    'capture',
                    'http://test.test/',
                    '{"type": "object"}',
                    '{"type": "object", "properties": {"id": {"type": "string", "x-collection-name": true}}}',
                    '{/id}',
                    '{"type": "success"}'
                ) on conflict do nothing
            ),
            materialize_tag as (
                insert into connector_tags (
                    connector_id,
                    image_tag,
                    protocol,
                    documentation_url,
                    endpoint_spec_schema,
                    resource_spec_schema,
                    resource_path_pointers,
                    job_status
                ) values (
                    (select id from materialize_image),
                    ':test',
                    'materialization',
                    'http://test.test/',
                    '{"type": "object"}',
                    '{"type": "object", "properties": {"id": {"type": "string", "x-collection-name": true}, "schema": {"type": "string", "x-schema-name": true}, "delta": {"type": "boolean", "x-delta-updates": true}}}',
                    '{/id}',
                    '{"type": "success"}'
                ) on conflict do nothing
            ),
            materialize_tag_no_annotations as (
                insert into connector_tags (
                    connector_id,
                    image_tag,
                    protocol,
                    documentation_url,
                    endpoint_spec_schema,
                    resource_spec_schema,
                    resource_path_pointers,
                    job_status
                ) values (
                    (select id from materialize_image),
                    ':test-no-annotation',
                    'materialization',
                    'http://test.test/',
                    '{"type": "object"}',
                    '{"type": "object", "properties": {"id": {"type": "string", "x-collection-name": true}, "schema": {"type": "string"}, "delta": {"type": "boolean"}}}',
                    '{/id}',
                    '{"type": "success"}'
                ) on conflict do nothing
            ),
            default_data_plane as (
                insert into data_planes (
                    data_plane_name,
                    data_plane_fqdn,
                    ops_logs_name,
                    ops_stats_name,
                    ops_l1_inferred_name,
                    ops_l1_stats_name,
                    ops_l1_events_name,
                    ops_l2_inferred_transform,
                    ops_l2_stats_transform,
                    ops_l2_events_transform,
                    broker_address,
                    reactor_address,
                    hmac_keys,
                    enable_l2
                ) values (
                    'ops/dp/public/test',
                    'test.dp.estuary-data.com',
                    'ops/logs',
                    'ops/stats',
                    'ops/L1/inferred',
                    'ops/L1/stats',
                    'ops/L1/events',
                    'from-L1-inferred',
                    'from-L1-stats',
                    'from-L1-events',
                    'broker:address',
                    'reactor:address',
                    '{secret-key}',
                    false
                ) on conflict do nothing
            )
            select 1 as "something: bool";
            "##).fetch_one(&self.pool).await.expect("failed to setup test connectors");
    }

    /// Ideally, we'd get a whole separate database for each integration test
    /// run, but for now we just clear out all the tables that we need to. Uses
    /// `delete from` instead of `truncate` becuase some dude on the internet
    /// said it's faster, lol.
    async fn truncate_tables(&mut self) {
        tracing::warn!("clearing all data before test");
        let system_user_id = self.control_plane().inner.system_user_id;

        // We need to disable this trigger, or else it will prevent us from deleting
        // applied directives.
        sqlx::query(
            r##"alter table applied_directives
           disable trigger "Verify delete of applied directives";"##,
        )
        .execute(&self.pool)
        .await
        .expect("failed to disable trigger");

        sqlx::query!(
            r#"
            with del_live_specs as (
                delete from live_specs
            ),
            del_flows as (
                delete from live_spec_flows
            ),
            del_controllers as (
                delete from controller_jobs
            ),
            del_automations as (
                delete from internal.tasks
            ),
            del_draft_specs as (
                delete from draft_specs
            ),
            del_draft_errs as (
                delete from draft_errors
            ),
            del_drafts as (
                delete from drafts
            ),
            del_discovers as (
                delete from discovers
            ),
            del_evolutions as (
                delete from evolutions
            ),
            del_publications as (
                delete from publications
            ),
            del_tenants as (
                delete from tenants
            ),
            del_user_grants as (
                -- preserve the system user's role grants
                delete from user_grants where user_id != $1
            ),
            del_role_grants as (
                delete from role_grants
            ),
            del_directives as (
                delete from directives
            ),
            del_applied_directives as (
                delete from applied_directives
            ),
            del_tasks as (
                delete from internal.tasks
            ),
            del_alert_subs as (
                delete from alert_subscriptions
            ),
            del_processing_alerts as (
                delete from alert_data_processing
            ),
            del_alert_history as (
                delete from alert_history
            ),
            del_inferred_schemas as (
                delete from inferred_schemas
            ),
            del_hourly_stats as (
                delete from catalog_stats_hourly
            ),
            del_daily_stats as (
                delete from catalog_stats_daily
            )
            delete from catalog_stats_monthly;"#,
            system_user_id
        )
        .execute(&self.pool)
        .await
        .expect("failed to truncate tables");

        sqlx::query(
            r##"alter table applied_directives
           enable trigger "Verify delete of applied directives";"##,
        )
        .execute(&self.pool)
        .await
        .expect("failed to enable trigger");
    }

    /// Returns a mutable reference to the control plane, which can be used for
    /// testing control plane operations or verifying results. See `TestControlPlane`
    /// comments for deets.
    pub fn control_plane(&mut self) -> &mut TestControlPlane {
        &mut self.control_plane
    }

    /// Setup a new tenant with the given name, and return the id of the user
    /// who has `admin` capabilities to it. Performs essentially the same setup
    /// as the beta onboarding directive, so the user_grants, role_grants,
    /// storage_mappings, and tenants tables should all look just like they
    /// would in production.
    pub async fn setup_tenant(&self, tenant: &str) -> sqlx::types::Uuid {
        let user_id = sqlx::types::Uuid::new_v4();
        let email = format!("{user_id}@{tenant}.test");

        let mut txn = self.pool.begin().await.unwrap();
        sqlx::query!(
            r#"insert into auth.users(id, email) values ($1, $2)"#,
            user_id,
            email.as_str()
        )
        .execute(&mut txn)
        .await
        .expect("failed to create user");

        agent_sql::directives::beta_onboard::provision_tenant(
            "support@estuary.dev",
            Some(format!("for test: {}", self.test_name)),
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
            .execute(&mut txn)
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
        .execute(&self.pool)
        .await
        .unwrap();
    }

    pub async fn add_user_grant(&mut self, user_id: Uuid, role: &str, capability: Capability) {
        let mut txn = self.pool.begin().await.unwrap();
        agent_sql::directives::grant::upsert_user_grant(
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

    pub async fn assert_specs_touched_since(&mut self, prev_specs: &tables::LiveCatalog) {
        let user_id = self.control_plane().inner.system_user_id;
        let owned_names: Vec<String> = prev_specs
            .all_spec_names()
            .map(|n| (*n).to_owned())
            .collect();
        let specs = agent_sql::live_specs::fetch_live_specs(
            user_id,
            &owned_names,
            false, /* don't fetch user capabilities */
            false, /* don't fetch spec capabilities */
            &self.pool,
        )
        .await
        .expect("failed to query live specs");
        assert_eq!(
            prev_specs.spec_count(),
            specs.len(),
            "expected to fetch {} specs, but got {}",
            prev_specs.spec_count(),
            specs.len()
        );

        for spec in specs {
            let (expect_last_pub, prev_last_build) = match spec.spec_type.map(Into::into) {
                None => panic!(
                    "expected spec {} to have been touched, but spec_type is null",
                    spec.catalog_name
                ),
                Some(CatalogType::Capture) => {
                    let row = prev_specs
                        .captures
                        .get_by_key(&models::Capture::new(&spec.catalog_name))
                        .unwrap();
                    (row.last_pub_id, row.last_build_id)
                }
                Some(CatalogType::Collection) => {
                    let row = prev_specs
                        .collections
                        .get_by_key(&models::Collection::new(&spec.catalog_name))
                        .unwrap();
                    (row.last_pub_id, row.last_build_id)
                }
                Some(CatalogType::Materialization) => {
                    let row = prev_specs
                        .materializations
                        .get_by_key(&models::Materialization::new(&spec.catalog_name))
                        .unwrap();
                    (row.last_pub_id, row.last_build_id)
                }
                Some(CatalogType::Test) => {
                    let row = prev_specs
                        .tests
                        .get_by_key(&models::Test::new(&spec.catalog_name))
                        .unwrap();
                    (row.last_pub_id, row.last_build_id)
                }
            };

            assert_eq!(
                expect_last_pub,
                spec.last_pub_id.into(),
                "expected touched spec '{}' to have last_pub_id: {}, but was: {}",
                spec.catalog_name,
                expect_last_pub,
                spec.last_pub_id
            );
            assert!(
                spec.last_build_id > prev_last_build,
                "expected touched spec '{}' to have last_build_id ({}) > the previous last_build_id ({})",
                spec.catalog_name,
                spec.last_build_id,
                prev_last_build,
            );

            assert!(
                spec.last_build_id > spec.last_pub_id,
                "sanity check to ensure that last_build_id > last_pub_id"
            );
            // sanity check that we haven't created publication specs
            let rows = sqlx::query!(
                r#"select
                ls.catalog_name,
                ps.pub_id as "pub_id: Id"
                from live_specs ls
                join publication_specs ps on ls.id = ps.live_spec_id
                where ls.catalog_name = $1
                and ps.pub_id > $2
                order by ls.catalog_name;"#,
                spec.catalog_name.as_str(),
                Id::from(expect_last_pub) as Id,
            )
            .fetch_all(&self.pool)
            .await
            .expect("failed to query publication_specs");
            assert!(
                rows.is_empty(),
                "expected no publication specs to exist for touched specs, got {rows:?}"
            );
        }
    }

    pub async fn get_enqueued_controllers(&mut self, within: chrono::Duration) -> Vec<String> {
        let threshold = chrono::Utc::now() + within;
        sqlx::query_scalar!(
            r#"select ls.catalog_name
            from live_specs ls
            join internal.tasks t on ls.controller_task_id = t.task_id
            where t.wake_at is not null and t.wake_at <= $1
            order by ls.catalog_name"#,
            threshold,
        )
        .fetch_all(&self.pool)
        .await
        .expect("failed to query enqueued controllers")
    }

    pub async fn assert_live_spec_hard_deleted(&mut self, name: &str) {
        let rows = sqlx::query!(
            r#"select
            id as "id: Id",
            spec as "spec: agent_sql::TextJson<Box<RawValue>>",
            spec_type as "spec_type: agent_sql::CatalogType"
            from live_specs where catalog_name = $1;"#,
            name
        )
        .fetch_all(&self.pool)
        .await
        .expect("failed to execute query");
        assert!(
            rows.is_empty(),
            "expected no live specs for name: '{name}', found: {rows:?}"
        );

        let inferred_schema = self
            .control_plane()
            .get_inferred_schema(models::Collection::new(name))
            .await
            .expect("failed to fetch inferred schema");
        assert!(
            inferred_schema.is_none(),
            "expecte inferred schema to have been deleted"
        );
    }

    pub async fn assert_live_spec_soft_deleted(&mut self, name: &str, last_pub_id: Id) {
        let row = sqlx::query!(
            r#"
            select
                id as "id: Id",
                last_pub_id as "last_pub_id: Id",
                spec_type as "spec_type?: agent_sql::CatalogType",
                spec as "spec: agent_sql::TextJson<Box<RawValue>>",
                reads_from as "reads_from: Vec<String>",
                writes_to as "writes_to: Vec<String>",
                built_spec as "built_spec: agent_sql::TextJson<Box<RawValue>>",
                inferred_schema_md5
            from live_specs
            where catalog_name = $1;
            "#,
            name
        )
        .fetch_one(&self.pool)
        .await
        .expect("failed to query live_specs");
        assert_eq!(last_pub_id, row.last_pub_id.into());
        assert!(row.spec_type.is_some()); // spec_type should still be present
        assert!(row.spec.is_none());
        assert!(row.built_spec.is_none());
        assert!(
            row.reads_from.is_none(),
            "reads_from should be None for soft deleted spec: {name}, {row:?}"
        );
        assert!(
            row.writes_to.is_none(),
            "writes_to should be None for soft deleted spec: {name}, {row:?}"
        );
        assert!(row.inferred_schema_md5.is_none());

        // Assert that ControlPlane::get_live_specs no longer returns the spec
        let mut name_set = BTreeSet::new();
        name_set.insert(name.to_string());
        let live_specs = self
            .control_plane()
            .get_live_specs(name_set)
            .await
            .expect("get_live_specs failed");
        assert_eq!(0, live_specs.spec_count());
    }

    pub async fn set_auto_discover_due(&mut self, capture: &str) {
        let now = Utc::now().to_rfc3339();
        sqlx::query!(
            r#"update controller_jobs
            set status = jsonb_set(status::jsonb, '{auto_discover, next_at}', to_jsonb($2::text), true)::json
            where live_spec_id = (select id from live_specs where catalog_name = $1)
            returning 1 as "must_exist: bool";"#,
            capture,
            now
        ).fetch_one(&self.pool)
        .await
        .expect("failed to set auto-discover next_at");
    }

    /// Change the timestamp of the last publication in the history to simulate
    /// the passage of time, so another publication can be attempted.
    pub async fn push_back_last_pub_history_ts(
        &mut self,
        catalog_name: &str,
        new_ts: DateTime<Utc>,
    ) {
        sqlx::query!(
            r#"
            update controller_jobs
            set status = jsonb_set(status::jsonb, '{publications, history, 0, completed }', $2)::json
            where live_spec_id = (select id from live_specs where catalog_name::text = $1);"#,
            catalog_name,
            serde_json::Value::String(new_ts.to_rfc3339()),
        )
        .execute(&self.pool)
        .await
        .unwrap();
    }

    /// Change the timestamp of the last attempted config update publication in the history to simulate
    /// the passage of time, so another config update publication can be attempted.
    pub async fn push_back_last_config_update_pub_history_ts(
        &mut self,
        catalog_name: &str,
        new_ts: DateTime<Utc>,
    ) {
        sqlx::query!(
            r#"
            UPDATE controller_jobs
            SET status = jsonb_set(
                status::jsonb,
                '{config_updates,next_attempt}',
                $2
            )::json
            WHERE live_spec_id = (
                SELECT id FROM live_specs WHERE catalog_name::text = $1
            );
            "#,
            catalog_name,
            serde_json::Value::String(new_ts.to_rfc3339()),
        )
        .execute(&self.pool)
        .await
        .unwrap();
    }

    /// Returns a `ControllerState` representing the given live spec and
    /// controller status from the perspective of a controller.
    pub async fn get_controller_state(&mut self, name: &str) -> ControllerState {
        let job = sqlx::query_as!(
            agent_sql::controllers::ControllerJob,
            r#"select
                ls.id as "live_spec_id: Id",
                ls.catalog_name as "catalog_name!: String",
                ls.last_pub_id as "last_pub_id: Id",
                ls.last_build_id as "last_build_id: Id",
                ls.spec as "live_spec: TextJson<Box<RawValue>>",
                ls.built_spec as "built_spec: TextJson<Box<RawValue>>",
                ls.spec_type as "spec_type: agent_sql::CatalogType",
                ls.dependency_hash as "live_dependency_hash",
                ls.created_at,
                ls.updated_at as "live_spec_updated_at",
                cj.controller_version as "controller_version: i32",
                cj.updated_at as "controller_updated_at",
                cj.logs_token,
                cj.status as "status: TextJson<Box<RawValue>>",
                cj.failures,
                cj.error,
                ls.data_plane_id as "data_plane_id: Id",
                dp.data_plane_name as "data_plane_name?: String"
            from live_specs ls
            join controller_jobs cj on ls.id = cj.live_spec_id
            left outer join data_planes dp on ls.data_plane_id = dp.id
            where ls.catalog_name = $1;"#,
            name
        )
        .fetch_one(&self.pool)
        .await
        .expect("failed to query controller state");
        ControllerState::parse_db_row(&job).expect("failed to parse controller state")
    }

    /// Runs a specific controller task, which must already have a non-null
    /// `wake_at`, though it doesn't necessarily have to be the oldest
    /// `wake_at`, and may also be in the future Returns the `ControllerState`
    /// as it was _after_ the controller ran.
    pub async fn run_pending_controller(&mut self, catalog_name: &str) -> ControllerState {
        self.assert_controller_pending(catalog_name).await;
        sqlx::query!(
            r#"
            select internal.send_to_task(
                (select controller_task_id from live_specs where catalog_name = $1),
                '00:00:00:00:00:00:00:00'::flowid,
                '{"type": "manual_trigger", "user_id": "ffffffff-ffff-ffff-ffff-ffffffffffff"}'
            )"#,
            catalog_name
        )
        .execute(&self.pool)
        .await
        .expect("failed to send controller task");

        let runs = self.run_pending_controllers(Some(1)).await;

        let run = runs.into_iter().next().unwrap();
        assert_eq!(catalog_name, &run.catalog_name);
        run
    }

    pub async fn assert_controller_pending(&mut self, catalog_name: &str) {
        let wake_at: Option<DateTime<Utc>> = self.get_controller_wake_at(catalog_name).await;
        assert!(
            wake_at.is_some(),
            "expected controller for '{catalog_name}' to have a non-null wake_at, but it was null"
        );
    }

    pub async fn assert_controller_not_pending(&mut self, catalog_name: &str) {
        let wake_at: Option<DateTime<Utc>> = self.get_controller_wake_at(catalog_name).await;
        assert!(
            wake_at.is_none(),
            "expected controller for '{catalog_name}' to have a null wake_at, but it was: {wake_at:?}"
        );
    }

    async fn get_controller_wake_at(&mut self, catalog_name: &str) -> Option<DateTime<Utc>> {
        sqlx::query_scalar!(
            r#"select t.wake_at
            from internal.tasks t
            join live_specs ls on t.task_id = ls.controller_task_id
            where ls.catalog_name = $1;"#,
            catalog_name,
        )
        .fetch_one(&self.pool)
        .await
        .expect("get_controller_wake_at query failed")
    }

    /// Runs all controllers until there are no more that are ready. Optionally, `max` can limit the
    /// number of controller runs to perform, which may leave some controllers still pending. Returns
    /// the _starting_ controller state for each controller that was run.
    pub async fn run_pending_controllers(&mut self, max: Option<usize>) -> Vec<ControllerState> {
        let max = max.unwrap_or(usize::MAX);
        assert!(max > 0, "run_pending_controllers max must be > 0");
        let mut states = Vec::new();

        for _ in 0..max {
            let ran = self
                .run_automation_task(automations::task_types::LIVE_SPEC_CONTROLLER)
                .await;
            let Some(task_id) = ran else {
                break;
            };
            let controller_state = crate::controllers::fetch_controller_state(task_id, &self.pool)
                .await
                .expect("failed to fetch controller state");
            if let Some(s) = controller_state {
                states.push(s);
            } else {
                tracing::info!(%task_id, "controller run deleted the task (this is expected if the live spec was deleted");
            }
        }

        states
    }

    /// Runs at most one automation task of the given type, and returns the id of the task that was run.
    /// Returns None if no eligible task was ready.
    pub async fn run_automation_task(&mut self, task_type: automations::TaskType) -> Option<Id> {
        use automations::{task_types, Server};

        let semaphor = Arc::new(Semaphore::new(1));
        let mut permit = semaphor.clone().acquire_owned().await.unwrap();

        let server = match task_type {
            task_types::LIVE_SPEC_CONTROLLER => {
                Server::new().register(self.controller_exec.clone())
            }
            task_types::PUBLICATIONS => Server::new().register(self.publisher.clone()),
            task_types::DISCOVERS => Server::new().register(self.discover_handler.clone()),
            task_types::APPLIED_DIRECTIVES => Server::new().register(self.directive_exec.clone()),
            _ => panic!("unsupported task type: {:?}", task_type),
        };
        let mut next = automations::server::dequeue_tasks(
            &mut permit,
            &self.pool,
            &server,
            &[task_type.0],
            std::time::Duration::from_secs(10),
        )
        .await
        .expect("failed to dequeue automations tasks");
        if next.is_empty() {
            assert_eq!(
                1,
                permit.num_permits(),
                "expect the semaphor permit to still be present"
            );
            return None;
        }

        assert_eq!(1, next.len(), "expected at most 1 dequeued task");
        let task = next.pop().unwrap();
        let task_id = task.task.id;
        tracing::debug!(%task_id, "polling automation task");
        automations::executors::poll_task(task, std::time::Duration::from_secs(10))
            .await
            .expect("failed to poll task");
        Some(task_id)
    }

    pub async fn user_discover(
        &mut self,
        image_name: &str,
        image_tag: &str,
        capture_name: &str,
        draft_id: Id,
        endpoint_config: &str, // TODO: different type?
        update_only: bool,
        mock_discover_resp: connectors::MockDiscover,
    ) -> UserDiscoverResult {
        let connector_tag = sqlx::query!(
            r##"select ct.id as "id: Id"
            from connectors c
            join connector_tags ct on c.id = ct.connector_id
            where c.image_name = $1
            and ct.image_tag = $2;"##,
            image_name,
            image_tag
        )
        .fetch_one(&self.pool)
        .await
        .expect("querying for connector_tags id");

        let config_json = TextJson(models::RawValue::from_str(endpoint_config).unwrap());
        let disco = sqlx::query!(
            r##"insert into discovers (
                capture_name,
                connector_tag_id,
                draft_id,
                endpoint_config,
                update_only,
                data_plane_name
            ) values ($1, $2, $3, $4, $5, 'ops/dp/public/test')
            returning id as "id: Id";"##,
            capture_name as &str,
            connector_tag.id as Id,
            draft_id as Id,
            config_json as TextJson<models::RawValue>,
            update_only
        )
        .fetch_one(&self.pool)
        .await
        .unwrap();
        let disco_id = disco.id;

        self.discover_handler
            .connectors
            .mock_discover(capture_name, mock_discover_resp);

        let Some(task_id) = self
            .run_automation_task(automations::task_types::DISCOVERS)
            .await
        else {
            panic!("expected a discover task to have run");
        };
        assert_eq!(
            task_id, disco_id,
            "expected discover {disco_id} to have run, but {task_id} got ran instead"
        );

        UserDiscoverResult::load(disco_id, &self.pool).await
    }

    /// Performs a publication as if it were initiated by `flowctl` or the UI,
    /// and return a `ScenarioResult` describing the results.
    pub async fn user_publication(
        &mut self,
        user_id: Uuid,
        detail: impl Into<String>,
        draft: tables::DraftCatalog,
    ) -> ScenarioResult {
        self.async_publication(user_id, detail, Either::L(draft))
            .await
    }

    pub async fn create_user_publication(
        &mut self,
        user_id: Uuid,
        draft_id: Id,
        detail: impl Into<String>,
    ) -> ScenarioResult {
        self.async_publication(user_id, detail, Either::R(draft_id))
            .await
    }

    /// Runs a publication by inserting into the `publications` table and
    /// waiting for the publications handler to process it. Returns
    /// a `ScenarioResult` (a hold over from the old publications tests, which
    /// were ported over) describing the results of the publication.
    async fn async_publication(
        &mut self,
        user_id: Uuid,
        detail: impl Into<String>,
        draft: Either<tables::DraftCatalog, Id>,
    ) -> ScenarioResult {
        let detail = detail.into();
        let draft_id = match draft {
            Either::L(catalog) => self.create_draft(user_id, detail.clone(), catalog).await,
            Either::R(id) => id,
        };
        let mut txn = self
            .pool
            .begin()
            .await
            .expect("failed to start transaction");
        let pub_id = agent_sql::publications::create(
            &mut txn,
            user_id,
            draft_id,
            detail.clone(),
            "ops/dp/public/test".to_string(),
        )
        .await
        .expect("failed to create publication");
        txn.commit().await.expect("failed to commit transaction");

        let task_id = self
            .run_automation_task(automations::task_types::PUBLICATIONS)
            .await
            .expect("expected a publication task to have run");
        assert_eq!(
            task_id, pub_id,
            "automations task id should match the publication that was just created"
        );

        let pub_result = self.get_publication_result(pub_id.into()).await;
        assert_ne!(publications::JobStatus::Queued, pub_result.status);
        pub_result
    }

    async fn get_publication_result(&mut self, publication_row_id: Id) -> ScenarioResult {
        let specs = sqlx::query_as!(
            LiveSpec,
            r#"
                        select catalog_name as "catalog_name!",
                               connector_image_name,
                               connector_image_tag,
                               reads_from,
                               writes_to,
                               spec,
                               spec_type as "spec_type: String"
                        from publications p
                        join live_specs ls on p.pub_id = ls.last_pub_id
                        where p.id = $1::flowid
                        order by ls.catalog_name;"#,
            publication_row_id as Id
        )
        .fetch_all(&self.pool)
        .await
        .unwrap();

        let result = sqlx::query!(
            r#"
            select job_status as "job_status: agent_sql::TextJson<publications::JobStatus>",
            draft_id as "draft_id: Id",
            pub_id as "pub_id: Id"
            from publications where id = $1"#,
            publication_row_id as Id
        )
        .fetch_one(&self.pool)
        .await
        .expect("failed to fetch publication");

        let errors = load_draft_errors(result.draft_id, &self.pool).await;

        ScenarioResult {
            publication_row_id,
            pub_id: result.pub_id,
            status: result.job_status.0,
            errors,
            live_specs: specs,
        }
    }

    pub async fn create_draft(
        &mut self,
        user_id: Uuid,
        detail: impl Into<String>,
        draft: tables::DraftCatalog,
    ) -> Id {
        use agent_sql::drafts as drafts_sql;
        let detail = detail.into();

        let mut txn = self
            .pool
            .begin()
            .await
            .expect("failed to start transaction");

        let draft_id = sqlx::query!(
            r#"insert into drafts (user_id, detail) values ($1, $2) returning id as "id: Id";"#,
            user_id,
            detail.as_str()
        )
        .fetch_one(&mut txn)
        .await
        .expect("failed to insert draft")
        .id;

        let tables::DraftCatalog {
            captures,
            collections,
            materializations,
            tests,
            ..
        } = draft;

        for row in captures {
            drafts_sql::upsert_spec(
                draft_id,
                row.capture.as_str(),
                row.model(),
                agent_sql::CatalogType::Capture,
                row.expect_pub_id.map(Into::into),
                &mut txn,
            )
            .await
            .unwrap();
        }
        for row in collections {
            drafts_sql::upsert_spec(
                draft_id,
                row.collection.as_str(),
                row.model(),
                agent_sql::CatalogType::Collection,
                row.expect_pub_id.map(Into::into),
                &mut txn,
            )
            .await
            .unwrap();
        }
        for row in materializations {
            drafts_sql::upsert_spec(
                draft_id,
                row.materialization.as_str(),
                row.model(),
                agent_sql::CatalogType::Materialization,
                row.expect_pub_id.map(Into::into),
                &mut txn,
            )
            .await
            .unwrap();
        }
        for row in tests {
            drafts_sql::upsert_spec(
                draft_id,
                row.test.as_str(),
                row.model(),
                agent_sql::CatalogType::Test,
                row.expect_pub_id.map(Into::into),
                &mut txn,
            )
            .await
            .unwrap();
        }
        txn.commit().await.expect("failed to commit txn");
        draft_id
    }

    pub async fn upsert_inferred_schema(&mut self, schema: tables::InferredSchema) {
        let tables::InferredSchema {
            collection_name,
            schema,
            ..
        } = schema;
        sqlx::query!(
            r#"insert into inferred_schemas (collection_name, schema, flow_document)
            values ($1, $2, '{}')
            on conflict(collection_name) do update set
            schema = excluded.schema;"#,
            collection_name.as_str() as &str,
            agent_sql::TextJson(schema) as agent_sql::TextJson<models::Schema>,
        )
        .execute(&self.pool)
        .await
        .expect("failed to update inferred schema");
    }

    pub async fn get_publication_specs(&mut self, catalog_name: &str) -> Vec<PublicationSpec> {
        sqlx::query_as!(
            PublicationSpec,
            r#"select
              ps.spec as "spec!: agent_sql::TextJson<models::RawValue>",
              ps.pub_id as "pub_id: Id",
              coalesce(ps.detail, '') as "detail!: String",
              ps.published_at as "published_at: DateTime<Utc>"
            from live_specs ls
            join publication_specs ps on ls.id = ps.live_spec_id
            where ls.catalog_name::text = $1
            order by ps.published_at;"#,
            catalog_name,
        )
        .fetch_all(&self.pool)
        .await
        .expect("failed to get publication specs")
    }

    pub async fn upsert_connector_status(
        &mut self,
        catalog_name: &str,
        status: models::status::ConnectorStatus,
    ) {
        sqlx::query!(
            r#"insert into connector_status (catalog_name, flow_document)
            values ($1, $2)
            on conflict (catalog_name) do update set flow_document = $2"#,
            catalog_name as &str,
            status as models::status::ConnectorStatus,
        )
        .execute(&self.pool)
        .await
        .expect("failed to upsert connector status");
    }

    pub async fn status_summary(&mut self, catalog_name: &str) -> models::status::Summary {
        let results =
            crate::api::public::status::fetch_status(&self.pool, &[catalog_name.to_string()], true)
                .await
                .expect("failed to fetch status for summary");
        assert_eq!(1, results.len(), "expected 1 status for '{catalog_name}'");
        let status = results.into_iter().next().unwrap();
        status.summary
    }
}

#[allow(unused)]
#[derive(Debug)]
pub struct PublicationSpec {
    pub pub_id: Id,
    pub published_at: DateTime<Utc>,
    pub detail: String,
    pub spec: agent_sql::TextJson<models::RawValue>,
}

/// Returns a simple json schema with properties named `p1,p2,pn`, up to `num_properties`.
pub fn mock_inferred_schema(
    collection_name: &str,
    generation_id: Id,
    num_properties: usize,
) -> tables::InferredSchema {
    let properties = (0..num_properties)
        .into_iter()
        .map(|i| (format!("p{i}"), serde_json::json!({"type": "string"})))
        .collect::<serde_json::Map<_, _>>();
    let schema: models::Schema = serde_json::from_value(serde_json::json!({
        "type": "object",
        "properties": properties,
        "x-collection-generation-id": generation_id.to_string(),
    }))
    .unwrap();
    let md5 = md5_hash(&schema);
    tables::InferredSchema {
        collection_name: models::Collection::new(collection_name),
        schema,
        md5,
    }
}

pub fn md5_hash<T: serde::Serialize>(val: &T) -> String {
    let s = serde_json::to_string(val).unwrap();
    let bytes = md5::compute(s);
    format!("{bytes:x}")
}

/// Returns a draft catalog for the given models::Catalog JSON.
pub fn draft_catalog(catalog_json: serde_json::Value) -> tables::DraftCatalog {
    let catalog: models::Catalog =
        serde_json::from_value(catalog_json).expect("failed to parse catalog");
    tables::DraftCatalog::from(catalog)
}

#[derive(Debug, serde::Serialize)]
pub struct Activation {
    pub catalog_name: String,
    pub catalog_type: CatalogType,
    pub built_spec: Option<AnyBuiltSpec>,
}

pub trait FailBuild: std::fmt::Debug + Send + 'static {
    fn modify(&mut self, result: &mut UncommittedBuild);
}

#[derive(Debug)]
pub struct InjectBuildError(Option<tables::Error>);
impl InjectBuildError {
    pub fn new(scope: url::Url, err: impl Into<anyhow::Error>) -> InjectBuildError {
        InjectBuildError(Some(tables::Error {
            scope,
            error: err.into(),
        }))
    }
}
impl FailBuild for InjectBuildError {
    fn modify(&mut self, result: &mut UncommittedBuild) {
        result.output.built.errors.insert(self.0.take().unwrap());
    }
}

struct ActivationStatus {
    shard_spec: proto_gazette::consumer::ShardSpec,
    statuses: Vec<replica_status::Code>,
}

impl ActivationStatus {}

struct ControlPlaneMocks {
    activations: Vec<Activation>,
    fail_activations: BTreeSet<String>,
    build_failures: InjectBuildFailures,
    shards: BTreeMap<String, ActivationStatus>,
}

/// A wrapper around `PGControlPlane` that has a few basic capbilities for verifying
/// activation calls and simulating failures of activations and publications.
#[derive(Clone)]
pub struct TestControlPlane {
    inner: PGControlPlane<MockDiscoverConnectors>,
    mocks: Arc<Mutex<ControlPlaneMocks>>,
}

/// A `Finalize` that can inject build failures in order to test failure scenarios.
/// `FailBuild`s are applied based on matching catalog names in the publication.
#[derive(Clone)]
struct InjectBuildFailures(Arc<Mutex<BTreeMap<String, VecDeque<Box<dyn FailBuild>>>>>);
impl crate::publications::FinalizeBuild for InjectBuildFailures {
    fn finalize(&self, build: &mut UncommittedBuild) -> anyhow::Result<()> {
        let mut build_failures = self.0.lock().unwrap();
        for (catalog_name, modifications) in build_failures.iter_mut() {
            if !build
                .output
                .built
                .all_spec_names()
                .any(|name| name == catalog_name.as_str())
            {
                continue;
            }
            if let Some(mut failure) = modifications.pop_front() {
                // log just to make it easier to debug tests
                tracing::info!(publication_id = %build.publication_id, %catalog_name, ?failure, "modifing test publication");
                failure.modify(build);
            }
        }
        Ok(())
    }
}

impl TestControlPlane {
    fn new(inner: PGControlPlane<MockDiscoverConnectors>) -> Self {
        Self {
            inner,
            mocks: Arc::new(Mutex::new(ControlPlaneMocks {
                activations: Vec::new(),
                fail_activations: BTreeSet::new(),
                build_failures: InjectBuildFailures(Arc::new(Mutex::new(BTreeMap::new()))),
                shards: BTreeMap::new(),
            })),
        }
    }

    pub fn reset_activations(&mut self) {
        let mut mocks = self.mocks.lock().unwrap();
        mocks.activations.clear();
        mocks.fail_activations.clear();
    }

    /// Cause all calls to activate the given catalog_name to fail until
    /// `reset_activations` is called.
    pub fn fail_next_activation(&mut self, catalog_name: &str) {
        let mut mocks = self.mocks.lock().unwrap();
        mocks.fail_activations.insert(catalog_name.to_string());
    }

    /// Asserts that there were calls to activate or delete all the given specs.
    /// If the catalog type is `Some`, then expect an activation, otherwise expect
    /// a deletion. Calls `reset_activations` at the end, assuming assertions pass.
    pub fn assert_activations(
        &mut self,
        desc: &str,
        mut expected: Vec<(&str, Option<CatalogType>)>,
    ) {
        let mocks = self.mocks.lock().unwrap();
        let mut actual = mocks
            .activations
            .iter()
            .map(|a| {
                (
                    a.catalog_name.as_str(),
                    if a.built_spec.is_some() {
                        Some(a.catalog_type)
                    } else {
                        None
                    },
                )
            })
            .collect::<Vec<_>>();
        actual.sort_by_key(|a| a.0);
        actual.dedup();
        expected.sort_by_key(|e| e.0);
        assert_eq!(
            expected, actual,
            "{desc} activations mismatch, expected:\n{expected:?}\nactual:\n{actual:?}\n"
        );
        std::mem::drop(mocks);
        self.reset_activations();
    }

    /// Cause the next publication that drafts `catalog_name` to fail, using the given
    /// `FailBuild` to inject errors into the build.
    pub fn fail_next_build<F>(&mut self, catalog_name: &str, modify: F)
    where
        F: FailBuild,
    {
        let mocks = self.mocks.lock().unwrap();
        let mut build_failures = mocks.build_failures.0.lock().unwrap();
        let modifications = build_failures.entry(catalog_name.to_string()).or_default();
        modifications.push_back(Box::new(modify));
    }

    pub fn mock_shard_status(&mut self, catalog_name: &str, statuses: Vec<replica_status::Code>) {
        let mut mocks = self.mocks.lock().unwrap();
        let Some(shards) = mocks.shards.get_mut(catalog_name) else {
            panic!("no shards found for catalog name: {catalog_name}");
        };
        shards.statuses = statuses;
    }
}

/// Returns the collection generation id, or panics if the spec is not for a
/// collection, or otherwise doesn't have a generation id.
pub fn get_collection_generation_id(state: &ControllerState) -> models::Id {
    let Some(proto_flow::AnyBuiltSpec::Collection(collection_spec)) = state.built_spec.as_ref()
    else {
        panic!("expected a collection spec, got: {:?}", state.built_spec);
    };
    let Some(template) = &collection_spec.partition_template else {
        panic!("missing collection partition template");
    };
    let id = assemble::extract_generation_id_suffix(&template.name);
    if id.is_zero() {
        panic!("expected a non-zero generation id");
    }
    id
}

#[async_trait::async_trait]
impl ControlPlane for TestControlPlane {
    #[tracing::instrument(level = "debug", err, skip(self))]
    async fn notify_dependents(&self, live_spec_id: models::Id) -> anyhow::Result<()> {
        self.inner.notify_dependents(live_spec_id).await
    }

    async fn get_config_updates(
        &self,
        catalog_name: String,
        build_id: Id,
    ) -> anyhow::Result<Option<ConfigUpdate>> {
        self.inner.get_config_updates(catalog_name, build_id).await
    }

    async fn delete_config_updates(
        &self,
        catalog_name: String,
        min_build: Id,
    ) -> anyhow::Result<()> {
        self.inner
            .delete_config_updates(catalog_name, min_build)
            .await
    }

    async fn get_shard_failures(&self, catalog_name: String) -> anyhow::Result<Vec<ShardFailure>> {
        self.inner.get_shard_failures(catalog_name).await
    }

    async fn delete_shard_failures(
        &self,
        catalog_name: String,
        min_build: Id,
        min_ts: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        self.inner
            .delete_shard_failures(catalog_name, min_build, min_ts)
            .await
    }

    async fn list_task_shards(
        &self,
        _data_plane_id: models::Id,
        _task_type: ops::TaskType,
        task_name: String,
    ) -> anyhow::Result<proto_gazette::consumer::ListResponse> {
        let mocks = self.mocks.lock().unwrap();

        let mut resp = proto_gazette::consumer::ListResponse::default();
        let Some(status) = mocks.shards.get(&task_name) else {
            // Return an empty response
            return Ok(resp);
        };

        for (i, code) in status.statuses.iter().enumerate() {
            let mut errors = Vec::new();
            if *code == replica_status::Code::Failed {
                errors.push(format!("mock error shard {i} failed"));
            }
            resp.shards
                .push(proto_gazette::consumer::list_response::Shard {
                    spec: Some(status.shard_spec.clone()),
                    mod_revision: 1,
                    // Nothing prevents us from mocking the `route` if we need to
                    route: None,
                    status: vec![ReplicaStatus {
                        code: *code as i32,
                        errors,
                    }],
                    create_revision: 1,
                });
        }
        Ok(resp)
    }

    async fn get_connector_spec(&self, image: String) -> anyhow::Result<ConnectorSpec> {
        self.inner.get_connector_spec(image).await
    }

    async fn get_live_specs(&self, names: BTreeSet<String>) -> anyhow::Result<tables::LiveCatalog> {
        self.inner.get_live_specs(names).await
    }

    fn current_time(&self) -> DateTime<Utc> {
        self.inner.current_time()
    }

    async fn evolve_collections(
        &self,
        draft: tables::DraftCatalog,
        collections: Vec<evolution::EvolveRequest>,
    ) -> anyhow::Result<evolution::EvolutionOutput> {
        self.inner.evolve_collections(draft, collections).await
    }

    async fn discover(
        &self,
        capture_name: models::Capture,
        draft: tables::DraftCatalog,
        update_only: bool,
        logs_token: Uuid,
        data_plane_id: models::Id,
    ) -> anyhow::Result<DiscoverOutput> {
        self.inner
            .discover(capture_name, draft, update_only, logs_token, data_plane_id)
            .await
    }

    async fn publish(
        &self,
        detail: Option<String>,
        logs_token: Uuid,
        draft: tables::DraftCatalog,
        data_plane_name: Option<String>,
    ) -> anyhow::Result<PublicationResult> {
        let finalize = {
            let mocks = self.mocks.lock().unwrap();
            mocks.build_failures.clone()
        };
        let publication = DraftPublication {
            user_id: self.inner.system_user_id,
            detail,
            draft,
            logs_token,
            dry_run: false,
            default_data_plane_name: data_plane_name,
            verify_user_authz: false,
            initialize: NoopInitialize,
            finalize,
            retry: DefaultRetryPolicy,
            with_commit: NoopWithCommit,
        };

        self.inner.publications_handler.publish(publication).await
    }

    async fn data_plane_activate(
        &self,
        catalog_name: String,
        spec: &AnyBuiltSpec,
        _data_plane_id: Id,
    ) -> anyhow::Result<()> {
        let mut mocks = self.mocks.lock().unwrap();
        if mocks.fail_activations.contains(&catalog_name) {
            anyhow::bail!("data_plane_delete simulated failure");
        }
        let (catalog_type, shard_template) = match spec {
            AnyBuiltSpec::Capture(b) => (CatalogType::Capture, b.shard_template.as_ref()),
            AnyBuiltSpec::Collection(b) => (
                CatalogType::Collection,
                b.derivation
                    .as_ref()
                    .and_then(|d| d.shard_template.as_ref()),
            ),
            AnyBuiltSpec::Materialization(b) => {
                (CatalogType::Materialization, b.shard_template.as_ref())
            }
            AnyBuiltSpec::Test(_) => panic!("unexpected catalog_type Test for data_plane_activate"),
        };

        if let Some(shard_spec) = shard_template {
            if let Some(existing) = mocks.shards.get_mut(&catalog_name) {
                existing.shard_spec = shard_spec.clone();
                for status in existing.statuses.iter_mut() {
                    *status = replica_status::Code::Primary;
                }
            } else {
                mocks.shards.insert(
                    catalog_name.clone(),
                    ActivationStatus {
                        shard_spec: shard_spec.clone(),
                        statuses: vec![proto_gazette::consumer::replica_status::Code::Primary],
                    },
                );
            }
        }

        mocks.activations.push(Activation {
            catalog_name,
            catalog_type,
            built_spec: Some(spec.clone()),
        });
        Ok(())
    }

    async fn data_plane_delete(
        &self,
        catalog_name: String,
        catalog_type: CatalogType,
        _data_plane_id: Id,
    ) -> anyhow::Result<()> {
        let mut mocks = self.mocks.lock().unwrap();
        if mocks.fail_activations.contains(&catalog_name) {
            anyhow::bail!("data_plane_delete simulated failure");
        }
        mocks.activations.push(Activation {
            catalog_name,
            catalog_type,
            built_spec: None,
        });
        Ok(())
    }
}

enum Either<L, R> {
    L(L),
    R(R),
}
