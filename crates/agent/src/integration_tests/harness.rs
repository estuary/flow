use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::{
    controllers::{ControllerHandler, ControllerState},
    controlplane::ConnectorSpec,
    publications::{self, PublicationResult, Publisher, UncommittedBuild},
    ControlPlane, HandleResult, Handler, PGControlPlane,
};
use agent_sql::{Capability, TextJson};
use chrono::{DateTime, Utc};
use models::CatalogType;
use proto_flow::AnyBuiltSpec;
use serde::Deserialize;
use serde_json::{value::RawValue, Value};
use sqlx::types::Uuid;
use tables::DraftRow;
use tempfile::tempdir;

const FIXED_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

#[derive(Debug, Deserialize)]
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
    pub publication_id: models::Id,
    pub status: publications::JobStatus,
    pub errors: Vec<(String, String)>,
    pub live_specs: Vec<LiveSpec>,
}

/// Facilitates writing integration tests.
/// **Note:** integration tests require exclusive access to the database,
/// so it's required to use the attribute: `#[serial_test::serial]` on every
/// test that uses a `TestHarness`. Initializing a new harness will clear out
/// (nearly) all data in the database, to ensure each test run starts with a
/// clean slate.
pub struct TestHarness {
    pub test_name: String,
    pub pool: sqlx::PgPool,
    pub publisher: Publisher,
    pub builds_root: tempfile::TempDir,
    pub controllers: ControllerHandler<TestControlPlane>,
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

        let publisher = Publisher::new(
            "/not/a/real/bin/dir",
            &url::Url::from_directory_path(builds_root.path()).unwrap(),
            "some-connector-network",
            &logs_tx,
            pool.clone(),
            id_gen.clone(),
        );

        let control_plane = PGControlPlane::new(
            pool.clone(),
            system_user_id,
            publisher.clone(),
            id_gen.clone(),
        );
        let controllers = ControllerHandler::new(TestControlPlane::new(control_plane));
        let mut harness = Self {
            test_name: test_name.to_string(),
            pool,
            publisher,
            controllers,
            builds_root,
        };
        harness.truncate_tables().await;
        harness.setup_test_connectors().await;
        harness
    }

    async fn setup_test_connectors(&mut self) {
        sqlx::query!(r##"
            with source_image as (
                insert into connectors (external_url, image_name, title, short_description, logo_url)
                values ('http://test.test/', 'source/test', '{"en-US": "test"}', '{"en-US": "test"}', '{"en-US": "http://test.test/"}')
                on conflict(image_name) do update set title = excluded.title
                returning id
            ),
            materialize_image as (
                insert into connectors (external_url, image_name, title, short_description, logo_url)
                values ('http://test.test/', 'materialize/test', '{"en-US": "test"}', '{"en-US": "test"}', '{"en-US": "http://test.test/"}')
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
                    '{"type": "object", "properties": {"id": {"type": "string", "x-collection-name": true}}}',
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
                    ops_l2_inferred_transform,
                    ops_l2_stats_transform,
                    broker_address,
                    reactor_address,
                    hmac_keys
                ) values (
                    'ops/dp/public/test',
                    'test.dp.estuary-data.com',
                    'ops/logs',
                    'ops/stats',
                    'ops/L1/inferred',
                    'ops/L1/stats',
                    'from-L1-inferred',
                    'from-L1-stats',
                    'broker:address',
                    'reactor:address',
                    '{secret-key}'
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
    }

    /// Returns a mutable reference to the control plane, which can be used for
    /// testing control plane operations or verifying results. See `TestControlPlane`
    /// comments for deets.
    pub fn control_plane(&mut self) -> &mut TestControlPlane {
        self.controllers.control_plane()
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

    pub async fn assert_live_spec_hard_deleted(&mut self, name: &str) {
        let rows = sqlx::query!(
            r#"select
            id as "id: agent_sql::Id",
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

    pub async fn assert_live_spec_soft_deleted(&mut self, name: &str, last_pub_id: models::Id) {
        let row = sqlx::query!(
            r#"
            select
                id as "id: agent_sql::Id",
                last_pub_id as "last_pub_id: agent_sql::Id",
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

    /// Returns a `ControllerState` representing the given live spec and
    /// controller status from the perspective of a controller.
    pub async fn get_controller_state(&mut self, name: &str) -> ControllerState {
        let job = sqlx::query_as!(
            agent_sql::controllers::ControllerJob,
            r#"select
                ls.id as "live_spec_id: agent_sql::Id",
                ls.catalog_name as "catalog_name!: String",
                ls.controller_next_run,
                ls.last_pub_id as "last_pub_id: agent_sql::Id",
                ls.spec as "live_spec: TextJson<Box<RawValue>>",
                ls.built_spec as "built_spec: TextJson<Box<RawValue>>",
                ls.spec_type as "spec_type: agent_sql::CatalogType",
                cj.controller_version as "controller_version: i32",
                cj.updated_at,
                cj.logs_token,
                cj.status as "status: TextJson<Box<RawValue>>",
                cj.failures,
                cj.error,
                ls.data_plane_id as "data_plane_id: agent_sql::Id"
            from live_specs ls
            join controller_jobs cj on ls.id = cj.live_spec_id
            where ls.catalog_name = $1;"#,
            name
        )
        .fetch_one(&self.pool)
        .await
        .expect("failed to query controller states");

        ControllerState::parse_db_row(&job).unwrap_or_else(|err| {
            panic!(
                "parsing controller jobs row {:?}, {err:?}",
                job.catalog_name
            )
        })
    }

    /// Runs a specific controller, which must already have a non-null `controller_next_run`,
    /// though it doesn't necessarily have to be the oldest `controller_next_run`.
    /// Returns the `ControllerState` as it was _before_ the controller ran.
    pub async fn run_pending_controller(&mut self, catalog_name: &str) -> ControllerState {
        // Set controller_next_run in the distant past to ensure that it doesn't race against
        // other tasks having controller_next_runs in the very recent past/present.
        sqlx::query!(
            r#"update live_specs
            set controller_next_run = '1999-01-01T01:01:01Z'::timestamptz
            where catalog_name = $1 and controller_next_run is not null
            returning 1 as "must_exist: bool";"#,
            catalog_name
        )
        .fetch_one(&self.pool)
        .await
        .expect("run_pending_controller fail");

        let runs = self.run_pending_controllers(Some(1)).await;
        runs.into_iter().next().unwrap()
    }

    /// Runs all controllers until there are no more that are ready. Optionally, `max` can limit the
    /// number of controller runs to perform, which may leave some controllers still pending. Returns
    /// the _starting_ controller state for each controller that was run.
    pub async fn run_pending_controllers(&mut self, max: Option<usize>) -> Vec<ControllerState> {
        let max = max.unwrap_or(usize::MAX);
        assert!(max > 0, "run_pending_controllers max must be > 0");
        let mut states = Vec::new();
        while let Some(state) = self
            .controllers
            .try_run_next(&self.pool)
            .await
            .expect("failed to run controller")
        {
            states.push(state);
            if states.len() == max {
                break;
            }
        }
        states
    }

    /// Performs a publication as if it were initiated by `flowctl` or the UI,
    /// and return a `ScenarioResult` describing the results.
    pub async fn user_publication(
        &mut self,
        user_id: Uuid,
        detail: impl Into<String>,
        draft: tables::DraftCatalog,
    ) -> ScenarioResult {
        self.async_publication(user_id, detail, draft, false, false)
            .await
    }

    pub async fn auto_discover_publication(
        &mut self,
        draft: tables::DraftCatalog,
        auto_evolve: bool,
    ) -> ScenarioResult {
        let system_user = self.control_plane().inner.system_user_id;
        self.async_publication(
            system_user,
            "test auto-discover publication",
            draft,
            auto_evolve,
            true,
        )
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
        draft: tables::DraftCatalog,
        auto_evolve: bool,
        background: bool,
    ) -> ScenarioResult {
        let detail = detail.into();
        let draft_id = self.create_draft(user_id, detail.clone(), draft).await;
        let mut txn = self
            .pool
            .begin()
            .await
            .expect("failed to start transaction");
        let pub_id = agent_sql::publications::create(
            &mut txn,
            user_id,
            draft_id,
            auto_evolve,
            detail.clone(),
            background,
            "ops/dp/public/test".to_string(),
        )
        .await
        .expect("failed to create publication");
        txn.commit().await.expect("failed to commit transaction");

        let pool = self.pool.clone();
        let handler_result = self
            .publisher
            .handle(&pool, true)
            .await
            .expect("publications handler failed");

        assert_eq!(
            HandleResult::HadJob,
            handler_result,
            "expected publications handler to have a job"
        );
        let pub_result = self.get_publication_result(pub_id.into()).await;
        assert_ne!(publications::JobStatus::Queued, pub_result.status);
        pub_result
    }

    async fn get_publication_result(&mut self, publication_id: models::Id) -> ScenarioResult {
        let pub_id: agent_sql::Id = publication_id.into();

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
                        from live_specs
                        where live_specs.last_pub_id = $1::flowid
                        order by live_specs.catalog_name;"#,
            pub_id as agent_sql::Id
        )
        .fetch_all(&self.pool)
        .await
        .unwrap();

        let result = sqlx::query!(
            r#"
            select job_status as "job_status: agent_sql::TextJson<publications::JobStatus>",
            draft_id as "draft_id: agent_sql::Id"
            from publications where id = $1"#,
            pub_id as agent_sql::Id
        )
        .fetch_one(&self.pool)
        .await
        .expect("failed to fetch publication");

        let errors = sqlx::query!(
            r#"select scope, detail from draft_errors where draft_id = $1;"#,
            result.draft_id as agent_sql::Id
        )
        .fetch_all(&self.pool)
        .await
        .unwrap()
        .into_iter()
        .map(|de| (de.scope, de.detail))
        .collect::<Vec<(String, String)>>();

        ScenarioResult {
            publication_id,
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
    ) -> agent_sql::Id {
        use agent_sql::drafts as drafts_sql;
        let detail = detail.into();

        let mut txn = self
            .pool
            .begin()
            .await
            .expect("failed to start transaction");

        let draft_id = sqlx::query!(
            r#"insert into drafts (user_id, detail) values ($1, $2) returning id as "id: agent_sql::Id";"#,
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
}

/// Returns a simple json schema with properties named `p1,p2,pn`, up to `num_properties`.
pub fn mock_inferred_schema(
    collection_name: &str,
    num_properties: usize,
) -> tables::InferredSchema {
    let properties = (0..num_properties)
        .into_iter()
        .map(|i| (format!("p{i}"), serde_json::json!({"type": "string"})))
        .collect::<serde_json::Map<_, _>>();
    let schema: models::Schema = serde_json::from_value(serde_json::json!({
        "type": "object",
        "properties": properties,
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

/// A wrapper around `PGControlPlane` that has a few basic capbilities for verifying
/// activation calls and simulating failures of activations and publications.
pub struct TestControlPlane {
    inner: PGControlPlane,
    activations: Vec<Activation>,
    fail_activations: BTreeSet<String>,
    build_failures: BTreeMap<String, VecDeque<Box<dyn FailBuild>>>,
}

impl TestControlPlane {
    fn new(inner: PGControlPlane) -> Self {
        Self {
            inner,
            activations: Vec::new(),
            fail_activations: BTreeSet::new(),
            build_failures: BTreeMap::new(),
        }
    }

    pub fn reset_activations(&mut self) {
        self.activations.clear();
        self.fail_activations.clear();
    }

    /// Cause all calls to activate the given catalog_name to fail until
    /// `reset_activations` is called.
    pub fn fail_next_activation(&mut self, catalog_name: &str) {
        self.fail_activations.insert(catalog_name.to_string());
    }

    /// Asserts that there were calls to activate or delete all the given specs.
    /// If the catalog type is `Some`, then expect an activation, otherwise expect
    /// a deletion. Calls `reset_activations` at the end, assuming assertions pass.
    pub fn assert_activations(
        &mut self,
        desc: &str,
        mut expected: Vec<(&str, Option<CatalogType>)>,
    ) {
        let mut actual = self
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
        self.reset_activations();
    }

    /// Cause the next publication that drafts `catalog_name` to fail, using the given
    /// `FailBuild` to inject errors into the build.
    pub fn fail_next_build<F>(&mut self, catalog_name: &str, modify: F)
    where
        F: FailBuild,
    {
        let modifications = self
            .build_failures
            .entry(catalog_name.to_string())
            .or_default();
        modifications.push_back(Box::new(modify));
    }
}

#[async_trait::async_trait]
impl ControlPlane for TestControlPlane {
    #[tracing::instrument(level = "debug", err, skip(self))]
    async fn notify_dependents(&mut self, catalog_name: String) -> anyhow::Result<()> {
        self.inner.notify_dependents(catalog_name).await
    }

    async fn get_connector_spec(&mut self, image: String) -> anyhow::Result<ConnectorSpec> {
        self.inner.get_connector_spec(image).await
    }

    async fn get_live_specs(
        &mut self,
        names: BTreeSet<String>,
    ) -> anyhow::Result<tables::LiveCatalog> {
        self.inner.get_live_specs(names).await
    }

    fn current_time(&self) -> DateTime<Utc> {
        self.inner.current_time()
    }

    /// Tests use a custom publish loop, so that failures can be injected into
    /// the build. This is admittedly a little gross, but at least it's pretty
    /// simple. And I'm hopeful that a better factoring of the `Publisher` will
    /// one day allow this to be replaced with something less bespoke.
    async fn publish(
        &mut self,
        publication_id: models::Id,
        detail: Option<String>,
        logs_token: Uuid,
        draft: tables::DraftCatalog,
    ) -> anyhow::Result<PublicationResult> {
        let mut result = self
            .inner
            .publications_handler
            .build(
                self.inner.system_user_id,
                publication_id,
                detail,
                draft,
                logs_token,
                "ops/dp/public/test",
            )
            .await?;

        for (catalog_name, modifications) in self.build_failures.iter_mut() {
            if !result
                .output
                .built
                .all_spec_names()
                .any(|name| name == catalog_name.as_str())
            {
                continue;
            }
            if let Some(mut failure) = modifications.pop_front() {
                // log just to make it easier to debug tests
                tracing::info!(%publication_id, %catalog_name, ?failure, "modifing test publication");
                failure.modify(&mut result);
            }
        }
        if result.has_errors() {
            Ok(result.build_failed())
        } else {
            self.inner.publications_handler.commit(result).await
        }
    }

    fn next_pub_id(&mut self) -> models::Id {
        self.inner.next_pub_id()
    }

    async fn data_plane_activate(
        &mut self,
        catalog_name: String,
        spec: &AnyBuiltSpec,
        _data_plane_id: models::Id,
    ) -> anyhow::Result<()> {
        if self.fail_activations.contains(&catalog_name) {
            anyhow::bail!("data_plane_delete simulated failure");
        }
        let catalog_type = match spec {
            AnyBuiltSpec::Capture(_) => CatalogType::Capture,
            AnyBuiltSpec::Collection(_) => CatalogType::Collection,
            AnyBuiltSpec::Materialization(_) => CatalogType::Materialization,
            AnyBuiltSpec::Test(_) => panic!("unexpected catalog_type Test for data_plane_activate"),
        };
        self.activations.push(Activation {
            catalog_name,
            catalog_type,
            built_spec: Some(spec.clone()),
        });
        Ok(())
    }

    async fn data_plane_delete(
        &mut self,
        catalog_name: String,
        catalog_type: CatalogType,
        _data_plane_id: models::Id,
    ) -> anyhow::Result<()> {
        if self.fail_activations.contains(&catalog_name) {
            anyhow::bail!("data_plane_delete simulated failure");
        }
        self.activations.push(Activation {
            catalog_name,
            catalog_type,
            built_spec: None,
        });
        Ok(())
    }
}
