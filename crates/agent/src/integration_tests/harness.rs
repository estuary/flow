use std::collections::BTreeSet;

use crate::{
    controllers::{ControllerHandler, ControllerState, NextRun},
    publications::{self, PublicationResult, Publisher},
    ControlPlane, HandleResult, Handler, PGControlPlane,
};
use agent_sql::TextJson;
use serde_json::value::RawValue;
use sqlx::types::Uuid;
use tables::DraftRow;
use tempfile::tempdir;

const FIXED_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

#[derive(Debug)]
pub struct UserPublication {
    pub publication_id: models::Id,
    pub user_id: Uuid,
    pub detail: String,
    pub job_status: publications::JobStatus,
}

pub struct TestHarness {
    pub test_name: String,
    pub pool: sqlx::PgPool,
    pub publisher: Publisher,
    pub control_plane: PGControlPlane,
    pub builds_root: tempfile::TempDir,
    pub controllers: ControllerHandler,
}

impl TestHarness {
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

        // TODO: shouldn't need to specify a bindir, or broker/consumer address
        let publisher = Publisher::new(
            "support@estuary.dev",
            false,
            true, // enable test mode to noop validations and activations
            "../../../.build/package/bin",
            &url::Url::parse("http://not-used.test/").unwrap(),
            &url::Url::from_directory_path(builds_root.path()).unwrap(),
            "some-connector-network",
            &url::Url::parse("http://not-used.test/").unwrap(),
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

        let controllers = ControllerHandler::new(control_plane.clone());
        let harness = Self {
            test_name: test_name.to_string(),
            pool,
            publisher,
            control_plane,
            controllers,
            builds_root,
        };
        harness.truncate_tables().await;
        harness
    }

    async fn truncate_tables(&self) {
        tracing::warn!("clearing all data before test");
        let system_user_id = self.control_plane.system_user_id;
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
            .control_plane
            .get_live_specs(name_set)
            .await
            .expect("get_live_specs failed");
        assert_eq!(0, live_specs.spec_count());
    }

    pub async fn fast_forward_next_controllers(&mut self) -> Vec<String> {
        // TODO: handle the case where there are some controllers that are still on version 0
        let names = sqlx::query!(
            r#"
            update live_specs
            set controller_next_run = now()
            where controller_next_run is not null
                and controller_next_run = (
                    select controller_next_run
                    from live_specs
                    where controller_next_run is not null
                    order by controller_next_run asc
                    limit 1
                )
            returning catalog_name as "name: String";"#
        )
        .fetch_all(&self.pool)
        .await
        .expect("failed to query next controller");

        let mut result = names.into_iter().map(|r| r.name).collect::<Vec<_>>();
        result.sort();

        tracing::info!(next_controller_names = ?result, "fast-forwarded controllers");
        result
    }

    pub async fn get_controller_state(&mut self, name: &str) -> ControllerState {
        let job = sqlx::query_as!(
            agent_sql::controllers::ControllerJob,
            r#"select
                ls.id as "live_spec_id: agent_sql::Id",
                ls.catalog_name as "catalog_name!: String",
                ls.controller_next_run,
                ls.last_pub_id as "last_pub_id: agent_sql::Id",
                ls.spec as "live_spec: TextJson<Box<RawValue>>",
                ls.spec_type as "spec_type!: agent_sql::CatalogType",
                cj.controller_version as "controller_version: i32",
                cj.updated_at,
                cj.logs_token,
                cj.status as "status: TextJson<Box<RawValue>>",
                cj.failures,
                cj.error
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

    // TODO: can maybe remove `run_next_controllers`?
    /// Runs the next set of pending controllers, even if their `controller_next_run` value is in
    /// the future.
    pub async fn run_next_controllers(&mut self) -> Vec<ControllerState> {
        let names = self.fast_forward_next_controllers().await;
        if names.is_empty() {
            return Vec::new();
        }
        tracing::info!(expected_controllers = ?names, "running controllers");
        // Pass a max number of controllers to avoid race conditions where a controller that's not
        // due right now becomes due before the `run_pending_controllers` loop terminates.
        let runs = self.run_pending_controllers(Some(names.len())).await;
        let run_names = runs
            .iter()
            .map(|s| s.catalog_name.as_str())
            .collect::<BTreeSet<_>>();
        let expected_names = names.iter().map(|n| n.as_str()).collect::<BTreeSet<_>>();

        assert_eq!(
            expected_names, run_names,
            "expected controllers did not run"
        );
        //self.controller_states(names).await
        todo!()
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

    pub async fn user_publication(
        &mut self,
        user_id: Uuid,
        detail: impl Into<String>,
        draft: tables::DraftCatalog,
    ) -> UserPublication {
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
            false,
            detail.clone(),
            false,
        )
        .await
        .expect("failed to create publication");
        txn.commit().await.expect("failed to commit transaction");

        let handler_result = self
            .publisher
            .handle(&self.pool, false)
            .await
            .expect("publications handler failed");
        assert_eq!(
            HandleResult::HadJob,
            handler_result,
            "expected publications handler to have a job"
        );

        let result = sqlx::query!(
            r#"
            select job_status as "job_status: agent_sql::TextJson<publications::JobStatus>"
            from publications where id = $1"#,
            pub_id as agent_sql::Id
        )
        .fetch_one(&self.pool)
        .await
        .expect("failed to fetch publication");

        UserPublication {
            publication_id: pub_id.into(),
            user_id,
            detail,
            job_status: result.job_status.0,
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

pub fn draft_catalog(catalog_json: serde_json::Value) -> tables::DraftCatalog {
    let catalog: models::Catalog =
        serde_json::from_value(catalog_json).expect("failed to parse catalog");
    tables::DraftCatalog::from(catalog)
}
