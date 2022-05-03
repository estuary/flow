use super::{logs, Handler, Id};

use anyhow::Context;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::types::{Json, Uuid};
use tracing::info;

mod builds;
mod specs;
mod storage;

#[derive(Debug, Default)]
pub struct Error {
    catalog_name: String,
    scope: Option<String>,
    detail: String,
}

/// JobStatus is the possible outcomes of a handled draft submission.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum JobStatus {
    Queued,
    BuildFailed,
    TestFailed,
    PublishFailed,
    Success,
}

/// A PublishHandler is a Handler which publishes catalog specifications.
pub struct PublishHandler {
    connector_network: String,
    bindir: String,
    logs_tx: logs::Tx,
    root: url::Url,
}

impl PublishHandler {
    pub fn new(connector_network: &str, bindir: &str, logs_tx: &logs::Tx, root: &url::Url) -> Self {
        Self {
            connector_network: connector_network.to_string(),
            bindir: bindir.to_string(),
            logs_tx: logs_tx.clone(),
            root: root.clone(),
        }
    }
}

// Row is the dequeued task shape of a draft build & test operation.
#[derive(Debug)]
struct Row {
    created_at: DateTime<Utc>,
    detail: Option<String>,
    draft_id: Id,
    dry_run: bool,
    logs_token: Uuid,
    pub_id: Id,
    updated_at: DateTime<Utc>,
    user_id: Uuid,
}

#[async_trait::async_trait]
impl Handler for PublishHandler {
    async fn handle(&mut self, pg_pool: &sqlx::PgPool) -> anyhow::Result<std::time::Duration> {
        let mut txn = pg_pool.begin().await?;

        let row: Row = match sqlx::query_as!(
            Row,
            r#"select
                created_at,
                detail,
                draft_id as "draft_id: Id",
                dry_run,
                logs_token,
                id as "pub_id: Id",
                updated_at,
                user_id
            from publications where job_status->>'type' = 'queued'
            order by id asc
            limit 1
            for update of publications skip locked;
            "#
        )
        .fetch_optional(&mut txn)
        .await?
        {
            None => return Ok(std::time::Duration::from_secs(5)),
            Some(row) => row,
        };

        let delete_draft_id = if !row.dry_run {
            Some(row.draft_id)
        } else {
            None
        };

        let (id, status) = self.process(row, &mut txn).await?;
        info!(%id, ?status, "finished");

        sqlx::query!(
            r#"update publications set
                    job_status = $2,
                    updated_at = clock_timestamp()
                where id = $1
                returning 1 as "must_exist";
            "#,
            id as Id,
            Json(&status) as Json<&JobStatus>,
        )
        .fetch_one(&mut txn)
        .await?;

        txn.commit().await?;

        // As a separate transaction, delete the draft if it has no draft_specs.
        // The user could have raced an insertion of a new spec.
        if let (Some(delete_draft_id), JobStatus::Success) = (delete_draft_id, status) {
            sqlx::query!(
                r#"
                delete from drafts where id = $1 and not exists
                    (select 1 from draft_specs where draft_id = $1)
                "#,
                delete_draft_id as Id,
            )
            .execute(pg_pool)
            .await?;
        }

        Ok(std::time::Duration::ZERO)
    }
}

impl PublishHandler {
    #[tracing::instrument(err, skip_all, fields(id=?row.pub_id))]
    async fn process(
        &mut self,
        row: Row,
        txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<(Id, JobStatus)> {
        info!(
            %row.created_at,
            %row.draft_id,
            %row.dry_run,
            %row.logs_token,
            %row.updated_at,
            %row.user_id,
            "processing publication",
        );

        // Remove draft errors from a previous publication attempt.
        sqlx::query!(
            "delete from draft_errors where draft_id = $1",
            row.draft_id as Id
        )
        .execute(&mut *txn)
        .await
        .context("clearing old errors")?;

        // Create a savepoint "noop" we can roll back to.
        sqlx::query!("savepoint noop;")
            .execute(&mut *txn)
            .await
            .context("creating savepoint")?;

        let spec_rows =
            specs::resolve_specifications(row.draft_id, row.pub_id, row.user_id, txn).await?;
        tracing::debug!(specs = %spec_rows.len(), "resolved specifications");

        let mut draft_catalog = models::Catalog::default();
        let mut live_catalog = models::Catalog::default();

        let errors = specs::extend_catalog(
            &mut live_catalog,
            spec_rows.iter().filter_map(|r| {
                r.live_type.map(|t| {
                    (
                        t,
                        r.catalog_name.as_str(),
                        r.live_spec.as_ref().unwrap().0.as_ref(),
                    )
                })
            }),
        );
        if !errors.is_empty() {
            anyhow::bail!("unexpected errors from live specs: {errors:?}");
        }

        let errors = specs::extend_catalog(
            &mut draft_catalog,
            spec_rows.iter().filter_map(|r| {
                r.draft_type.map(|t| {
                    (
                        t,
                        r.catalog_name.as_str(),
                        r.draft_spec.as_ref().unwrap().0.as_ref(),
                    )
                })
            }),
        );
        if !errors.is_empty() {
            return stop_with_errors(errors, JobStatus::BuildFailed, row, txn).await;
        }

        let errors =
            specs::validate_transition(&draft_catalog, &live_catalog, row.pub_id, &spec_rows);
        if !errors.is_empty() {
            return stop_with_errors(errors, JobStatus::BuildFailed, row, txn).await;
        }

        for spec_row in &spec_rows {
            specs::apply_updates_for_row(
                &draft_catalog,
                row.detail.as_ref(),
                row.pub_id,
                spec_row,
                row.user_id,
                &mut *txn,
            )
            .await
            .with_context(|| format!("applying spec updates for {}", spec_row.catalog_name))?;
        }

        let expanded_rows = specs::expanded_specifications(&spec_rows, txn).await?;
        tracing::debug!(specs = %expanded_rows.len(), "resolved expanded specifications");

        let errors = specs::extend_catalog(
            &mut draft_catalog,
            expanded_rows
                .iter()
                .map(|r| (r.live_type, r.catalog_name.as_str(), r.live_spec.0.as_ref())),
        );
        if !errors.is_empty() {
            anyhow::bail!("unexpected errors from expanded specs: {errors:?}");
        }

        let errors = storage::inject_mappings(
            spec_rows
                .iter()
                .map(|r| r.catalog_name.as_ref())
                .chain(expanded_rows.iter().map(|r| r.catalog_name.as_ref())),
            &mut draft_catalog,
            txn,
        )
        .await?;
        if !errors.is_empty() {
            return stop_with_errors(errors, JobStatus::BuildFailed, row, txn).await;
        }

        let tmpdir = tempfile::TempDir::new().context("creating tempdir")?;
        let tmpdir = tmpdir.path();

        let errors = builds::build_catalog(
            &self.root,
            &draft_catalog,
            &self.connector_network,
            &self.bindir,
            row.logs_token,
            &self.logs_tx,
            row.pub_id,
            tmpdir,
        )
        .await?;
        if !errors.is_empty() {
            return stop_with_errors(errors, JobStatus::BuildFailed, row, txn).await;
        }

        let data_plane_job = builds::data_plane(
            &self.connector_network,
            &self.bindir,
            row.logs_token,
            &self.logs_tx,
            tmpdir,
        );
        let test_jobs = builds::test_catalog(
            &self.connector_network,
            &self.bindir,
            row.logs_token,
            &self.logs_tx,
            row.pub_id,
            tmpdir,
        );

        // Drive the data-plane and test jobs, until test jobs complete.
        tokio::pin!(test_jobs);
        let errors: Vec<Error> = tokio::select! {
            r = data_plane_job => {
                tracing::error!(?r, "test data-plane exited unexpectedly");
                test_jobs.await // Wait for test jobs to finish.
            }
            r = &mut test_jobs => r,
        }?;

        if !errors.is_empty() {
            return stop_with_errors(errors, JobStatus::TestFailed, row, txn).await;
        }

        if row.dry_run {
            sqlx::query!("rollback transaction to noop;")
                .execute(&mut *txn)
                .await
                .context("rolling back to savepoint")?;

            return Ok((row.pub_id, JobStatus::Success));
        }

        let errors = builds::deploy_build(
            &spec_rows,
            &expanded_rows,
            &self.connector_network,
            &self.bindir,
            row.logs_token,
            &self.logs_tx,
            row.pub_id,
        )
        .await
        .context("deploying build")?;

        if !errors.is_empty() {
            return stop_with_errors(errors, JobStatus::PublishFailed, row, txn).await;
        }

        Ok((row.pub_id, JobStatus::Success))
    }
}

async fn stop_with_errors(
    errors: Vec<Error>,
    job_status: JobStatus,
    row: Row,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<(Id, JobStatus)> {
    sqlx::query!("rollback transaction to noop;")
        .execute(&mut *txn)
        .await
        .context("rolling back to savepoint")?;

    specs::insert_errors(row.draft_id, errors, txn).await?;

    Ok((row.pub_id, job_status))
}
