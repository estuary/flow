use super::{logs, Handler, Id};

use agent_sql::publications::Row;
use anyhow::Context;
use serde::{Deserialize, Serialize};
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
    bindir: String,
    broker_address: url::Url,
    builds_root: url::Url,
    connector_network: String,
    consumer_address: url::Url,
    logs_tx: logs::Tx,
}

impl PublishHandler {
    pub fn new(
        bindir: &str,
        broker_address: &url::Url,
        builds_root: &url::Url,
        connector_network: &str,
        consumer_address: &url::Url,
        logs_tx: &logs::Tx,
    ) -> Self {
        Self {
            bindir: bindir.to_string(),
            broker_address: broker_address.clone(),
            builds_root: builds_root.clone(),
            connector_network: connector_network.to_string(),
            consumer_address: consumer_address.clone(),
            logs_tx: logs_tx.clone(),
        }
    }
}

#[async_trait::async_trait]
impl Handler for PublishHandler {
    async fn handle(&mut self, pg_pool: &sqlx::PgPool) -> anyhow::Result<std::time::Duration> {
        let mut txn = pg_pool.begin().await?;

        let row: Row = match agent_sql::publications::dequeue(&mut txn).await? {
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

        agent_sql::publications::resolve(id, &status, &mut txn).await?;
        txn.commit().await?;

        // As a separate transaction, delete the draft if it has no draft_specs.
        // The user could have raced an insertion of a new spec.
        if let (Some(delete_draft_id), JobStatus::Success) = (delete_draft_id, status) {
            agent_sql::publications::delete_draft(delete_draft_id, pg_pool).await?;
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
        agent_sql::publications::delete_draft_errors(row.draft_id, txn)
            .await
            .context("clearing old errors")?;

        // Create a savepoint "noop" we can roll back to.
        agent_sql::publications::savepoint_noop(txn)
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

        // Touch all expanded specifications to update their build ID.
        // TODO(johnny): This can potentially deadlock. We may eventually want
        // to catch this condition and gracefully roll-back the transaction to
        // allow it to be re-attempted. BUT I'm avoiding this extra code path
        // (and the potential for new bugs) until we actually see this in practice.
        // Current behavior is that the agent will crash and restart, and the
        // publication will then go on to retry as desired.
        agent_sql::publications::update_expanded_live_specs(
            &expanded_rows
                .iter()
                .map(|r| r.live_spec_id)
                .collect::<Vec<_>>(),
            row.pub_id,
            &mut *txn,
        )
        .await
        .context("updating build_id of expanded specifications")?;

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
            &self.builds_root,
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
            agent_sql::publications::rollback_noop(txn)
                .await
                .context("rolling back to savepoint")?;

            return Ok((row.pub_id, JobStatus::Success));
        }

        let errors = builds::deploy_build(
            &self.bindir,
            &self.broker_address,
            &self.connector_network,
            &self.consumer_address,
            &expanded_rows,
            row.logs_token,
            &self.logs_tx,
            row.pub_id,
            &spec_rows,
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
    agent_sql::publications::rollback_noop(txn)
        .await
        .context("rolling back to savepoint")?;

    specs::insert_errors(row.draft_id, errors, txn).await?;

    Ok((row.pub_id, job_status))
}
