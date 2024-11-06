use crate::{HandleResult, Handler};

use super::{evolve, Evolution, EvolveRequest, EvolvedCollection};
use agent_sql::evolutions::Row;
use anyhow::Context;
use itertools::Itertools;
use models::Id;
use serde::{Deserialize, Serialize};

pub struct EvolutionHandler;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum JobStatus {
    EvolutionFailed {
        error: String,
    },
    Success {
        evolved_collections: Vec<EvolvedCollection>,
        publication_id: Option<Id>,
    },
    Queued,
}

fn error_status(err: impl Into<String>) -> anyhow::Result<JobStatus> {
    Ok(JobStatus::EvolutionFailed { error: err.into() })
}

#[async_trait::async_trait]
impl Handler for EvolutionHandler {
    async fn handle(
        &mut self,
        pg_pool: &sqlx::PgPool,
        allow_background: bool,
    ) -> anyhow::Result<HandleResult> {
        loop {
            let mut txn = pg_pool.begin().await?;

            let Some(row) = agent_sql::evolutions::dequeue(&mut txn, allow_background).await?
            else {
                return Ok(HandleResult::NoJobs);
            };

            let time_queued = chrono::Utc::now().signed_duration_since(row.updated_at);
            let id: Id = row.id;
            let process_result = process_row(row, pg_pool, &mut txn).await;
            let job_status = match process_result {
                Ok(s) => s,
                Err(err) if crate::is_acquire_lock_error(&err) => {
                    tracing::info!(%id, %time_queued, "cannot acquire all row locks for evolution (will retry)");
                    // Since we failed to acquire a necessary row lock, wait a short
                    // while and then try again.
                    txn.rollback().await?;
                    // The sleep is really just so we don't spam the DB in a busy
                    // loop.  I arrived at these values via the very scientific 😉
                    // process of reproducing failures using a couple of different
                    // values and squinting at the logs in my terminal. In
                    // practice, it's common for another agent process to pick up
                    // the job while this one is sleeping, which is why I didn't
                    // see a need for jitter. All agents process the job queue in
                    // the same order, so the next time any agent polls the
                    // handler, it should get this same job, since we've released
                    // the lock on the job row. Evolutions jobs will fail _quite_
                    // quickly in this scenario, hence the full second.
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
                Err(other_err) => return Err(other_err),
            };

            let status = serde_json::to_value(job_status)?;
            tracing::info!(%id, %time_queued, %status, "evolution finished");
            agent_sql::evolutions::resolve(id, &status, &mut txn).await?;
            txn.commit().await?;

            return Ok(HandleResult::HadJob);
        }
    }

    fn table_name(&self) -> &'static str {
        "evolutions"
    }
}

pub async fn process_row(
    row: Row,
    db: &sqlx::PgPool,
    txn: &mut sqlx::Transaction<'static, sqlx::Postgres>,
) -> anyhow::Result<JobStatus> {
    let Row {
        draft_id,
        user_id,
        collections,
        ..
    } = row;
    let collections_requests: Vec<EvolveRequest> =
        serde_json::from_str(collections.get()).context("invalid 'collections' input")?;

    if collections_requests.is_empty() {
        return error_status("evolution collections parameter is empty");
    }

    let draft = crate::draft::load_draft(draft_id, db).await?;

    let evolution = Evolution {
        draft,
        requests: collections_requests,
        user_id,
        require_user_can_admin: true,
    };
    let output = evolve(evolution, db).await?;

    let job_status = if output.is_success() {
        agent_sql::drafts::delete_errors(draft_id, txn).await?;

        crate::draft::upsert_draft_catalog(draft_id, &output.draft, txn).await?;
        let delete_specs = output
            .actions
            .iter()
            .filter(|a| a.old_name != a.new_name)
            .map(|a| a.old_name.as_str())
            .collect::<Vec<_>>();
        if !delete_specs.is_empty() {
            agent_sql::drafts::delete_specs(draft_id, &delete_specs, txn).await?;
        }

        JobStatus::Success {
            evolved_collections: output.actions,
            publication_id: None,
        }
    } else {
        let errors = output
            .draft
            .errors
            .iter()
            .map(crate::draft::Error::from_tables_error)
            .collect::<Vec<_>>();
        crate::draft::insert_errors(draft_id, errors, txn).await?;
        let error = output.draft.errors.iter().map(|e| &e.error).join(", ");
        JobStatus::EvolutionFailed { error }
    };
    Ok(job_status)
}
