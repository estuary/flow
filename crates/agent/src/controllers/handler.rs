use agent_sql::controllers::{dequeue, update, ControllerJob};
use anyhow::Context;
use chrono::{DateTime, Utc};
use rand::Rng;

use crate::{
    controlplane::{ControlPlane, PGControlPlane},
    HandleResult, Handler,
};

use super::{ControllerState, NextRun, Status};

use crate::controllers::CONTROLLER_VERSION;

pub struct ControllerHandler {
    control_plane: PGControlPlane,
}

impl ControllerHandler {
    pub fn new(control_plane: PGControlPlane) -> Self {
        Self { control_plane }
    }

    pub async fn try_run_next(
        &mut self,
        pg_pool: &sqlx::PgPool,
    ) -> anyhow::Result<Option<ControllerState>> {
        let mut txn = pg_pool.begin().await?;

        let Some(job) = dequeue(&mut txn, CONTROLLER_VERSION).await? else {
            txn.rollback().await?;
            return Ok(None);
        };

        // TODO: move savepoint_noop to common module
        // TODO TODO: do we even need a savepoint/rollback here?
        agent_sql::publications::savepoint_noop(&mut txn)
            .await
            .context("creating savepoint")?;

        let state = ControllerState::parse_db_row(&job)
            .with_context(|| format!("building controller state for '{}'", job.catalog_name))?;
        let mut next_status = state.current_status.clone();
        let controller_result =
            run_controller(&state, &mut next_status, &mut self.control_plane).await;
        match controller_result {
            Ok(next_run) if state.live_spec.is_some() => {
                update(
                    &mut txn,
                    job.live_spec_id,
                    CONTROLLER_VERSION,
                    next_status,
                    0,                       // zero out failures beause we succeeded
                    None,                    // no error
                    job.controller_next_run, // See comment on `upsert` about optimistic locking
                    next_run.as_ref().map(NextRun::compute_time),
                )
                .await?;
            }
            Ok(next_run) => {
                assert!(
                    next_run.is_none(),
                    "expected next_run to be None because live spec was deleted"
                );
                assert!(state.live_spec.is_none());
                // The live spec has been deleted, and the controller ran successfully, so we can now delete the controller job row.
                agent_sql::live_specs::hard_delete_live_spec(job.live_spec_id, &mut txn)
                    .await
                    .context("deleting live_specs row")?;
            }
            Err(error) => {
                let failures = job.failures + 1;
                // next_run may be None, in which case the job will not be re-scheduled automatically.
                // Note that we leave the job as `active`. This means that manual publications of the task
                // may still trigger new runs of the controller, though continued failures will not be subject
                // to further retries until there's been at least one success.
                let next_run = backoff_next_run(failures);
                tracing::warn!(%failures, ?next_run, ?error, ?job, "controller job update failed");
                let err_str = error.to_string();
                agent_sql::controllers::update(
                    &mut txn,
                    job.live_spec_id,
                    job.controller_version, // Don't update the controller version if the job failed
                    next_status,            // A failure may still change the status
                    failures,
                    Some(err_str.as_str()),
                    job.controller_next_run, // See comment on `upsert` about optimistic locking
                    Some(next_run),
                )
                .await?;
            }
        }
        txn.commit().await.context("committing transaction")?;
        Ok(Some(state))
    }
}

#[async_trait::async_trait]
impl Handler for ControllerHandler {
    async fn handle(
        &mut self,
        pg_pool: &sqlx::PgPool,
        allow_background: bool,
    ) -> anyhow::Result<HandleResult> {
        if !allow_background {
            return Ok(HandleResult::NoJobs);
        }
        let prev_state = self.try_run_next(pg_pool).await?;
        let result = if prev_state.is_some() {
            HandleResult::HadJob
        } else {
            HandleResult::NoJobs
        };
        Ok(result)
    }

    fn table_name(&self) -> &'static str {
        "control_jobs"
    }
}

/// Applies a jittered backoff to determine the next time to retry the job.
fn backoff_next_run(failures: i32) -> DateTime<Utc> {
    let failures = failures.max(1).min(8) as i64;
    let multiplier: i64 = if failures <= 3 {
        60 // a minute per failure
    } else {
        3600 // an hour per failure
    };
    let total = failures * multiplier;

    let max_jitter = (total as f64 * 0.2) as i64;
    let add_secs = rand::thread_rng().gen_range(0..=max_jitter);
    // We use `from_timestamp` because it's guaranteed to round-trip through a `timestamptz` column.
    // See: https://docs.rs/sqlx/latest/sqlx/types/chrono/struct.DateTime.html#method.from_timestamp
    DateTime::<Utc>::from_timestamp(Utc::now().timestamp() + total + add_secs, 0)
        .expect("from_timestamp cannot fail because subsecond nanos is 0")
}

#[tracing::instrument(err, skip(state, next_status, control_plane), fields(
    catalog_name = %state.catalog_name,
    enqueued_at = ?state.next_run,
    last_update = %state.updated_at,
    last_pub_id = %state.last_pub_id))]
async fn run_controller<C: ControlPlane>(
    state: &ControllerState,
    next_status: &mut Status,
    control_plane: &mut C,
) -> anyhow::Result<Option<NextRun>> {
    next_status.update(&state, control_plane).await
}
