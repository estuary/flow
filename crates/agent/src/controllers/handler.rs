use agent_sql::controllers::{dequeue, update};
use anyhow::Context;

use crate::{
    controlplane::{ControlPlane, PGControlPlane},
    DataPlaneConnectors, HandleResult, Handler,
};

use super::{controller_update, ControllerState, NextRun, RetryableError, Status};

use crate::controllers::CONTROLLER_VERSION;

pub struct ControllerHandler<C: ControlPlane = PGControlPlane<DataPlaneConnectors>> {
    control_plane: C,
}

impl<C: ControlPlane> ControllerHandler<C> {
    pub fn new(control_plane: C) -> Self {
        Self { control_plane }
    }

    #[cfg(test)]
    pub fn control_plane(&mut self) -> &mut C {
        &mut self.control_plane
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
                // All errors are retryable unless explicitly marked as terminal
                let next_run = match error.downcast_ref::<RetryableError>() {
                    Some(retryable) => retryable.retry.map(|next| next.compute_time()),
                    None => Some(fallback_backoff_next_run(failures).compute_time()),
                };
                tracing::warn!(%failures, ?error, ?job, ?next_run, "controller job update failed");
                let err_str = format!("{:#}", error);
                agent_sql::controllers::update(
                    &mut txn,
                    job.live_spec_id,
                    job.controller_version, // Don't update the controller version if the job failed
                    next_status,            // A failure may still change the status
                    failures,
                    Some(err_str.as_str()),
                    job.controller_next_run, // See comment on `upsert` about optimistic locking
                    next_run,
                )
                .await?;
            }
        }
        txn.commit().await.context("committing transaction")?;
        Ok(Some(state))
    }
}

fn fallback_backoff_next_run(failures: i32) -> NextRun {
    let minutes = match failures.max(1).min(8) as u32 {
        1 => 1,
        2 => 10,
        more => more * 45,
    };
    NextRun::after_minutes(minutes).with_jitter_percent(50)
}

#[async_trait::async_trait]
impl Handler for ControllerHandler {
    async fn handle(
        &mut self,
        pg_pool: &sqlx::PgPool,
        _allow_background: bool,
    ) -> anyhow::Result<HandleResult> {
        let prev_state = self.try_run_next(pg_pool).await?;
        let result = if prev_state.is_some() {
            HandleResult::HadJob
        } else {
            HandleResult::NoJobs
        };
        Ok(result)
    }

    fn table_name(&self) -> &'static str {
        "controller_jobs"
    }
}

#[tracing::instrument(err(level = tracing::Level::WARN), skip(state, next_status, control_plane), fields(
    catalog_name = %state.catalog_name,
    enqueued_at = ?state.next_run,
    last_update = %state.controller_updated_at,
    last_pub_id = %state.last_pub_id,
    last_build_id = %state.last_build_id,
    data_plane_id = %state.data_plane_id,
))]
async fn run_controller<C: ControlPlane>(
    state: &ControllerState,
    next_status: &mut Status,
    control_plane: &mut C,
) -> anyhow::Result<Option<NextRun>> {
    controller_update(next_status, &state, control_plane).await
}
