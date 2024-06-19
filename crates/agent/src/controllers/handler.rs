use agent_sql::controllers::{dequeue, update};
use anyhow::Context;
use chrono::{DateTime, Utc};

use crate::{
    controlplane::{ControlPlane, PGControlPlane},
    HandleResult, Handler,
};

use super::{ControllerState, NextRun, RetryableError, Status};

use crate::controllers::CONTROLLER_VERSION;

pub struct ControllerHandler<C: ControlPlane = PGControlPlane> {
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
                // All errors are considered terminal unless the controller
                // specifically opts into retrying them (by wrapping them in a
                // `ControllerError`).
                let next_run = error
                    .downcast_ref::<RetryableError>()
                    .and_then(|ce| ce.retry)
                    .map(|next| next.compute_time());
                tracing::warn!(%failures, ?error, ?job, ?next_run, "controller job update failed with a terminal error");
                let err_str = format!("{:?}", error);
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
        "controller_jobs"
    }
}

#[tracing::instrument(err(level = tracing::Level::WARN), skip(state, next_status, control_plane), fields(
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
