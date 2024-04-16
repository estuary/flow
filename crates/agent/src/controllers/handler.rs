use agent_sql::controllers::{dequeue, upsert, ControllerJob};
use anyhow::Context;
use chrono::Utc;
use rand::Rng;
use serde_json::Value;

use crate::{HandleResult, Handler};

use super::{ControlJob, ControllerState, ControllerUpdate};

pub struct ControllerHandler;

#[async_trait::async_trait]
impl Handler for ControllerHandler {
    async fn handle(
        &mut self,
        pg_pool: &sqlx::PgPool,
        allow_background: bool,
    ) -> anyhow::Result<HandleResult> {
        let mut txn = pg_pool.begin().await?;

        let Some(job) = dequeue(&mut txn, allow_background).await? else {
            txn.rollback().await?;
            return Ok(HandleResult::NoJobs);
        };

        // TODO: move savepoint_noop to common module
        agent_sql::publications::savepoint_noop(&mut txn)
            .await
            .context("creating savepoint")?;

        let controller_result = run_controller(&job, &mut txn).await;
        match controller_result {
            Ok(ControllerUpdate {
                active,
                next_run,
                status,
            }) => {
                upsert(
                    &mut txn,
                    &job.catalog_name,
                    &job.controller,
                    next_run.map(|n| n.compute_time()),
                    active,
                    status,
                    0,
                    None,
                )
                .await?;
            }
            Err(error) => {
                agent_sql::publications::rollback_noop(&mut txn)
                    .await
                    .context("rolling back to savepoint")?;

                let failures = job.failures + 1;
                // next_run may be None, in which case the job will not be re-scheduled automatically.
                // Note that we leave the job as `active`. This means that manual publications of the task
                // may still trigger new runs of the controller, though continued failures will not be subject
                // to further retries until there's been at least one success.
                let next_run = backoff_next_run(failures);
                tracing::warn!(%failures, ?next_run, ?error, ?job, "controller job update failed");
                let err_str = error.to_string();
                upsert::<Value>(
                    &mut txn,
                    &job.catalog_name,
                    &job.controller,
                    next_run,
                    job.active,
                    None, // leave status unchanged
                    failures,
                    Some(err_str.as_str()),
                )
                .await?;
            }
        }
        txn.commit().await.context("committing transaction")?;
        Ok(HandleResult::HadJob)
    }

    fn table_name(&self) -> &'static str {
        "control_jobs"
    }
}

/// Applies a jittered backoff to determine the next time to retry the job. Jobs that have failed
/// 15 or mor times will not be retried. The backoff time increases quite a bit after the first
/// few failures, such that surpassing the maximum number of retries will take a minimum of 3+ days.
fn backoff_next_run(failures: i32) -> Option<chrono::DateTime<Utc>> {
    let base = if failures <= 3 {
        chrono::Duration::minutes(failures.max(1) as i64)
    } else if failures <= 15 {
        chrono::Duration::hours(failures as i64 - 3)
    } else {
        return None;
    };

    let max_jitter = (base.num_seconds() as f64 * 0.2) as i64;
    let add_secs = rand::thread_rng().gen_range(0..=max_jitter);
    let next = Utc::now() + (base + chrono::Duration::seconds(add_secs));
    Some(next)
}

async fn run_controller(
    job: &ControllerJob,
    _txn: &mut sqlx::Transaction<'static, sqlx::Postgres>,
) -> anyhow::Result<ControllerUpdate<Value>> {
    match job.controller.as_str() {
        "AutoDiscover" => Err(anyhow::anyhow!(
            "auto-discovers controller not yet implemented"
        )),
        "InferredSchema" => Err(anyhow::anyhow!(
            "inferred schema controller not yet implemented"
        )),
        other => Err(anyhow::anyhow!("unknown controller: {other:?}")),
    }
}

fn to_controller_state<C: ControlJob>(
    job: &ControllerJob,
) -> anyhow::Result<ControllerState<C::Status>> {
    let status =
        serde_json::from_str(job.status.get()).context("deserializing controller status")?;
    Ok(ControllerState {
        status,
        active: job.active,
        next_run: job.next_run,
        updated_at: job.updated_at,
        failures: job.failures,
    })
}
