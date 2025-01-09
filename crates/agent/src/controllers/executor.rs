//! The automations task `Executor` for live specs controllers
//!
//! This invokes the controller update logic and persists the outcomes.
//! Errors that are returned from the controller update functions are handled by
//! the executor, and are not considered errors of the automation task itself.
//! Any messages sent to the task will be considered "handled" in this case.
//! This allows controllers to have move complete controll over backoffs and
//! retries.
use std::collections::VecDeque;

use crate::{
    controllers::{fetch_controller_state, RetryableError},
    ControlPlane,
};
use anyhow::Context;
use automations::{Action, Executor, TaskType};
use models::{status::ControllerStatus, Id};
use serde::{Deserialize, Serialize};

use super::{controller_update, ControllerState, NextRun, CONTROLLER_VERSION};

pub struct LiveSpecControllerExecutor<C: ControlPlane> {
    control_plane: C,
}

impl<C: ControlPlane> LiveSpecControllerExecutor<C> {
    pub fn new(control_plane: C) -> Self {
        Self { control_plane }
    }
}

/// Messages that can be sent to a controller.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    /// A dependency of the controlled spec has been updated.
    DependencyUpdated,
    /// The controlled spec has just been published.
    SpecPublished {
        /// The ID of the publication that touched or modified the spec.
        pub_id: models::Id,
    },
    /// The inferred schema of the controlled collection spec has been updated.
    InferredSchemaUpdated,
    /// A request to trigger the controller manually. This is primarily used
    /// in tests to trigger the controller without waiting the `wake_at` time.
    ManualTrigger {
        /// The ID of the user who sent the message.
        user_id: uuid::Uuid,
    },
}

/// The state of the controller automation task stores infomation that's useful
/// for debugging, but isn't meant to be directly exposed to users.
#[derive(Serialize, Deserialize, Default)]
pub struct State {
    /// The total number of inbox messages that have been processed by the
    /// controller, either successfully or unsuccessfully.
    pub messages_processed: u64,
    /// Count of total controller update attempts that resulted in errors. These
    /// are not considered errors of the automation task itself, and instead
    /// result in an error status being recorded in the `controller_jobs` table.
    pub total_failures: u64,
    /// Count of total controller update attempts that were successful.
    pub total_successes: u64,
}

#[derive(Debug)]
pub struct Outcome {
    live_spec_id: models::Id,
    /// The next status of the controller.
    status: ControllerStatus,
    /// When to run the controller next. This will account for any backoff after errors.
    next_run: Option<NextRun>,
    /// Counts of _consecutive_ failures of the controller, which resets to 0 on
    /// any sucessful update.
    failures: i32,
    /// Rendered error message, if the controller failed.
    error: Option<String>,
    /// Whether the live spec has been deleted. If true, then the `live_specs`,
    /// `tasks`, and `controller_jobs` rows will be deleted after a successful
    /// controller run.
    live_spec_deleted: bool,
}

impl automations::Outcome for Outcome {
    async fn apply(self, txn: &mut sqlx::PgConnection) -> anyhow::Result<Action> {
        let Outcome {
            live_spec_id,
            status,
            next_run,
            failures,
            error,
            live_spec_deleted,
        } = self;

        if live_spec_deleted && error.is_none() {
            assert!(
                next_run.is_none(),
                "expected next_run to be None because live spec was deleted"
            );
            agent_sql::live_specs::hard_delete_live_spec(live_spec_id, txn)
                .await
                .context("deleting live_specs row")?;
            tracing::debug!(%live_spec_id, "completed controller task for deleted live spec");
            return Ok(Action::Done);
        }

        agent_sql::controllers::update_status(
            txn,
            live_spec_id,
            CONTROLLER_VERSION,
            &status,
            failures,
            error.as_deref(),
        )
        .await
        .context("updating controller status")?;

        let action = next_run
            .map(|n| Action::Sleep(n.compute_duration()))
            .unwrap_or(Action::Suspend);
        Ok(action)
    }
}

impl<C: ControlPlane + Send + Sync + 'static> Executor for LiveSpecControllerExecutor<C> {
    const TASK_TYPE: TaskType = automations::task_types::LIVE_SPEC_CONTROLLER;

    type Receive = Event;
    type State = State;
    type Outcome = Outcome;

    async fn poll<'s>(
        &'s self,
        pool: &'s sqlx::PgPool,
        task_id: models::Id,
        _parent_id: Option<models::Id>,
        state: &'s mut Self::State,
        inbox: &'s mut std::collections::VecDeque<(models::Id, Option<Self::Receive>)>,
    ) -> anyhow::Result<Self::Outcome> {
        let controller_state = fetch_controller_state(task_id, pool)
            .await?
            .unwrap_or_else(|| panic!("failed to fetch controller state for task_id: {task_id}"));
        // Note that `failures` here only counts the number of _consecutive_
        // failures, and resets to 0 on any sucessful update.
        let (status, failures, error, next_run) =
            run_controller(state, inbox, &controller_state, &self.control_plane).await;

        Ok(Outcome {
            live_spec_id: controller_state.live_spec_id,
            status,
            failures,
            error,
            next_run,
            live_spec_deleted: controller_state.live_spec.is_none(),
        })
    }
}

#[tracing::instrument(skip_all, fields(
    live_spec_id = %controller_state.live_spec_id,
    catalog_name = %controller_state.catalog_name
))]
async fn run_controller<C: ControlPlane>(
    task_state: &mut State,
    inbox: &mut VecDeque<(Id, Option<Event>)>,
    controller_state: &ControllerState,
    control_plane: &C,
) -> (ControllerStatus, i32, Option<String>, Option<NextRun>) {
    let mut next_status = controller_state.current_status.clone();
    tracing::debug!(?inbox, "inbox events");
    task_state.messages_processed += inbox.len() as u64;
    let result = controller_update(&mut next_status, controller_state, control_plane).await;
    let result_parts = match result {
        Ok(next) => {
            task_state.total_successes += 1;
            tracing::info!(next_run = ?next, inbox_len = inbox.len(), "successfully finished controller update");
            (next_status, 0, None, next)
        }
        Err(error) => {
            task_state.total_failures += 1;
            let failures = controller_state.failures + 1;
            // All errors are retryable unless explicitly marked as terminal
            let next_run = match error.downcast_ref::<RetryableError>() {
                Some(retryable) => retryable.retry,
                None => Some(fallback_backoff_next_run(failures)),
            };
            tracing::warn!(%failures, ?error, ?next_run, inbox_len = inbox.len(), "controller job update failed");

            let err_str = format!("{:#}", error);
            (next_status, failures, Some(err_str), next_run)
        }
    };
    inbox.clear();
    result_parts
}

fn fallback_backoff_next_run(failures: i32) -> NextRun {
    let minutes = match failures.max(1).min(8) as u32 {
        1 => 1,
        2 => 10,
        more => more * 45,
    };
    NextRun::after_minutes(minutes).with_jitter_percent(50)
}
