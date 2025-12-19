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
    ControlPlane,
    alerts::{AlertViewRow, evaluate_alert_actions},
    controllers::{RetryableError, fallback_backoff_next_run, fetch_controller_state},
};
use anyhow::Context;
use automations::{Action, Executor, TaskType};
use control_plane_api::{alerts, live_specs};
use models::{
    Id,
    status::{AlertType, ControllerStatus},
};
use serde::{Deserialize, Serialize};

use super::{CONTROLLER_VERSION, ControllerState, NextRun, controller_update};

#[derive(Clone)]
pub struct LiveSpecControllerExecutor<C: ControlPlane> {
    control_plane: std::sync::Arc<C>,
}

impl<C: ControlPlane> LiveSpecControllerExecutor<C> {
    pub fn new(control_plane: C) -> Self {
        Self {
            control_plane: std::sync::Arc::new(control_plane),
        }
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
    ShardFailed,
    ConfigUpdated,
}

pub type Inbox = VecDeque<(Id, Option<Event>)>;

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
    next_status: ControllerStatus,
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
    /// Changes to alert states as a result of this controller run
    alert_actions: Vec<alerts::AlertAction>,
}

impl automations::Outcome for Outcome {
    async fn apply(self, txn: &mut sqlx::PgConnection) -> anyhow::Result<Action> {
        let Outcome {
            live_spec_id,
            next_status: status,
            next_run,
            failures,
            mut error,
            live_spec_deleted,
            alert_actions,
        } = self;

        if live_spec_deleted && error.is_none() {
            // Do we need to delete the live spec? If `live_spec_id.is_zero()`,
            // it means that the `live_specs` row had _already_ been deleted
            // before this controller run began. That can happen due an edge
            // case where a message gets sent to this task's inbox during the
            // controller run that performs the hard deletion of the live spec.
            // In that case, returning `Action::Done` will not delete/remove the
            // task, so we'll need to try again.
            if live_spec_id.is_zero() {
                tracing::debug!(
                    "completing automations task for live spec that was already deleted"
                );
            } else {
                assert!(
                    next_run.is_none(),
                    "expected next_run to be None because live spec was deleted"
                );
                live_specs::hard_delete_live_spec(live_spec_id, txn)
                    .await
                    .context("deleting live_specs row")?;
                tracing::debug!(%live_spec_id, "completed controller task for deleted live spec");
            }
            return Ok(Action::Done);
        }

        // Guard against any null bytes in an error message, which would be disallowed by postgres.
        if error.as_ref().is_some_and(|e| e.contains('\0')) {
            error = error.map(|e| e.replace('\0', "\u{FFFD}")); // unicode replacement char
            tracing::warn!(%live_spec_id, ?error, "controller error contained null chars");
        }

        if let Err(error) = control_plane_api::controllers::update_status(
            txn,
            live_spec_id,
            CONTROLLER_VERSION,
            &status,
            failures,
            error.as_deref(),
        )
        .await
        {
            tracing::error!(%live_spec_id, ?error, new_controller_status = ?status, controller_error = ?error, "failed to update controller status");
            return Err(anyhow::Error::from(error)).context("failed to update controller status");
        }

        control_plane_api::alerts::apply_alert_actions(alert_actions, txn)
            .await
            .context("applying alert actions")?;

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
        let Some(controller_state) = fetch_controller_state(task_id, pool).await? else {
            tracing::info!(?task_id, ?inbox, "no controller state found for task");
            inbox.clear();
            return Ok(Outcome {
                live_spec_id: models::Id::zero(),
                live_spec_deleted: true,
                failures: 0,
                next_run: None,
                error: None,
                next_status: ControllerStatus::Uninitialized, // ignored
                alert_actions: Vec::default(),
            });
        };
        // Note that `failures` here only counts the number of _consecutive_
        // failures, and resets to 0 on any sucessful update.
        let (next_status, failures, error, next_run) = run_controller(
            state,
            inbox,
            task_id,
            &controller_state,
            &*self.control_plane,
        )
        .await;

        let alert_actions = {
            use rand::Rng;
            let id_gen_shard = rand::rng().random_range(1u16..1024u16);
            let mut id_gen = models::IdGenerator::new(id_gen_shard);
            let alert_status = next_status.alerts_status();
            evaluate_controller_alerts(
                controller_state.catalog_name.as_str(),
                alert_status,
                pool,
                &mut id_gen,
            )
            .await
            .context("evaluating controller alerts")?
        };

        Ok(Outcome {
            live_spec_id: controller_state.live_spec_id,
            next_status,
            failures,
            error,
            next_run,
            alert_actions,
            live_spec_deleted: controller_state.live_spec.is_none(),
        })
    }
}

async fn evaluate_controller_alerts(
    catalog_name: &str,
    alerts_status: Option<&models::status::Alerts>,
    pool: &sqlx::PgPool,
    id_gen: &mut models::IdGenerator,
) -> anyhow::Result<Vec<alerts::AlertAction>> {
    // Start by fetching all of the _controller-managed_ open alerts for this
    // task. Alert types with an associated view name are managed outside of
    // controllers.
    let controller_alert_types = models::status::AlertType::all()
        .into_iter()
        .filter(|ty| ty.view_name().is_none())
        .map(|ty| *ty)
        .collect::<std::collections::HashSet<AlertType>>();
    let open_alerts = alerts::fetch_open_alerts_by_catalog_name(&[catalog_name], pool)
        .await
        .context("querying for open alerts")?
        .into_iter()
        .filter(|alert| controller_alert_types.contains(&alert.alert_type))
        .collect::<Vec<_>>();

    let current_alerts = if let Some(status) = alerts_status {
        to_alert_view(catalog_name, status)?
    } else {
        Vec::new()
    };

    let eval_time = chrono::Utc::now();
    Ok(evaluate_alert_actions(
        eval_time,
        id_gen,
        open_alerts,
        current_alerts,
    ))
}

fn to_alert_view(
    catalog_name: &str,
    alert_status: &models::status::Alerts,
) -> anyhow::Result<Vec<AlertViewRow>> {
    let mut results = Vec::with_capacity(alert_status.len());
    for (key, status_alert) in alert_status.iter() {
        // Convert the controller's alert status into alert arguments, by detouring through a sj::Value.
        let args_val = serde_json::to_value(status_alert.clone())?;
        let arguments = serde_json::from_value::<alerts::ArgsObject>(args_val)?;
        let firing = status_alert.state == models::status::AlertState::Firing;
        results.push(AlertViewRow {
            catalog_name: catalog_name.to_string(),
            alert_type: *key,
            base_arguments: Some(sqlx::types::Json(arguments)),
            firing,
        });
    }
    Ok(results)
}

#[tracing::instrument(skip_all, fields(
    task_id = %_task_id,
    live_spec_id = %controller_state.live_spec_id,
    catalog_name = %controller_state.catalog_name,
    data_plane_id = %controller_state.data_plane_id,
    last_build_id = %controller_state.last_build_id
))]
async fn run_controller<C: ControlPlane>(
    task_state: &mut State,
    inbox: &mut VecDeque<(Id, Option<Event>)>,
    _task_id: Id,
    controller_state: &ControllerState,
    control_plane: &C,
) -> (ControllerStatus, i32, Option<String>, Option<NextRun>) {
    let mut next_status = controller_state.current_status.clone();
    tracing::debug!(?inbox, "inbox events");
    task_state.messages_processed += inbox.len() as u64;

    let result =
        controller_update(&mut next_status, controller_state, &*inbox, control_plane).await;
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
