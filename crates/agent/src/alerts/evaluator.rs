use std::collections::{BTreeMap, HashMap};

use anyhow::Context;
use automations::{Action, Executor, TaskType, task_types};
use chrono::{DateTime, Utc};
use control_plane_api::alerts::{
    Alert, AlertAction, ArgsObject, FireAlert, ResolveAlert, apply_alert_actions,
    fetch_open_alerts_by_type,
};
use models::status::AlertType;
use sqlx::types::Json;

/// Create an `automations::Executor` for evaluating tenant alerts (`free_trial`, etc).
pub fn new_tenant_alerts_executor(
    evaluation_interval: std::time::Duration,
) -> AlertEvaluator<TenantAlerts> {
    AlertEvaluator {
        view: TenantAlerts,
        id_gen: crate::id_generator::tenant_alerts,
        evaluation_interval,
    }
}

/// Create an `automations::Executor` for evaluating `data_movement_stalled` alerts.
pub fn new_data_movement_alerts_executor(
    evaluation_interval: std::time::Duration,
) -> AlertEvaluator<DataMovementStalledAlerts> {
    AlertEvaluator {
        view: DataMovementStalledAlerts,
        id_gen: crate::id_generator::data_movement_stalled_alerts,
        evaluation_interval,
    }
}

/// This function inspects an alert view and compares it against the given
/// array of `open_alerts`, returning a list of alert state transitions.
/// Alerts only have two possible state transitions:
///
/// - Alerts that `current_alert_state` shows as firing, but are _not_
///   already included in `open_alerts` will result in an `AlertAction::Fire`.
/// - Alerts that are in `open_alerts`, but which are _not_ shown to be firing
///   in `current_alert_state`, will result in `AlertAction::Resolve`.
///
/// Normally, it's sufficient for the `current_alert_state` to simply omit any
/// alerts that have resolved. But it's also possible to include the alert
/// with `firing: false`, in which case any `base_arguments` will become the
/// `resolved_arguments` in alert history.
pub fn evaluate_alert_actions(
    eval_timestamp: DateTime<Utc>,
    id_gen: &mut models::IdGenerator,
    open_alerts: Vec<Alert>,
    current_alert_state: Vec<AlertViewRow>,
) -> Vec<AlertAction> {
    let mut indexed_open: HashMap<(String, AlertType), (models::Id, DateTime<Utc>, ArgsObject)> =
        open_alerts
            .into_iter()
            .map(|alert| {
                let Alert {
                    id,
                    catalog_name,
                    alert_type,
                    fired_at,
                    resolved_at: _,
                    arguments,
                    resolved_arguments: _,
                } = alert;

                let key = (catalog_name, alert_type);
                let value = (id, fired_at, arguments.0);
                (key, value)
            })
            .collect();

    let mut actions = Vec::new();
    for view_alert in current_alert_state {
        let AlertViewRow {
            catalog_name,
            alert_type,
            base_arguments: arguments,
            firing,
        } = view_alert;
        let key = (catalog_name, alert_type);

        if let Some((_open_key, (id, _fired_at, _open_args))) = indexed_open.remove_entry(&key) {
            if !firing {
                // The alert is currently open, but the view now shows it as resolved
                let (catalog_name, alert_type) = key;
                actions.push(AlertAction::Resolve(ResolveAlert {
                    id,
                    catalog_name,
                    alert_type,
                    resolved_at: eval_timestamp,
                    base_resolved_arguments: arguments.map(|j| j.0),
                }));
            }
        } else {
            // The alert is not currently open, so if the view shows it as
            // firing, then we need to fire the alert now.
            if firing {
                let (catalog_name, alert_type) = key;
                let id = id_gen.next();
                actions.push(AlertAction::Fire(FireAlert {
                    id,
                    catalog_name,
                    alert_type,
                    fired_at: eval_timestamp,
                    base_arguments: arguments.map(|j| j.0).unwrap_or_default(),
                }));
            }
        }
    }

    // Any open alerts remaining in `indexed_open` can now be resolved, since they no
    // longer appear in the corresponding alert view.
    for ((catalog_name, alert_type), (id, _fired_at, _)) in indexed_open {
        actions.push(AlertAction::Resolve(ResolveAlert {
            id,
            catalog_name,
            alert_type,
            resolved_at: eval_timestamp,
            base_resolved_arguments: None,
        }));
    }
    actions
}

/// A possible alert, which may or may not be firing.
pub struct AlertViewRow {
    pub catalog_name: String,
    pub alert_type: AlertType,
    /// Optional arguments for the alert, _not_ including any recipients. If an
    /// alert view row with `arguments: None` triggers the firing of an alert,
    /// then the effective arguments will be an empty object. The `recipients`
    /// will be added to these arguments automatically if this alert is firing
    /// or resolving, overwriting any previous value.
    pub base_arguments: Option<sqlx::types::Json<ArgsObject>>,
    /// Whether the alert should currently be firing. Normally, it's sufficient
    /// for a view to simply not include a previously firing alert in order for
    /// it to be resolved. But certain alert types may wish to provide a
    /// different set of arguments for the alert resolution, and they can do so
    /// by including a row with `firing: false` and providing `arguments`.
    pub firing: bool,
}

/// Represents a source of possible alerts
pub trait AlertView: std::fmt::Debug + Clone + Send + Sync + 'static {
    fn alert_types(&self) -> Vec<AlertType>;

    fn query<'a>(
        &'a self,
        db: &'a sqlx::PgPool,
    ) -> impl Future<Output = anyhow::Result<Vec<AlertViewRow>>> + 'a;
}

#[derive(Debug, Clone)]
pub struct TenantAlerts;
impl AlertView for TenantAlerts {
    fn alert_types(&self) -> Vec<AlertType> {
        AlertType::all()
            .into_iter()
            .filter(|ty| ty.view_name() == Some("tenant_alerts"))
            .map(|ty| *ty)
            .collect()
    }

    async fn query<'a>(&'a self, db: &'a sqlx::PgPool) -> anyhow::Result<Vec<AlertViewRow>> {
        // This queries the `internal.tenant_alerts` view for historical
        // reasons. If we ever need to change that view, we should consider
        // dropping the view in favor of a regular sql query, which is easier to
        // manage.
        let rows = sqlx::query_as!(
            AlertViewRow,
            r#"select
                catalog_name as "catalog_name!: String",
                alert_type as "alert_type!: AlertType",
                arguments as "base_arguments: Json<ArgsObject>",
                coalesce(firing, false) as "firing!: bool"
            from internal.tenant_alerts
            where catalog_name is not null
            and alert_type is not null
                "#
        )
        .fetch_all(db)
        .await?;
        Ok(rows)
    }
}

#[derive(Debug, Clone)]
pub struct DataMovementStalledAlerts;
impl AlertView for DataMovementStalledAlerts {
    fn alert_types(&self) -> Vec<AlertType> {
        AlertType::all()
            .into_iter()
            .filter(|ty| ty.view_name() == Some("alert_data_movement_stalled"))
            .map(|ty| *ty)
            .collect()
    }

    async fn query<'a>(&'a self, db: &'a sqlx::PgPool) -> anyhow::Result<Vec<AlertViewRow>> {
        // This queries the `internal.alert_data_movement_stalled` view for
        // historical reasons. If we ever need to change that view, we should
        // consider dropping the view in favor of a regular sql query, which is
        // easier to manage.
        let rows = sqlx::query_as!(
            AlertViewRow,
            r#"select
                catalog_name as "catalog_name!: String",
                alert_type as "alert_type!: AlertType",
                arguments as "base_arguments: Json<ArgsObject>",
                coalesce(firing, false) as "firing!: bool"
            from internal.alert_data_movement_stalled
            where catalog_name is not null
            and alert_type is not null
            "#,
        )
        .fetch_all(db)
        .await?;
        Ok(rows)
    }
}

#[derive(Debug, Clone)]
pub struct AlertEvaluator<V> {
    view: V,
    /// A function returning the id generator for this alert type. Id generators
    /// can't be safely cloned, so we have this function return an id generator
    /// with a shard id that's exclusively used by this alert evaluator. This
    /// just seemed better than using an `Arc<Mutex<IdGenerator>>` or generating
    /// a new random shard every time.
    id_gen: fn() -> models::IdGenerator,
    evaluation_interval: std::time::Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum EvaluatorMessage {
    ManualTrigger,
    Pause,
    Resume,
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct EvaluatorState {
    pub(crate) paused_at: Option<DateTime<Utc>>,
    pub(crate) last_evaluation_time: DateTime<Utc>,
    pub(crate) last_result: EvaluationSummary,
    pub(crate) open_alerts: BTreeMap<AlertType, usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
    pub(crate) failures: u32,
}

impl EvaluatorState {
    fn evaluation_succeeded(&mut self, ts: DateTime<Utc>, result: EvaluationSummary) {
        self.last_evaluation_time = ts;
        self.failures = 0;
        self.error.take();
        self.open_alerts = result.starting_open.clone();

        for (alert_type, count) in result.fired.iter() {
            let open = self.open_alerts.entry(*alert_type).or_default();
            *open += *count;
        }
        for (alert_type, count) in result.resolved.iter() {
            let open = self.open_alerts.entry(*alert_type).or_default();
            *open = open.saturating_sub(*count);
        }
        self.last_result = result;
    }

    fn evaluation_failed(&mut self, ts: DateTime<Utc>, err: &anyhow::Error) {
        self.last_evaluation_time = ts;
        self.failures += 1;
        self.last_result = EvaluationSummary::default();
        self.error = Some(format!("{err:#}"));
    }
}

#[derive(Debug, Default, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EvaluationSummary {
    /// Counts of alerts that were fired during this evaluation.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    fired: BTreeMap<AlertType, usize>,
    /// Counts of alerts that were resolved during this evaluation.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    resolved: BTreeMap<AlertType, usize>,
    /// Counts of alerts that had already been firing before evaluation began.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    starting_open: BTreeMap<AlertType, usize>,
    /// Total number of alerts that were in the alert view, which may include both
    /// firing and resolved alerts.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    view_evaluated: BTreeMap<AlertType, usize>,
}

impl EvaluationSummary {
    fn start(open: &[Alert], view: &[AlertViewRow]) -> EvaluationSummary {
        let mut summary = EvaluationSummary::default();
        for row in open {
            count(&mut summary.starting_open, row.alert_type);
        }
        for row in view {
            count(&mut summary.view_evaluated, row.alert_type);
        }
        summary
    }

    fn count_actions(&mut self, actions: &[AlertAction]) {
        for action in actions {
            match action {
                AlertAction::Fire(f) => count(&mut self.fired, f.alert_type),
                AlertAction::Resolve(r) => count(&mut self.resolved, r.alert_type),
            }
        }
    }
}

fn count(map: &mut BTreeMap<AlertType, usize>, alert_type: AlertType) {
    let n = map.entry(alert_type).or_default();
    *n += 1;
}

impl<V: AlertView> AlertEvaluator<V> {
    #[tracing::instrument(skip_all, fields(view))]
    async fn run_evaluation_executor(
        &self,
        pool: &sqlx::PgPool,
        state: &mut EvaluatorState,
        inbox: &mut std::collections::VecDeque<(models::Id, Option<EvaluatorMessage>)>,
    ) -> anyhow::Result<Action> {
        tracing::Span::current().record("view", tracing::field::debug(&self.view));
        let mut manual_trigger = false;
        for (_, message) in inbox.drain(..) {
            match message {
                Some(EvaluatorMessage::ManualTrigger) => {
                    tracing::info!("alert evaluation manually triggered");
                    manual_trigger = true;
                }
                Some(EvaluatorMessage::Pause) => {
                    tracing::warn!("alert evaluation is being paused");
                    state.paused_at = Some(Utc::now());
                }
                Some(EvaluatorMessage::Resume) => {
                    tracing::warn!(paused_at = ?state.paused_at, "alert evaluation is being resumed");
                    state.paused_at.take();
                }
                None => { /* pass */ }
            }
        }

        // Ignore paused state if we've received a manual trigger message
        if let Some(paused_at) = state.paused_at
            && !manual_trigger
        {
            tracing::warn!(%paused_at, "skipping alert evaluation because it is paused");
            // We still run every evaluation interval so that we'll log this warning periodically
            // while the alert is paused. Just trying to make it harder to forget to resume.
            return Ok(Action::Sleep(self.evaluation_interval));
        }

        let eval_timestamp = Utc::now();
        let start_time = std::time::Instant::now();
        let result = self.try_evaluate(eval_timestamp, pool).await;
        let duration = start_time.elapsed();

        match result {
            Ok(eval_result) => {
                tracing::info!(?eval_result, duration_ms = %duration.as_millis(), "alert evaluation finished successfully");
                state.evaluation_succeeded(eval_timestamp, eval_result);
                Ok(Action::Sleep(self.evaluation_interval))
            }
            Err(error) => {
                tracing::error!(?error, duration_ms = %duration.as_millis(), "alert evaluation failed");
                state.evaluation_failed(eval_timestamp, &error);
                Err(error)
            }
        }
    }

    async fn try_evaluate(
        &self,
        eval_timestamp: DateTime<Utc>,
        pool: &sqlx::PgPool,
    ) -> anyhow::Result<EvaluationSummary> {
        let alert_types = self.view.alert_types();

        let open_alerts = fetch_open_alerts_by_type(&alert_types, pool)
            .await
            .context("fetching open alerts")?;

        let new_alert_state = self.view.query(pool).await.context("querying alert view")?;

        let mut result = EvaluationSummary::start(&open_alerts, &new_alert_state);

        let mut id_gen = (self.id_gen)();
        let actions =
            evaluate_alert_actions(eval_timestamp, &mut id_gen, open_alerts, new_alert_state);
        result.count_actions(&actions);

        if actions.is_empty() {
            return Ok(result);
        }

        let mut txn = pool.begin().await?;

        apply_alert_actions(actions, &mut txn)
            .await
            .context("applying alert actions")?;

        txn.commit().await.context("committing transaction")?;

        Ok(result)
    }
}

impl Executor for AlertEvaluator<TenantAlerts> {
    const TASK_TYPE: TaskType = task_types::TENANT_ALERT_EVALS;

    type Receive = EvaluatorMessage;
    type State = EvaluatorState;
    type Outcome = Action;

    async fn poll<'s>(
        &'s self,
        pool: &'s sqlx::PgPool,
        _task_id: models::Id,
        _parent_id: Option<models::Id>,
        state: &'s mut Self::State,
        inbox: &'s mut std::collections::VecDeque<(models::Id, Option<Self::Receive>)>,
    ) -> anyhow::Result<Self::Outcome> {
        self.run_evaluation_executor(pool, state, inbox).await
    }
}

impl Executor for AlertEvaluator<DataMovementStalledAlerts> {
    const TASK_TYPE: TaskType = task_types::DATA_MOVEMENT_ALERT_EVALS;

    type Receive = EvaluatorMessage;
    type State = EvaluatorState;
    type Outcome = Action;

    async fn poll<'s>(
        &'s self,
        pool: &'s sqlx::PgPool,
        _task_id: models::Id,
        _parent_id: Option<models::Id>,
        state: &'s mut Self::State,
        inbox: &'s mut std::collections::VecDeque<(models::Id, Option<Self::Receive>)>,
    ) -> anyhow::Result<Self::Outcome> {
        self.run_evaluation_executor(pool, state, inbox).await
    }
}
