use anyhow::Context;
use std::sync::Arc;

mod executors;
pub mod server;

/// BoxedRaw is a type-erased raw JSON message.
type BoxedRaw = Box<serde_json::value::RawValue>;

/// TaskType is the type of a task, and maps it to an Executor.
#[derive(
    Debug,
    serde::Deserialize,
    serde::Serialize,
    sqlx::Type,
    PartialOrd,
    PartialEq,
    Ord,
    Eq,
    Clone,
    Copy,
)]
#[sqlx(transparent)]
pub struct TaskType(pub i16);

/// Task types must be globally unique, and very bad things will happen if we
/// accidentally run two different executors for the same task type. So we
/// define constants for all in-use task types here so it's easier to avoid
/// collisions. These must not change once they're in use, as there's also
/// places where we hard-code them in sql.
pub mod task_types {
    use super::TaskType;

    pub const DATA_PLANE_CONTROLLER: TaskType = TaskType(1);
    pub const LIVE_SPEC_CONTROLLER: TaskType = TaskType(2);
}

/// Outcome of an `Executor::poll()` for a given task, which encloses
/// an Action with which it's applied as a single transaction.
///
/// As an example of how Executor, Outcome, and Action are used together,
/// suppose an implementation of `Executor::poll()` is called:
///
/// - It reads DB state associated with the task using sqlx::PgPool.
/// - It performs long-running work, running outside of a DB transaction.
/// - It returns an Outcome implementation which encapsulates the
///   preconditions it observed, as well as its domain-specific outcome.
/// - `Outcome::apply()` is called and re-verifies preconditions using `txn`,
///   returning an error if preconditions have changed.
/// - It applies the effects of its outcome and returns a polling Action.
/// - `txn` is further by this crate as required by the Action, and then commits.
///
pub trait Outcome: Send {
    /// Apply the effects of an Executor poll. While this is an async routine,
    /// apply() runs in the context of a held transaction and should be fast.
    fn apply<'s>(
        self,
        txn: &'s mut sqlx::PgConnection,
    ) -> impl std::future::Future<Output = anyhow::Result<Action>> + Send + 's;
}

/// Action undertaken by an Executor task poll.
#[derive(Debug)]
pub enum Action {
    /// Spawn a new TaskId with the given TaskType and send a first message.
    /// The TaskId must not exist.
    Spawn(models::Id, TaskType, BoxedRaw),
    /// Send a message (Some) or EOF (None) to another TaskId, which must exist.
    Send(models::Id, Option<BoxedRaw>),
    /// Yield to send a message to this task's parent.
    Yield(BoxedRaw),
    /// Sleep for at-most the indicated Duration, then poll again.
    /// The task may be woken earlier if it receives a message.
    Sleep(std::time::Duration),
    /// Suspend the task until it receives a message.
    Suspend,
    /// Done completes and removes the task.
    /// If this task has a parent, that parent is sent an EOF.
    Done,
}

// Action implements an Outcome with no side-effects.
impl Outcome for Action {
    async fn apply<'s>(self, _txn: &'s mut sqlx::PgConnection) -> anyhow::Result<Action> {
        Ok(self)
    }
}

/// Executor is the core trait implemented by executors of various task types.
pub trait Executor: Send + Sync + 'static {
    const TASK_TYPE: TaskType;

    type Receive: serde::de::DeserializeOwned + serde::Serialize + Send;
    type State: Default + serde::de::DeserializeOwned + serde::Serialize + Send;
    type Outcome: Outcome;

    fn poll<'s>(
        &'s self,
        pool: &'s sqlx::PgPool,
        task_id: models::Id,
        parent_id: Option<models::Id>,
        state: &'s mut Self::State,
        inbox: &'s mut std::collections::VecDeque<(models::Id, Option<Self::Receive>)>,
    ) -> impl std::future::Future<Output = anyhow::Result<Self::Outcome>> + Send + 's;
}

/// Server holds registered implementations of Executors and serves them.
pub struct Server(Vec<Arc<dyn executors::ObjSafe>>);

impl Action {
    pub fn spawn<M: serde::Serialize>(
        spawn_id: models::Id,
        task_type: TaskType,
        msg: M,
    ) -> anyhow::Result<Self> {
        Ok(Self::Spawn(
            spawn_id,
            task_type,
            serde_json::value::to_raw_value(&msg).context("failed to encode task spawn message")?,
        ))
    }

    pub fn send<M: serde::Serialize>(task_id: models::Id, msg: Option<M>) -> anyhow::Result<Self> {
        Ok(Self::Send(
            task_id,
            match msg {
                Some(msg) => Some(
                    serde_json::value::to_raw_value(&msg)
                        .context("failed to encode sent message")?,
                ),
                None => None,
            },
        ))
    }

    pub fn yield_<M: serde::Serialize>(msg: M) -> anyhow::Result<Self> {
        Ok(Self::Yield(
            serde_json::value::to_raw_value(&msg).context("failed to encode yielded message")?,
        ))
    }
}

pub fn next_task_id() -> models::Id {
    static ID_GENERATOR: std::sync::LazyLock<std::sync::Mutex<models::IdGenerator>> =
        std::sync::LazyLock::new(|| std::sync::Mutex::new(models::IdGenerator::new(1)));

    ID_GENERATOR.lock().unwrap().next()
}
