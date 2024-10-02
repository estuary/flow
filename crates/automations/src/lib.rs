use anyhow::Context;
use std::sync::Arc;

mod executors;
mod server;

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

/// PollOutcome is the outcome of an `Executor::poll()` for a given task.
#[derive(Debug)]
pub enum PollOutcome<Yield> {
    /// Spawn a new TaskId with the given TaskType and send a first message.
    /// The TaskId must not exist.
    Spawn(models::Id, TaskType, BoxedRaw),
    /// Send a message (Some) or EOF (None) to another TaskId, which must exist.
    Send(models::Id, Option<BoxedRaw>),
    /// Yield to send a message to this task's parent.
    Yield(Yield),
    /// Sleep for at-most the indicated Duration, then poll again.
    /// The task may be woken earlier if it receives a message.
    Sleep(std::time::Duration),
    /// Suspend the task until it receives a message.
    Suspend,
    /// Done completes and removes the task.
    /// If this task has a parent, that parent is sent an EOF.
    Done,
}

/// Executor is the core trait implemented by executors of various task types.
pub trait Executor: Send + Sync + 'static {
    const TASK_TYPE: TaskType;

    type Receive: serde::de::DeserializeOwned + serde::Serialize + Send;
    type State: Default + serde::de::DeserializeOwned + serde::Serialize + Send;
    type Yield: serde::Serialize;

    fn poll<'s>(
        &'s self,
        task_id: models::Id,
        parent_id: Option<models::Id>,
        state: &'s mut Self::State,
        inbox: &'s mut std::collections::VecDeque<(models::Id, Option<Self::Receive>)>,
    ) -> impl std::future::Future<Output = anyhow::Result<PollOutcome<Self::Yield>>> + Send + 's;
}

/// Server holds registered implementations of Executor,
/// and serves them.
pub struct Server(Vec<Arc<dyn executors::ObjSafe>>);

impl<Yield> PollOutcome<Yield> {
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
}

pub fn next_task_id() -> models::Id {
    static ID_GENERATOR: std::sync::LazyLock<std::sync::Mutex<models::IdGenerator>> =
        std::sync::LazyLock::new(|| std::sync::Mutex::new(models::IdGenerator::new(1)));

    ID_GENERATOR.lock().unwrap().next()
}
