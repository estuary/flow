use super::{executors, BoxedRaw, Executor, Server, TaskType};
use futures::stream::StreamExt;
use sqlx::types::Json as SqlJson;
use std::sync::Arc;
use tokio::sync::OwnedSemaphorePermit;

impl Server {
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    /// Register an Executor to be served by this Server.
    pub fn register<E: Executor>(mut self, executor: E) -> Self {
        let index = match self
            .0
            .binary_search_by_key(&E::TASK_TYPE, |entry| entry.task_type())
        {
            Ok(_index) => panic!("an Executor for {:?} is already registered", E::TASK_TYPE),
            Err(index) => index,
        };

        self.0.insert(index, Arc::new(executor));
        self
    }

    /// Serve this Server until signaled to stop by `shutdown`.
    pub async fn serve(
        self,
        permits: u32,
        pool: sqlx::PgPool,
        dequeue_interval: std::time::Duration,
        heartbeat_timeout: std::time::Duration,
        shutdown: impl std::future::Future<Output = ()>,
    ) {
        serve(
            self,
            permits,
            pool,
            dequeue_interval,
            heartbeat_timeout,
            shutdown,
        )
        .await
    }
}

pub struct ReadyTask {
    pub executor: Arc<dyn executors::ObjSafe>,
    pub permit: tokio::sync::OwnedSemaphorePermit,
    pub pool: sqlx::PgPool,
    pub task: DequeuedTask,
}

pub struct DequeuedTask {
    pub id: models::Id,
    pub type_: TaskType,
    pub parent_id: Option<models::Id>,
    pub inbox: Option<Vec<SqlJson<(models::Id, Option<BoxedRaw>)>>>,
    pub state: Option<SqlJson<BoxedRaw>>,
    pub last_heartbeat: String,
}

pub async fn serve(
    executors: Server,
    permits: u32,
    pool: sqlx::PgPool,
    dequeue_interval: std::time::Duration,
    heartbeat_timeout: std::time::Duration,
    shutdown: impl std::future::Future<Output = ()>,
) {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(permits as usize));

    // Use Box::pin to ensure we can fullly drop `ready_tasks` later,
    // as it may hold `semaphore` permits.
    let mut ready_tasks = Box::pin(ready_tasks(
        executors,
        pool.clone(),
        dequeue_interval,
        heartbeat_timeout,
        semaphore.clone(),
    ));
    tokio::pin!(shutdown);

    // Poll for ready tasks and start them until `shutdown` is signaled.
    while let Some(ready_tasks) = tokio::select! {
        ready = ready_tasks.next() => ready,
        () = &mut shutdown => None,
    } {
        let ready_tasks: Vec<ReadyTask> = match ready_tasks {
            Ok(tasks) => tasks,
            Err(err) => {
                tracing::error!(?err, "failed to poll for tasks (will retry)");
                Vec::new()
            }
        };

        for ready in ready_tasks {
            tokio::spawn(async move {
                let (task_id, task_type, parent_id) =
                    (ready.task.id, ready.task.type_, ready.task.parent_id);

                if let Err(err) = executors::poll_task(ready, heartbeat_timeout).await {
                    tracing::warn!(
                        ?task_id,
                        ?task_type,
                        ?parent_id,
                        ?err,
                        "task executor failed and will be retried after heartbeat timeout"
                    );
                    // The task will be retried once it's heartbeat times out.
                }
            });
        }
    }
    tracing::info!("task polling loop signaled to stop and is awaiting running tasks");
    std::mem::drop(ready_tasks);

    // Acquire all permits, when only happens after all running tasks have finished.
    let _ = semaphore.acquire_many_owned(permits).await.unwrap();
}

pub fn ready_tasks(
    executors: Server,
    pool: sqlx::PgPool,
    dequeue_interval: std::time::Duration,
    heartbeat_timeout: std::time::Duration,
    semaphore: Arc<tokio::sync::Semaphore>,
) -> impl futures::stream::Stream<Item = sqlx::Result<Vec<ReadyTask>>> {
    let task_types: Vec<_> = executors.0.iter().map(|e| e.task_type().0).collect();

    coroutines::coroutine(move |mut co| async move {
        loop {
            () = ready_tasks_iter(
                &mut co,
                &executors,
                heartbeat_timeout,
                dequeue_interval,
                &pool,
                &semaphore,
                &task_types,
            )
            .await;
        }
    })
}

async fn ready_tasks_iter(
    co: &mut coroutines::Suspend<sqlx::Result<Vec<ReadyTask>>, ()>,
    executors: &Server,
    heartbeat_timeout: std::time::Duration,
    dequeue_interval: std::time::Duration,
    pool: &sqlx::PgPool,
    semaphore: &Arc<tokio::sync::Semaphore>,
    task_types: &[i16],
) {
    // Block until at least one permit is available.
    if semaphore.available_permits() == 0 {
        let _ = semaphore.clone().acquire_owned().await.unwrap();
    }

    // Acquire all available permits, and then poll for up to that many tasks.
    let mut permits = semaphore
        .clone()
        .acquire_many_owned(semaphore.available_permits() as u32)
        .await
        .unwrap();

    let dequeued = sqlx::query_as!(
        DequeuedTask,
        r#"
        WITH picked AS (
            SELECT task_id
            FROM internal.tasks
            WHERE
                task_type = ANY($1) AND
                wake_at   < NOW() AND
                heartbeat < NOW() - $2::INTERVAL
            ORDER BY wake_at DESC
            LIMIT $3
            FOR UPDATE SKIP LOCKED
        )
        UPDATE internal.tasks
        SET heartbeat = NOW()
        WHERE task_id in (SELECT task_id FROM picked)
        RETURNING
            task_id as "id: models::Id",
            task_type as "type_: TaskType",
            parent_id as "parent_id: models::Id",
            inbox as "inbox: Vec<SqlJson<(models::Id, Option<BoxedRaw>)>>",
            inner_state as "state: SqlJson<BoxedRaw>",
            heartbeat::TEXT as "last_heartbeat!";
        "#,
        &task_types as &[i16],
        heartbeat_timeout as std::time::Duration,
        permits.num_permits() as i64,
    )
    .fetch_all(pool)
    .await;

    let dequeued = match dequeued {
        Ok(dequeued) => {
            tracing::debug!(dequeued = dequeued.len(), "completed task dequeue");
            dequeued
        }
        Err(err) => {
            () = co.yield_(Err(err)).await;
            Vec::new() // We'll sleep as if it were idle, then retry.
        }
    };

    let ready = dequeued
        .into_iter()
        .map(|task| {
            let Ok(index) = task_types.binary_search(&task.type_.0) else {
                panic!("polled {:?} with unexpected {:?}", task.id, task.type_);
            };
            ReadyTask {
                task,
                executor: executors.0[index].clone(),
                permit: permits.split(1).unwrap(),
                pool: pool.clone(),
            }
        })
        .collect();

    () = co.yield_(Ok(ready)).await;

    // If permits remain, there were not enough tasks to dequeue.
    // Sleep for up-to `dequeue_interval`, cancelling early if a task completes.
    if permits.num_permits() != 0 {
        // Jitter dequeue by 10% in either direction, to ensure
        // distribution of tasks and retries across executors.
        let jitter = 0.9 + rand::random::<f64>() * 0.2; // [0.9, 1.1)

        tokio::select! {
            () = tokio::time::sleep(dequeue_interval.mul_f64(jitter)) => (),
            _ = semaphore.clone().acquire_owned() => (), // Cancel sleep.
        }
    }
}

pub async fn dequeue_tasks(
    permits: &mut OwnedSemaphorePermit,
    pool: &sqlx::PgPool,
    executors: &Server,
    task_types: &[i16],
    heartbeat_timeout: std::time::Duration,
) -> sqlx::Result<Vec<ReadyTask>> {
    let dequeued = sqlx::query_as!(
        DequeuedTask,
        r#"
        WITH picked AS (
            SELECT task_id
            FROM internal.tasks
            WHERE
                task_type = ANY($1) AND
                wake_at   < NOW() AND
                heartbeat < NOW() - $2::INTERVAL
            ORDER BY wake_at DESC
            LIMIT $3
            FOR UPDATE SKIP LOCKED
        )
        UPDATE internal.tasks
        SET heartbeat = NOW()
        WHERE task_id in (SELECT task_id FROM picked)
        RETURNING
            task_id as "id: models::Id",
            task_type as "type_: TaskType",
            parent_id as "parent_id: models::Id",
            inbox as "inbox: Vec<SqlJson<(models::Id, Option<BoxedRaw>)>>",
            inner_state as "state: SqlJson<BoxedRaw>",
            heartbeat::TEXT as "last_heartbeat!";
        "#,
        &task_types as &[i16],
        heartbeat_timeout as std::time::Duration,
        permits.num_permits() as i64,
    )
    .fetch_all(pool)
    .await?;

    let ready = dequeued
        .into_iter()
        .map(|task| {
            let Ok(index) = task_types.binary_search(&task.type_.0) else {
                panic!("polled {:?} with unexpected {:?}", task.id, task.type_);
            };
            ReadyTask {
                task,
                executor: executors.0[index].clone(),
                permit: permits.split(1).unwrap(),
                pool: pool.clone(),
            }
        })
        .collect();

    Ok(ready)
}
