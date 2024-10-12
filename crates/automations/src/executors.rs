use super::{server, BoxedRaw, Executor, PollOutcome, TaskType};
use anyhow::Context;
use futures::future::{BoxFuture, FutureExt};
use sqlx::types::Json as SqlJson;

/// ObjSafe is an object-safe and type-erased trait which is implemented for all Executors.
pub trait ObjSafe: Send + Sync + 'static {
    fn task_type(&self) -> TaskType;

    fn poll<'s>(
        &'s self,
        task_id: models::Id,
        parent_id: Option<models::Id>,
        state: &'s mut Option<SqlJson<BoxedRaw>>,
        inbox: &'s mut Option<Vec<SqlJson<(models::Id, Option<BoxedRaw>)>>>,
    ) -> BoxFuture<'s, anyhow::Result<PollOutcome<BoxedRaw>>>;
}

impl<E: Executor> ObjSafe for E {
    fn task_type(&self) -> TaskType {
        E::TASK_TYPE
    }

    fn poll<'s>(
        &'s self,
        task_id: models::Id,
        parent_id: Option<models::Id>,
        state: &'s mut Option<SqlJson<BoxedRaw>>,
        inbox: &'s mut Option<Vec<SqlJson<(models::Id, Option<BoxedRaw>)>>>,
    ) -> BoxFuture<'s, anyhow::Result<PollOutcome<BoxedRaw>>> {
        async move {
            let mut state_parsed: E::State = if let Some(state) = state {
                serde_json::from_str(state.get()).context("failed to decode task state")?
            } else {
                E::State::default()
            };

            let mut inbox_parsed: std::collections::VecDeque<(models::Id, Option<E::Receive>)> =
                inbox
                    .as_ref()
                    .into_iter()
                    .flatten()
                    .map(|SqlJson((task_id, rx))| {
                        if let Some(rx) = rx {
                            anyhow::Result::Ok((*task_id, Some(serde_json::from_str(rx.get())?)))
                        } else {
                            anyhow::Result::Ok((*task_id, None))
                        }
                    })
                    .collect::<anyhow::Result<_>>()
                    .context("failed to decode received message")?;

            let outcome = E::poll(
                self,
                task_id,
                parent_id,
                &mut state_parsed,
                &mut inbox_parsed,
            )
            .await?;

            // Re-encode state for persistence.
            // If we're Done, then the output state is NULL which is implicitly Default.
            if matches!(outcome, PollOutcome::Done) {
                *state = None
            } else {
                *state = Some(SqlJson(
                    serde_json::value::to_raw_value(&state_parsed)
                        .context("failed to encode inner state")?,
                ));
            }

            // Re-encode the unconsumed portion of the inbox.
            if inbox_parsed.is_empty() {
                *inbox = None
            } else {
                *inbox = Some(
                    inbox_parsed
                        .into_iter()
                        .map(|(task_id, msg)| {
                            Ok(SqlJson((
                                task_id,
                                match msg {
                                    Some(msg) => Some(serde_json::value::to_raw_value(&msg)?),
                                    None => None,
                                },
                            )))
                        })
                        .collect::<anyhow::Result<Vec<_>>>()
                        .context("failed to encode unconsumed inbox message")?,
                );
            }

            Ok(match outcome {
                PollOutcome::Done => PollOutcome::Done,
                PollOutcome::Send(task_id, msg) => PollOutcome::Send(task_id, msg),
                PollOutcome::Sleep(interval) => PollOutcome::Sleep(interval),
                PollOutcome::Spawn(task_id, task_type, msg) => {
                    PollOutcome::Spawn(task_id, task_type, msg)
                }
                PollOutcome::Suspend => PollOutcome::Suspend,
                PollOutcome::Yield(msg) => PollOutcome::Yield(
                    serde_json::value::to_raw_value(&msg)
                        .context("failed to encode yielded message")?,
                ),
            })
        }
        .boxed()
    }
}

pub async fn poll_task(
    server::ReadyTask {
        executor,
        permit: _guard,
        pool,
        task:
            server::DequeuedTask {
                id: task_id,
                type_: _,
                parent_id,
                mut inbox,
                mut state,
                mut last_heartbeat,
            },
    }: server::ReadyTask,
    heartbeat_timeout: std::time::Duration,
) -> anyhow::Result<()> {
    let mut heartbeat_ticks = tokio::time::interval(heartbeat_timeout / 2);
    let _instant = heartbeat_ticks.tick().await; // Discard immediate first tick.

    // Build a Future which forever maintains our heartbeat or fails.
    let update_heartbeats = async {
        loop {
            let _instant = heartbeat_ticks.tick().await;

            last_heartbeat =
                match update_heartbeat(&pool, task_id, heartbeat_timeout, last_heartbeat).await {
                    Ok(last_heartbeat) => last_heartbeat,
                    Err(err) => return err,
                }
        }
    };
    tokio::pin!(update_heartbeats);

    // Poll `executor` and `update_heartbeats` in tandem, so that a failure
    // to update our heartbeat also cancels the executor.
    let outcome = tokio::select! {
        outcome = executor.poll(task_id, parent_id, &mut state, &mut inbox) => { outcome? },
        err = &mut update_heartbeats => return Err(err),
    };

    // The possibly long-lived polling operation is now complete.
    // Build a Future that commits a (hopefully) brief transaction of `outcome`.
    let persist_outcome = async {
        let mut txn = pool.begin().await?;
        () = persist_outcome(outcome, &mut *txn, task_id, parent_id, state, inbox).await?;
        Ok(txn.commit().await?)
    };

    // Poll `persist_outcome` while continuing to poll `update_heartbeats`,
    // to guarantee we cannot commit an outcome after our lease is lost.
    tokio::select! {
        result = persist_outcome => result,
        err = update_heartbeats => Err(err),
    }
}

async fn update_heartbeat(
    pool: &sqlx::PgPool,
    task_id: models::Id,
    heartbeat_timeout: std::time::Duration,
    expect_heartbeat: String,
) -> anyhow::Result<String> {
    let update = sqlx::query!(
        r#"
        UPDATE internal.tasks
        SET heartbeat = NOW()
        WHERE task_id = $1 AND heartbeat::TEXT = $2
        RETURNING heartbeat::TEXT AS "heartbeat!";
        "#,
        task_id as models::Id,
        expect_heartbeat,
    )
    .fetch_optional(pool);

    // We must guard against both explicit errors and also timeouts when updating
    // the heartbeat, to ensure we bubble up an error that cancels our paired
    // executor prior to `heartbeat_timeout` elapsing.
    let updated = match tokio::time::timeout(heartbeat_timeout / 4, update).await {
        Ok(Ok(Some(updated))) => updated,
        Ok(Ok(None)) => anyhow::bail!("task heartbeat was unexpectedly updated externally"),
        Ok(Err(err)) => return Err(anyhow::anyhow!(err).context("failed to update task heartbeat")),
        Err(err) => return Err(anyhow::anyhow!(err).context("timed out updating task heartbeat")),
    };

    tracing::info!(
        last = expect_heartbeat,
        next = updated.heartbeat,
        "updated task heartbeat"
    );

    Ok(updated.heartbeat)
}

async fn persist_outcome(
    outcome: PollOutcome<BoxedRaw>,
    txn: &mut sqlx::PgConnection,
    task_id: models::Id,
    parent_id: Option<models::Id>,
    state: Option<SqlJson<BoxedRaw>>,
    inbox: Option<Vec<SqlJson<(models::Id, Option<BoxedRaw>)>>>,
) -> anyhow::Result<()> {
    use std::time::Duration;

    if let PollOutcome::Spawn(spawn_id, spawn_type, _msg) = &outcome {
        sqlx::query!(
            "SELECT internal.create_task($1, $2, $3)",
            *spawn_id as models::Id,
            *spawn_type as TaskType,
            task_id as models::Id,
        )
        .execute(&mut *txn)
        .await
        .context("failed to spawn new task")?;
    }

    if let Some((send_id, msg)) = match &outcome {
        // When a task is spawned, send its first message.
        PollOutcome::Spawn(spawn_id, _spawn_type, msg) => Some((*spawn_id, Some(msg))),
        // If we're Done but have a parent, send it an EOF.
        PollOutcome::Done => parent_id.map(|parent_id| (parent_id, None)),
        // Send an arbitrary message to an identified task.
        PollOutcome::Send(task_id, msg) => Some((*task_id, msg.as_ref())),
        // Yield is sugar for sending to our parent.
        PollOutcome::Yield(msg) => {
            let Some(parent_id) = parent_id else {
                anyhow::bail!("task yielded illegally, because it does not have a parent");
            };
            Some((parent_id, Some(msg)))
        }
        _ => None,
    } {
        sqlx::query!(
            "SELECT internal.send_to_task($1, $2, $3::JSON);",
            send_id as models::Id,
            task_id as models::Id,
            SqlJson(msg) as SqlJson<_>,
        )
        .execute(&mut *txn)
        .await
        .with_context(|| format!("failed to send message to {send_id:?}"))?;
    }

    let wake_at_interval = if inbox.is_some() {
        Some(Duration::ZERO) // Always poll immediately if inbox items remain.
    } else {
        match &outcome {
            PollOutcome::Sleep(interval) => Some(*interval),
            // These outcomes do not suspend the task, and it should wake as soon as possible.
            PollOutcome::Spawn(..) | PollOutcome::Send(..) | PollOutcome::Yield(..) => {
                Some(Duration::ZERO)
            }
            // Suspend indefinitely (note that NOW() + NULL::INTERVAL is NULL).
            PollOutcome::Done | PollOutcome::Suspend => None,
        }
    };

    let updated = sqlx::query!(
        r#"
        UPDATE internal.tasks SET
            heartbeat = '0001-01-01T00:00:00Z',
            inbox = $3::JSON[] || inbox_next,
            inbox_next = NULL,
            inner_state = $2::JSON,
            wake_at =
                CASE WHEN inbox_next IS NOT NULL
                THEN NOW()
                ELSE NOW() + $4::INTERVAL
                END
        WHERE task_id = $1
        RETURNING wake_at IS NULL AS "suspended!"
        "#,
        task_id as models::Id,
        state as Option<SqlJson<BoxedRaw>>,
        inbox as Option<Vec<SqlJson<(models::Id, Option<BoxedRaw>)>>>,
        wake_at_interval as Option<Duration>,
    )
    .fetch_one(&mut *txn)
    .await
    .context("failed to update task row")?;

    // If we're Done and also successfully suspended, then delete ourselves.
    // (Otherwise, the task has been left in a like-new state).
    if matches!(&outcome, PollOutcome::Done if updated.suspended) {
        sqlx::query!(
            "DELETE FROM internal.tasks WHERE task_id = $1;",
            task_id as models::Id,
        )
        .execute(&mut *txn)
        .await
        .context("failed to delete task row")?;
    }

    Ok(())
}
