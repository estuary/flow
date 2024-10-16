use automations::Action;
use std::collections::VecDeque;

/// Fibonacci is one of the least-efficient calculators of the Fibonacci
/// sequence on the planet. It solves in exponential time by spawning two
/// sub-tasks in the recursive case, and does not re-use the results of
/// sub-computations.
pub struct Fibonacci {
    // Percentage of the time that task polls should randomly fail.
    // Value should be in range [0, 1) where 0 never fails.
    pub failure_rate: f32,
    // Amount of time to wait before allowing a poll to complete.
    pub sleep_for: std::time::Duration,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Message {
    pub value: i64,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum State {
    // Init spawns the first task and transitions to SpawnOne
    Init,
    // SpawnOne spawns the second task and transitions to Waiting
    SpawnOne(i64),
    // Waiting waits for `pending` child tasks to complete,
    // accumulating their yielded values, and then transitions to Sleeping.
    Waiting { partial: i64, pending: usize },
    Finished,
}

impl Default for State {
    fn default() -> Self {
        Self::Init
    }
}

impl automations::Executor for Fibonacci {
    const TASK_TYPE: automations::TaskType = automations::TaskType(32767);

    type Receive = Message;
    type State = State;
    type Outcome = Action;

    #[tracing::instrument(
        ret,
        err(Debug, level = tracing::Level::ERROR),
        skip_all,
        fields(?task_id, ?parent_id, ?state, ?inbox),
    )]
    async fn poll<'s>(
        &'s self,
        _pool: &'s sqlx::PgPool,
        task_id: models::Id,
        parent_id: Option<models::Id>,
        state: &'s mut Self::State,
        inbox: &'s mut VecDeque<(models::Id, Option<Message>)>,
    ) -> anyhow::Result<Self::Outcome> {
        if rand::random::<f32>() < self.failure_rate {
            return Err(
                anyhow::anyhow!("A no good, very bad error!").context("something bad happened")
            );
        }

        if let State::SpawnOne(value) = state {
            let spawn = Action::spawn(
                automations::next_task_id(),
                Self::TASK_TYPE,
                Message { value: *value - 2 },
            );
            *state = State::Waiting {
                partial: 0,
                pending: 2,
            };

            return spawn;
        }

        match (std::mem::take(state), inbox.pop_front()) {
            // Base case:
            (State::Init, Some((_parent_id, Some(Message { value })))) if value <= 2 => {
                *state = State::Finished;
                Action::yield_(Message { value: 1 })
            }

            // Recursive case:
            (State::Init, Some((_parent_id, Some(Message { value })))) => {
                *state = State::SpawnOne(value);

                Action::spawn(
                    automations::next_task_id(),
                    Self::TASK_TYPE,
                    Message { value: value - 1 },
                )
            }

            (State::Waiting { partial, pending }, None) => {
                *state = State::Waiting { partial, pending };
                // Sleeping at this point in the lifecycle exercises handling of
                // messages sent to a task that's currently being polled.
                () = tokio::time::sleep(self.sleep_for).await;
                Ok(Action::Suspend)
            }

            (State::Waiting { partial, pending }, Some((_child_id, Some(Message { value })))) => {
                *state = State::Waiting {
                    partial: partial + value,
                    pending,
                };
                Ok(Action::Suspend)
            }

            (State::Waiting { partial, pending }, Some((_child_id, None))) => {
                if pending != 1 || parent_id.is_none() {
                    *state = State::Waiting {
                        partial,
                        pending: pending - 1,
                    };
                    Ok(Action::Suspend)
                } else {
                    *state = State::Finished;
                    Action::yield_(Message { value: partial })
                }
            }

            (State::Finished, None) => Ok(Action::Done),

            state => anyhow::bail!("unexpected poll with state {state:?} and inbox {inbox:?}"),
        }
    }
}
