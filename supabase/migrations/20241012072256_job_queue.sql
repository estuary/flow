BEGIN;

CREATE TABLE internal.tasks (
    task_id     public.flowid PRIMARY KEY NOT NULL,
    task_type   SMALLINT NOT NULL,
    parent_id   public.flowid,

    inner_state JSON,

    wake_at     TIMESTAMPTZ,
    inbox       JSON[],
    inbox_next  JSON[],

    heartbeat   TIMESTAMPTZ NOT NULL DEFAULT '0001-01-01T00:00:00Z'
);

CREATE INDEX idx_tasks_ready_at ON internal.tasks
    USING btree (wake_at) INCLUDE (task_type);

COMMENT ON TABLE internal.tasks IS '
The tasks table supports a distributed and asynchronous task execution system
implemented in the Rust "automations" crate.

Tasks are poll-able coroutines which are identified by task_id and have a task_type.
They may be short-lived and polled just once, or very long-lived and polled
many times over their life-cycle.

Tasks are polled by executors which dequeue from the tasks table and run
bespoke executors parameterized by the task type. A polling routine may take
an arbitrarily long amount of time to finish, and the executor
is required to periodically update the task heartbeat as it runs.

A task is polled by at-most one executor at a time. Executor failures are
detected through a failure to update the task heartbeat within a threshold amount
of time, which makes the task re-eligible for dequeue by another executor.

Tasks are coroutines and may send messages to one another, which is tracked in the
inbox of each task and processed by the task executor. If a task is currently being
polled (its heartbeat is not the DEFAULT), then messages accrue in inbox_next.
';


CREATE FUNCTION internal.create_task(
    p_task_id    public.flowid,
    p_task_type  SMALLINT,
    p_parent_id  public.flowid
)
RETURNS VOID
SET search_path = ''
AS $$
BEGIN

    INSERT INTO internal.tasks (task_id, task_type, parent_id)
    VALUES (p_task_id, p_task_type, p_parent_id);

END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION internal.send_to_task(
    p_task_id    public.flowid,
    p_from_id    public.flowid,
    p_message    JSON
)
RETURNS VOID
SET search_path = ''
AS $$
BEGIN

    UPDATE internal.tasks SET
        wake_at = LEAST(wake_at, NOW()),
        inbox =
            CASE WHEN heartbeat = '0001-01-01T00:00:00Z'
            THEN ARRAY_APPEND(inbox, JSON_BUILD_ARRAY(p_from_id, p_message))
            ELSE inbox
            END,
        inbox_next =
            CASE WHEN heartbeat = '0001-01-01T00:00:00Z'
            THEN inbox_next
            ELSE ARRAY_APPEND(inbox_next, JSON_BUILD_ARRAY(p_from_id, p_message))
            END
    WHERE task_id = p_task_id;

END;
$$ LANGUAGE plpgsql;


COMMIT;