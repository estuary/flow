-- Incrementally enable automation-based controllers for existing live specs,
-- while preserving the ability to run legacy controllers for specs that haven't
-- been upgraded yet. This is important because a controller that has been
-- upgraded could subsequently be downgraded by a legacy controller (due to
-- legacy agents running `notify_dependents`, which sets `controller_next_run`).
-- It's possible for rows to have `controller_next_run` even after we've already
-- initialized a `controller_task_id` for a spec, so this handles that by
-- copying the `controller_next_run` to the `wake_at` of the corresponding task.
begin;

with to_update as (
    select id as live_spec_id, internal.id_generator() as tid, controller_next_run
    from public.live_specs
    where controller_task_id is null or controller_next_run is not null
    limit 1000
),
new_tasks as (
    update public.live_specs
    set
        controller_task_id = new_ls.tid,
        controller_next_run = null
    from (
            select tid, live_spec_id
            from to_update
        ) new_ls
    where live_specs.id = new_ls.live_spec_id
)
insert into internal.tasks (task_id, task_type, wake_at)
select
    tid as controller_task_id,
    2 as task_type,
    controller_next_run as wake_at
from to_update
on conflict (task_id) do update set
    wake_at = least(internal.tasks.wake_at, excluded.wake_at);

commit;
