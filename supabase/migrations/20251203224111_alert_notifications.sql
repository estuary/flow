begin;

-- Disable the alerting cron job. This job doesn't exist in local environments, so
-- we do it this way to keep the migration from breaking locally.
with job as (
  select jobid from cron.job where jobname = 'evaluate-alert-events'
)
select cron.alter_job(job.jobid, active := false)
from job;

alter table alert_history disable trigger "Send email after alert fired";
alter table alert_history disable trigger "Send email after alert resolved";

-- Add a new column, which will serve as a surrogate key. This will generate new
-- unique ids for all existing rows because of the default on the flowid domain.
alter table public.alert_history add column id flowid not null default internal.id_generator();
comment on column public.alert_history.id is
'Unique id of this alert instance. This id is also used as the task_id of \
the automations task that sends alert notifications, since there is a 1-1 \
relationship between alert instances and notifier tasks.';

insert into internal.tasks (task_id, task_type, parent_id, inner_state)
select id, 9, '0000000000000000'::flowid, '{"fired_completed": "2025-12-31T23:59:59Z"}'::json
from public.alert_history
where resolved_at is null
on conflict do nothing;

create unique index id_uniq on alert_history (id);

-- create the automations tasks for the alert evaluator jobs.
-- 10 is for all tenant alerts and 11 is for data_movement_stalled
select internal.create_task(internal.id_generator(), 10::smallint, '0000000000000000'::flowid);
select internal.create_task(internal.id_generator(), 11::smallint, '0000000000000000'::flowid);

with new_tasks as (
    select task_id from internal.tasks where task_type = 10 or task_type = 11
)
select internal.send_to_task(task_id, '0000000000000000'::flowid, '"Resume"'::json)
from new_tasks;

commit;
