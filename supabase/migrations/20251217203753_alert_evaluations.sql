begin;

-- Disable the alerting cron job. This job doesn't exist in local environments, so
-- we do it this way to keep the migration from breaking locally.
with job as (
  select jobid from cron.job where jobname = 'evaluate-alert-events'
)
select cron.alter_job(job.jobid, active := false)
from job;

-- create the automations tasks for the alert evaluator jobs.
select internal.create_task(internal.id_generator(), 10::smallint, '0000000000000000'::flowid);
select internal.create_task(internal.id_generator(), 11::smallint, '0000000000000000'::flowid);

with new_tasks as (
    select task_id from internal.tasks where task_type = 10 or task_type = 11
)
select internal.send_to_task(task_id, '0000000000000000'::flowid, '"Resume"'::json)
from new_tasks;

commit;
