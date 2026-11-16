-- Tenant controllers evaluate the free_trial, free_trial_ending,
-- free_trial_stalled and missing_payment_method alerts. See
-- crates/agent/src/tenant_controller/alerts.rs.

begin;

-- Must match automations::task_types::TENANT_ALERT_EVALS = TaskType(10), whose
-- executor the agent no longer registers.
delete from internal.tasks where task_type = 10;

-- The evaluator ran hourly. Wake each tenant controller within the hour to
-- evaluate its alerts, including controllers suspended after their trial began.
update internal.tasks
set wake_at = now() + random() * interval '1 hour'
where task_type = 12
  and (wake_at is null or wake_at > now() + interval '1 hour');

-- `evaluate_alert_events` is the only reader of `alert_all`, and the agent's
-- alert evaluation replaced it. Its cron job was disabled rather than removed.
-- pg_cron exists only on the Supabase database, and not in sqlx::test databases.
do $$
begin
    if to_regclass('cron.job') is not null then
        perform cron.unschedule(jobid) from cron.job where jobname = 'evaluate-alert-events';
    end if;
end
$$;
drop function internal.evaluate_alert_events();
drop view internal.alert_all;
drop view internal.tenant_alerts;
drop view internal.free_trial_alerts;

commit;
