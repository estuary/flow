-- Resolve orphaned alerts for tasks that have already been deleted.
-- These are alerts in alert_history with resolved_at IS NULL where either:
-- (a) the live_specs row has been hard-deleted (no matching row), or
-- (b) the live_specs row is soft-deleted (spec IS NULL).
with resolved as (
  update public.alert_history ah
  set resolved_at = now()
  where ah.resolved_at is null
    and not exists (
      select 1 from public.live_specs ls
      where ls.catalog_name = ah.catalog_name
        and ls.spec is not null
    )
  returning ah.id
)
-- Clean up the orphaned notification tasks for these alerts.
-- alert_history.id is the task_id of the corresponding notification task.
delete from internal.tasks t
using resolved r
where t.task_id = r.id;
