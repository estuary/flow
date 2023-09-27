begin;
-- The purpose of this is to cleanup old drafts, draft_specs, discovers, evolutions, etc.
-- These things can pile up over time, and there's no need to retain them for a long time.
-- Note that the cleanup of draft_specs, discovers, and evolutions happens due to cascading
-- deletions from drafts.

-- We need to add the foreign key constraint to evolutions, since it was not there originally.
delete from evolutions e where not exists (select d.id from drafts d where d.id = e.draft_id);
alter table evolutions add foreign key (draft_id) references drafts(id) on delete cascade;

create or replace function internal.delete_old_rows()
returns jsonb as $$
declare
  n_drafts integer;
  n_logs integer;
  n_hourly_stats integer;
begin
  with d as (
    delete from public.drafts where updated_at < (now() - '10 days'::interval) returning *
  )
  select into n_drafts count(*) as n from d;

  -- log_lines gets a lot of volume, so we use a much shorter retention period with them.
  with l as (
    delete from internal.log_lines where logged_at < (now() - '2 days'::interval) returning *
  )
  select into n_logs count(*) as n from l;

  with s as (
    delete from catalog_stats_hourly where grain = 'hourly' and ts < (now() - '30 days'::interval) returning *
  )
  select into n_hourly_stats count(*) from s;

  return json_build_object(
    'drafts', coalesce(n_drafts, 0),
    'log_lines', coalesce(n_logs, 0),
    'catalog_stats_hourly', coalesce(n_hourly_stats, 0)
  );
end;
$$ language plpgsql security definer;

create extension if not exists pg_cron with schema extensions;
select cron.schedule(
  'delete-drafts', -- name of the cron job
  '0 05 * * *', -- Every day at 05:00Z
  $$ select internal.delete_old_rows() $$
);

commit;
