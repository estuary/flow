-- The purpose of this is to cleanup old drafts, draft_specs, discovers, and evolutions.
-- These things can pile up over time, and there's no need to retain them for a long time.
-- Note that the cleanup of draft_specs, discovers, and evolutions happens due to cascading
-- deletions from drafts.

-- We need to add the foreign key constraint to evolutions, since it was not there originally.
alter table evolutions add foreign key (draft_id) references drafts(id) on delete cascade;

create or replace function internal.delete_old_drafts()
returns bigint as $$
  -- These CTE shennanigans brought to you by this rando:
  -- https://stackoverflow.com/a/47857304
  with d as (
    delete from drafts where updated_at < (now() - '30 days'::interval) returning *
  )
  select count(*) from d;
$$ language sql security definer;

-- create extension if not exists pg_cron with schema extensions;
-- select cron.schedule(
-- 	'delete-drafts', -- name of the cron job
-- 	'0 15 * * *', -- Every day at 15:00Z (midmorning EST)
-- 	$$ delete from drafts $$
-- );
