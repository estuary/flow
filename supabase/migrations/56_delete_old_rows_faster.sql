begin;

-- Delete old drafts, which will cascade to draft_specs, draft_errors, and discovers
create function internal.delete_old_drafts()
returns integer as $$
    with d as (
        delete from public.drafts where updated_at < (now() - '10 days'::interval) returning id
    )
    select count(id) from d;
$$ language sql security definer;

comment on function internal.delete_old_drafts is
'deletes drafts, discovers, draft_specs, and draft_errors rows that have aged out';

select cron.schedule(
    'delete-drafts',
    '7 * * * *', -- Every hour at 7 minutes past
    $$ select internal.delete_old_drafts() $$
);

-- Delete old hourly stats
create function internal.delete_old_hourly_stats()
returns integer as $$
    with s as (
        delete from catalog_stats_hourly where grain = 'hourly' and ts < (now() - '30 days'::interval) returning ts
    )
    select count(ts) from s;
$$ language sql security definer;

comment on function internal.delete_old_hourly_stats is
'deletes catalog_stats_hourly rows that have aged out';

select cron.schedule(
    'delete-hourly-stats',
    '19 * * * *', -- Every hour at 19 minutes past
    $$ select internal.delete_old_hourly_stats() $$
);

-- Delete old log lines
create function internal.delete_old_log_lines()
returns integer as $$
    with l as (
        delete from internal.log_lines where logged_at < (now() - '2 days'::interval) returning logged_at
    )
    select count(*) from l;
$$ language sql security definer;

comment on function internal.delete_old_log_lines is
'deletes internal.log_lines rows that have aged out';

select cron.schedule(
    'delete-log-lines',
    '27 * * * *', -- Every hour at 27 minutes past
    $$ select internal.delete_old_log_lines() $$
);

-- The pgcron extenstion records each run in the job_run_details table.
-- It does not clean these up automatically, but recommends creating a cron job to do it.
-- https://github.com/citusdata/pg_cron/blob/9490f9cc9803f75105f2f7d89839a998f011f8d8/README.md#viewing-job-run-details
create function internal.delete_old_cron_runs()
returns integer as $$
    with r as (
        delete from cron.job_run_details where end_time < now() - '10 days'::interval returning runid
    )
    select count(*) from r;
$$ language sql security definer;

comment on function internal.delete_old_cron_runs is
'deletes cron.job_run_details rows that have aged out.';

select cron.schedule(
    'delete-job-run-details',
    '0 12 * * *', -- Every day at 12:00Z
    $$ select internal.delete_old_cron_runs() $$
);

drop function internal.delete_old_rows;

commit;
