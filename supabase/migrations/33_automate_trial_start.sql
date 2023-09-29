begin;

create or replace view internal.new_free_trial_tenants as
select
    tenants.tenant as tenant,
    max(daily_stats.usage_seconds / (60.0 * 60)) as max_daily_usage_hours,
    max(monthly_stats.usage_seconds / (60.0 * 60)) as max_monthly_usage_hours,
    max(ceil((monthly_stats.bytes_written_by_me + monthly_stats.bytes_read_by_me) / (1024.0 * 1024 * 1024))) as max_monthly_gb,
    count(distinct live_specs.id) filter (where live_specs.spec_type = 'capture') as today_captures,
    count(distinct live_specs.id) filter (where live_specs.spec_type = 'materialization') as today_materializations
from catalog_stats_monthly as monthly_stats
join catalog_stats_daily as daily_stats on daily_stats.catalog_name = monthly_stats.catalog_name
join tenants on tenants.tenant = split_part(monthly_stats.catalog_name,'/',1)||'/'
join live_specs on (live_specs.catalog_name = monthly_stats.catalog_name and (live_specs.spec #>> '{shards,disable}')::boolean is not true)
where tenants.trial_start is null
group by tenants.tenant
having (
  (
    max(daily_stats.usage_seconds / (60.0 * 60)) > (2 * 24) or
    max(monthly_stats.usage_seconds / (60.0 * 60)) > (24 * 30 * 2) or
    max((monthly_stats.bytes_written_by_me + monthly_stats.bytes_read_by_me) / (1024.0 * 1024 * 1024)) > 10
  ) and (
    count(distinct live_specs.id) filter (where live_specs.spec_type = 'capture') > 0 or
    count(distinct live_specs.id) filter (where live_specs.spec_type = 'materialization') > 0
  )
);

create or replace function internal.set_new_free_trials()
returns integer as $$
declare
    tenant_row record;
    update_count integer = 0;
begin
    for tenant_row in select tenant from internal.new_free_trial_tenants loop
      update tenants set trial_start = date_trunc('day', now())
      where tenants.tenant = tenant_row.tenant;

      -- INSERT statements set FOUND true if at least one row is affected, false if no row is affected.
      if found then
        update_count = update_count + 1;
      end if;
    end loop;
    return update_count;
end
$$ language plpgsql volatile;

create extension if not exists pg_cron with schema extensions;
select cron.schedule(
  'free-trials', -- name of the cron job
  '0 05 * * *', -- Every day at 05:00Z
  $$ select internal.set_new_free_trials() $$
);

commit;