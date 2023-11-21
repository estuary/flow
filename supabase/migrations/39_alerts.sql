create table alert_subscriptions (
  like internal._model including all,

  catalog_prefix    catalog_prefix                 not null,
  email             text
);
alter table alert_subscriptions enable row level security;

create policy "Users access subscriptions for the prefixes they admin"
  on alert_subscriptions as permissive
  using (exists(
    select 1 from auth_roles('admin') r where catalog_prefix ^@ r.role_prefix
  ));

grant select, insert, update, delete on alert_subscriptions to authenticated;

create table alert_data_processing (
  catalog_name           catalog_name not null,
  evaluation_interval    interval     not null,
  primary key (catalog_name)
);
alter table alert_data_processing enable row level security;

create policy "Users access alerts for admin-authorized tasks"
  on alert_data_processing as permissive
  using (exists(
    select 1 from auth_roles('admin') r where catalog_name ^@ r.role_prefix
  ));

grant update (evaluation_interval) on alert_data_processing to authenticated;
grant select, insert, delete on alert_data_processing to authenticated;

create table alert_history (
  alert_type      text         not null,
  catalog_name    catalog_name not null,
  fired_at        timestamptz  not null,
  resolved_at     timestamptz,
  arguments       json         not null,
  primary key (alert_type, catalog_name, fired_at)
);
alter table alert_history enable row level security;

create policy "Users access alert history for admin-authorized tasks"
  on alert_history as permissive
  using (exists(
    select 1 from auth_roles('admin') r where catalog_name ^@ r.role_prefix
  ));

grant select on alert_history to authenticated;

create view internal.alert_data_processing_firing as
select
  alert_data_processing.*,
  'data_not_processed_in_interval' as alert_type,
  alert_subscriptions.email,
  live_specs.spec_type,
  coalesce(sum(catalog_stats_hourly.bytes_written_by_me + catalog_stats_hourly.bytes_written_to_me + catalog_stats_hourly.bytes_read_by_me), 0)::bigint as bytes_processed
from alert_data_processing
  left join live_specs on alert_data_processing.catalog_name = live_specs.catalog_name and live_specs.spec is not null and (live_specs.spec->'shards'->>'disable')::boolean is not true
  left join catalog_stats_hourly on alert_data_processing.catalog_name = catalog_stats_hourly.catalog_name and catalog_stats_hourly.ts >= date_trunc('hour', now() - alert_data_processing.evaluation_interval)
  left join alert_subscriptions on alert_data_processing.catalog_name ^@ alert_subscriptions.catalog_prefix and email is not null
where live_specs.created_at <= date_trunc('hour', now() - alert_data_processing.evaluation_interval)
group by
  alert_data_processing.catalog_name,
  alert_data_processing.evaluation_interval,
  alert_subscriptions.email,
  live_specs.spec_type
having coalesce(sum(catalog_stats_hourly.bytes_written_by_me + catalog_stats_hourly.bytes_written_to_me + catalog_stats_hourly.bytes_read_by_me), 0)::bigint = 0;

create view alert_all_firing as
select
  internal.alert_data_processing_firing.catalog_name,
  internal.alert_data_processing_firing.alert_type,
  json_build_object(
    'bytes_processed', internal.alert_data_processing_firing.bytes_processed,
    'emails', array_agg(internal.alert_data_processing_firing.email),
    'evaluation_interval', internal.alert_data_processing_firing.evaluation_interval,
    'spec_type', internal.alert_data_processing_firing.spec_type
    ) as arguments
from internal.alert_data_processing_firing
group by
  internal.alert_data_processing_firing.catalog_name,
  internal.alert_data_processing_firing.alert_type,
  internal.alert_data_processing_firing.bytes_processed,
  internal.alert_data_processing_firing.evaluation_interval,
  internal.alert_data_processing_firing.spec_type
order by catalog_name asc;

create or replace function internal.evaluate_alert_events()
returns void as $$
begin

-- Create alerts which have transitioned from !firing => firing
with open_alerts as (
  select alert_type, catalog_name from alert_history
  where resolved_at is null
)
insert into alert_history (alert_type, catalog_name, fired_at, arguments)
  select alert_type, catalog_name, now(), arguments from alert_all_firing
  where (alert_type, catalog_name) not in (select * from open_alerts);

-- Resolve alerts that have transitioned from firing => !firing
with open_alerts as (
  select alert_type, catalog_name from alert_all_firing
)
update alert_history set resolved_at = now()
    where resolved_at is null and (alert_type, catalog_name) not in (select * from open_alerts);

end;
$$ language plpgsql security definer;

create extension if not exists pg_cron with schema extensions;
select
  cron.schedule (
    'evaluate-alert-events', -- name of the cron job
    '*/3 * * * *', -- every three minutes, update alert event history
    $$ perform internal.evaluate_alert_events() $$
  );

create extension if not exists pg_net with schema extensions;
create or replace function internal.send_alerts()
returns trigger as $trigger$
begin

if new.alert_type = 'data_not_processed_in_interval' then
  perform
    net.http_post(
      url:='http://host.docker.internal:5431/functions/v1/alert-data-processing',
      headers:='{"Content-Type": "application/json", "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0"}'::jsonb,
      body:=concat('{"time": "', now(), '"}')::jsonb
    );
end if;

return null;

end;
$trigger$ LANGUAGE plpgsql;

create trigger "Send alerts" after insert or update on alert_history
  for each row execute procedure internal.send_alerts();
