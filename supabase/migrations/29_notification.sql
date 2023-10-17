create table notification_preferences (
  like internal._model including all,

  prefix           text                           not null,
  subscribed_by    uuid references auth.users(id) not null,
  user_id          uuid references auth.users(id)
);
alter table notification_preferences enable row level security;

create policy "Users select preferences for the prefixes they admin"
  on notification_preferences as permissive for select
  using (exists(
    select 1 from auth_roles('admin') r where prefix ^@ r.role_prefix
  ));
create policy "Users insert preferences for the prefixes they admin"
  on notification_preferences as permissive for insert
  with check (exists(
    select 1 from auth_roles('admin') r where prefix ^@ r.role_prefix
  ));
create policy "Users update preferences for the prefixes they admin"
  on notification_preferences as permissive for update
  using (exists(
    select 1 from auth_roles('admin') r where prefix ^@ r.role_prefix
  ));
create policy "Users delete preferences for the prefixes they admin"
  on notification_preferences as permissive for delete
  using (exists(
    select 1 from auth_roles('admin') r where prefix ^@ r.role_prefix
  ));

grant select, insert, update, delete on notification_preferences to authenticated;

create table notification_messages (
  like internal._model including all,

  title     text,
  message   text
);
grant select on notification_messages to authenticated;

insert into notification_messages (detail, title, message)
  values
    (
      'data-not-processed-in-interval',
      'Estuary Flow: Alert for {spec_type} {catalog_name}',
      '<p>You are receiving this alert because your task, {spec_type} {catalog_name} hasn''t seen new data in {notification_interval}.  You can locate your task here to make changes or update its alerting settings.</p>'
    );

create table notifications (
  like internal._model including all,

  method_id                  flowid            not null,
  message_id                 flowid            not null,
  acknowledged               boolean           not null default false,
  evaluation_interval        interval,
  live_spec_id               flowid
);
grant insert (detail, live_spec_id, method_id, message_id, evaluation_interval) on notifications to authenticated;
grant update (evaluation_interval, acknowledged) on notifications to authenticated;
grant select, delete on notifications to authenticated;

create view notification_preferences_ext as
select
  notification_preferences.*,
  auth.users.email as verified_email
from notification_preferences
  left join auth.users on notification_preferences.user_id = auth.users.id
order by notification_preferences.prefix asc;
grant select on notification_preferences_ext to authenticated;

create view notifications_ext as
select
  notifications.id as notification_id,
  notifications.evaluation_interval as evaluation_interval,
  notifications.acknowledged as acknowledged,
  notification_messages.title as notification_title,
  notification_messages.message as notification_message,
  notification_messages.detail as classification,
  notification_preferences_ext.id as preference_id,
  notification_preferences_ext.verified_email as verified_email,
  live_specs.id as live_spec_id,
  live_specs.catalog_name as catalog_name,
  live_specs.spec_type as spec_type
from notifications
  left join live_specs on notifications.live_spec_id = live_specs.id and (live_specs.spec->'shards'->>'disable')::boolean is not true
  left join notification_preferences_ext on notifications.method_id = notification_preferences_ext.id
  left join notification_messages on notifications.message_id = notification_messages.id;
grant select on notifications_ext to authenticated;

create type notification_query as (
  notification_id text,
  evaluation_interval interval,
  acknowledged boolean,
  notification_title text,
  notification_message text,
  verified_email text,
  catalog_name text,
  spec_type text
);

create type catalog_stat_query as (
  ts timestamptz,
  bytes_written_by_me bigint,
  bytes_written_to_me bigint,
  bytes_read_by_me bigint,
  bytes_read_from_me bigint
);

create or replace function internal.evaluate_data_processing_notifications()
returns void as $$
declare
  query notification_query;
  confirmation_pending notification_query[];
  alert_pending notification_query[];
  start_stat catalog_stat_query := null;
  end_stat catalog_stat_query := null;
  bytes_written_by bigint;
  bytes_written_to bigint;
  bytes_read_by bigint;
  bytes_read_from bigint;
begin

  for query in
    select
      notifications_ext.notification_id,
      notifications_ext.evaluation_interval,
      notifications_ext.acknowledged,
      notifications_ext.notification_title,
      notifications_ext.notification_message,
      notifications_ext.verified_email,
      live_specs.catalog_name,
      live_specs.spec_type
    from notifications_ext
      left join live_specs on notifications_ext.live_spec_id = live_specs.id
    where
      notifications_ext.classification = 'data-not-processed-in-interval'
      and live_specs.created_at <= date_trunc('hour', now() - notifications_ext.evaluation_interval)
  loop

  start_stat := (
    select
      catalog_stats_hourly.ts,
      catalog_stats_hourly.bytes_written_by_me,
      catalog_stats_hourly.bytes_written_to_me,
      catalog_stats_hourly.bytes_read_by_me,
      catalog_stats_hourly.bytes_read_from_me
    from catalog_stats_hourly
    where
      catalog_stats_hourly.catalog_name = query.catalog_name
      and catalog_stats_hourly.ts = date_trunc('hour', now() - notifications_ext.evaluation_interval)
  );

  end_stat := (
    select
      catalog_stats_hourly.ts,
      catalog_stats_hourly.bytes_written_by_me,
      catalog_stats_hourly.bytes_written_to_me,
      catalog_stats_hourly.bytes_read_by_me,
      catalog_stats_hourly.bytes_read_from_me
    from catalog_stats_hourly
    where
      catalog_stats_hourly.catalog_name = query.catalog_name
      and catalog_stats_hourly.ts = date_trunc('hour', now())
  );

  continue when start_stat is null or end_stat is null;

  bytes_written_by := end_stat.bytes_written_by_me - start_stat.bytes_written_by_me;
  bytes_written_to := end_stat.bytes_written_to_me - start_stat.bytes_written_to_me;

  bytes_read_by := end_stat.bytes_read_by_me - start_stat.bytes_read_by_me;
  bytes_read_from := end_stat.bytes_read_from_me - start_stat.bytes_read_from_me;

  if query.spec_type = 'capture' then
    if bytes_written_by > 0 then
      if query.acknowledged then
        -- Send confirmation email
        confirmation_pending := array_append(confirmation_pending, query);
      end if;
    else
      if not query.acknowledged then
        -- Send alert email
        alert_pending := array_append(alert_pending, query);
      end if;
    end if;
  end if;

  if query.spec_type = 'materialization' then
    if bytes_read_by > 0 then
      if query.acknowledged then
        -- Send confirmation email
        confirmation_pending := array_append(confirmation_pending, query);
      end if;
    else
      if not query.acknowledged then
        -- Send alert email
        alert_pending := array_append(alert_pending, query);
      end if;
    end if;
  end if;

  if query.spec_type = 'collection' then
    if bytes_written_by > 0 or bytes_written_to > 0 then
      if query.acknowledged then
        -- Send confirmation email
        confirmation_pending := array_append(confirmation_pending, query);
      end if;
    else
      if not query.acknowledged then
        -- Send alert email
        alert_pending := array_append(alert_pending, query);
      end if;
    end if;
  end if;

  start_stat := null;
  end_stat := null;
  end loop;

  -- Insert pending notification queries into request body
end;
$$ language plpgsql security definer;

create extension pg_cron with schema extensions;
select
  cron.schedule (
    'evaluate-data-processing-notifications', -- name of the cron job
    '*/5 * * * *', -- every five minutes, check to see if an alert needs to be sent
    $$
    select
      net.http_post(
        url:='http://host.docker.internal:5431/functions/v1/resend',
        headers:='{"Content-Type": "application/json", "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0"}'::jsonb,
        body:=concat('{"time": "', now(), '"}')::jsonb
      ) as request_id;
    $$
  );