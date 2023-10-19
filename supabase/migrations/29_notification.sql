create table notification_preferences (
  like internal._model including all,

  catalog_prefix    catalog_prefix                 not null,
  user_id           uuid references auth.users(id)
);
alter table notification_preferences enable row level security;

create policy "Users access preferences for the prefixes they admin"
  on notification_preferences as permissive
  using (exists(
    select 1 from auth_roles('admin') r where catalog_prefix ^@ r.role_prefix
  ));

grant select, insert, update, delete on notification_preferences to authenticated;

-- TODO: Consider whether the notification_messages table should be renamed to notification_templates
--   or whether there should be a flag indicating that a message is, in fact, a template (i.e., in need of string manipulation).
create table internal.notification_messages (
  classification          text,
  title                   text,
  message                 text,
  confirmation_title      text,
  confirmation_message    text,
  primary key (classification)
);

insert into internal.notification_messages (classification, title, message, confirmation_title, confirmation_message)
  values
    (
      'data-not-processed-in-interval',
      'Estuary Flow: Alert for {spec_type} {catalog_name}',
      '<p>You are receiving this alert because your task, {spec_type} {catalog_name} hasn''t seen new data in {evaluation_interval}.  You can locate your task here to make changes or update its alerting settings.</p>',
      'Estuary Flow: Alert for {spec_type} {catalog_name}',
      '<p>You are receiving this alert because your task, {spec_type} {catalog_name} has resumed processing data.  You can locate your task here to make changes or update its alerting settings.</p>'
    );

-- TODO: Consider renaming the `acknowledged` column. Potential name alternatives include, but are not limited to, the following: alerting, firing, active.
create table notification_subscriptions (
  catalog_prefix         catalog_prefix not null,
  classification         text           not null,
  acknowledged           boolean        not null default false,
  evaluation_interval    interval,
  live_spec_id           flowid,
  primary key (catalog_prefix, classification)
);
alter table notification_subscriptions enable row level security;

create policy "Users access subscriptions for the prefixes they admin"
  on notification_subscriptions as permissive
  using (exists(
    select 1 from auth_roles('admin') r where catalog_prefix ^@ r.role_prefix
  ));

grant insert (catalog_prefix, classification, acknowledged, evaluation_interval, live_spec_id) on notification_subscriptions to authenticated;
grant update (acknowledged, evaluation_interval) on notification_subscriptions to authenticated;
grant select, delete on notification_subscriptions to authenticated;

create view notification_preferences_ext as
select
  notification_preferences.*,
  auth.users.email as verified_email
from notification_preferences
  left join auth.users on notification_preferences.user_id = auth.users.id
order by notification_preferences.catalog_prefix asc;
grant select on notification_preferences_ext to authenticated;

create view notification_subscriptions_ext as
select
  notification_subscriptions.catalog_prefix,
  notification_subscriptions.acknowledged,
  notification_subscriptions.evaluation_interval,
  internal.notification_messages.title as notification_title,
  internal.notification_messages.message as notification_message,
  internal.notification_messages.confirmation_title,
  internal.notification_messages.confirmation_message,
  internal.notification_messages.classification,
  notification_preferences_ext.verified_email,
  live_specs.catalog_name,
  live_specs.spec_type,
  coalesce(sum(catalog_stats_hourly.bytes_written_by_me + catalog_stats_hourly.bytes_written_to_me + catalog_stats_hourly.bytes_read_by_me), 0)::bigint as bytes_processed
from notification_subscriptions
  left join live_specs on notification_subscriptions.live_spec_id = live_specs.id and live_specs.spec is not null and (live_specs.spec->'shards'->>'disable')::boolean is not true
  left join catalog_stats_hourly on live_specs.catalog_name = catalog_stats_hourly.catalog_name
  left join notification_preferences_ext on notification_subscriptions.catalog_prefix = notification_preferences_ext.catalog_prefix
  left join internal.notification_messages on notification_subscriptions.classification = internal.notification_messages.classification
where (
  case
    when internal.notification_messages.classification = 'data-not-processed-in-interval' and notification_subscriptions.evaluation_interval is not null then
      live_specs.created_at <= date_trunc('hour', now() - notification_subscriptions.evaluation_interval)
      and catalog_stats_hourly.ts >= date_trunc('hour', now() - notification_subscriptions.evaluation_interval)
  end
)
group by
  notification_subscriptions.catalog_prefix,
  notification_subscriptions.acknowledged,
  notification_subscriptions.evaluation_interval,
  internal.notification_messages.title,
  internal.notification_messages.message,
  internal.notification_messages.confirmation_title,
  internal.notification_messages.confirmation_message,
  internal.notification_messages.classification,
  notification_preferences_ext.verified_email,
  live_specs.catalog_name,
  live_specs.spec_type;
grant select on notification_subscriptions_ext to authenticated;

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