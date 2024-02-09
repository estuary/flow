begin;

create type alert_type as enum (
  'free_trial',
  'free_trial_ending',
  'free_trial_stalled',
  'missing_payment_method',
  'data_movement_stalled'
);

-- In order to allow alerts to contain arguments after they're done firing
-- we need to refactor alerts to contain a `firing` boolean, rather than
-- simply omitting no-longer-firing alerts from the view.
create type alert_snapshot as (
  alert_type alert_type,
  catalog_name catalog_name,
  arguments json,
  firing boolean
);

create or replace view internal.alert_free_trial as
select
  'free_trial' as alert_type,
  (tenants.tenant || 'alerts/free_trial')::catalog_name as catalog_name,
  json_build_object(
    'tenant', tenants.tenant,
    'recipients', array_agg(json_build_object(
      'email', alert_subscriptions.email,
      'full_name', auth.users.raw_user_meta_data->>'full_name'
    )),
    'trial_start', tenants.trial_start::date,
    'trial_end', (tenants.trial_start + interval '1 month')::date,
    'has_credit_card', stripe.customers."invoice_settings/default_payment_method" is not null
  ) as arguments,
  -- Since we don't need to communicate post-alert arguments, we can instead
  -- simply omit tenants that are no longer in their free trial, and mark the
  -- those that are as firing.
  true as firing
from tenants
  left join alert_subscriptions on alert_subscriptions.catalog_prefix ^@ tenants.tenant and email is not null
  left join stripe.customers on stripe.customers."name" = tenants.tenant
  -- Filter out sso users because auth.users is only guarinteed unique when that is false:
  -- CREATE UNIQUE INDEX users_email_partial_key ON auth.users(email text_ops) WHERE is_sso_user = false;
  left join auth.users on auth.users.email = alert_subscriptions.email and auth.users.is_sso_user is false
where tenants.trial_start is not null and
  -- Select for tenants currently in their free trials
  -- meaning trial start is at most 1 month ago
  (now() - tenants.trial_start) < interval '1 month' and
  -- Filter out unexpected future start dates
  tenants.trial_start <= now()
group by
    tenants.tenant,
    tenants.trial_start,
    alert_subscriptions.email,
    customers.name,
    users.raw_user_meta_data,
    stripe.customers."invoice_settings/default_payment_method";

-- Trigger 5 days before trial ends
create or replace view internal.alert_free_trial_ending as
select
  'free_trial_ending' as alert_type,
  (tenants.tenant || 'alerts/free_trial_ending')::catalog_name as catalog_name,
  json_build_object(
    'tenant', tenants.tenant,
    'recipients', array_agg(json_build_object(
      'email', alert_subscriptions.email,
      'full_name', auth.users.raw_user_meta_data->>'full_name'
    )),
    'trial_start', tenants.trial_start::date,
    'trial_end', (tenants.trial_start + interval '1 month')::date,
    'has_credit_card', stripe.customers."invoice_settings/default_payment_method" is not null
  ) as arguments,
  true as firing
from tenants
  left join alert_subscriptions on alert_subscriptions.catalog_prefix ^@ tenants.tenant and email is not null
  left join stripe.customers on stripe.customers."name" = tenants.tenant
  -- Filter out sso users because auth.users is only guarinteed unique when that is false:
  -- CREATE UNIQUE INDEX users_email_partial_key ON auth.users(email text_ops) WHERE is_sso_user = false;
  left join auth.users on auth.users.email = alert_subscriptions.email and auth.users.is_sso_user is false
where tenants.trial_start is not null and
  -- e.g "You're >= 25 days into your trial but < 26 days"
  (now() - tenants.trial_start) >= (interval '1 month' - interval '5 days') and
  (now() - tenants.trial_start) < (interval '1 month' - interval '4 days') and
  -- Filter out unexpected future start dates
  tenants.trial_start <= now()
group by
    tenants.tenant,
    tenants.trial_start,
    alert_subscriptions.email,
    customers."invoice_settings/default_payment_method",
    users.raw_user_meta_data;

-- Alert us internally when they go past 5 days over the trial
create or replace view internal.alert_free_trial_stalled as
select
  'free_trial_stalled' as alert_type,
  (tenants.tenant || 'alerts/free_trial_stalled')::catalog_name as catalog_name,
  json_build_object(
    'tenant', tenants.tenant,
    'recipients', array_agg(json_build_object(
      'email', alert_subscriptions.email,
      'full_name', auth.users.raw_user_meta_data->>'full_name'
    )),
    'trial_start', tenants.trial_start::date,
    'trial_end', (tenants.trial_start + interval '1 month')::date
  ) as arguments,
  true as firing
from tenants
  left join alert_subscriptions on alert_subscriptions.catalog_prefix ^@ tenants.tenant and email is not null
  left join stripe.customers on stripe.customers."name" = tenants.tenant
  -- Filter out sso users because auth.users is only guarinteed unique when that is false:
  -- CREATE UNIQUE INDEX users_email_partial_key ON auth.users(email text_ops) WHERE is_sso_user = false;
  left join auth.users on auth.users.email = alert_subscriptions.email and auth.users.is_sso_user is false
where tenants.trial_start is not null and
  (now() - tenants.trial_start) >= (interval '1 month' + interval '5 days') and
    -- Filter out unexpected future start dates
  tenants.trial_start <= now() and
  stripe.customers."invoice_settings/default_payment_method" is null
group by
    tenants.tenant,
    tenants.trial_start,
    alert_subscriptions.email,
    customers.name,
    users.raw_user_meta_data;

-- We created this alert so we can notify when it _stops_ firing, i.e
-- when a tenant provides a payment method.
create or replace view internal.alert_missing_payment_method as
select
  'missing_payment_method' as alert_type,
  (tenants.tenant || 'alerts/missing_payment_method')::catalog_name as catalog_name,
  json_build_object(
    'tenant', tenants.tenant,
    'recipients', array_agg(json_build_object(
      'email', alert_subscriptions.email,
      'full_name', auth.users.raw_user_meta_data->>'full_name'
    )),
    'trial_start', tenants.trial_start::date,
    'trial_end', (tenants.trial_start + interval '1 month')::date,
    -- if tenants.trial_start is null, that means they entered their cc
    -- while they're still in the free tier
    'plan_state', (
      case
        when tenants.trial_start is null then 'free_tier'
        when (now() - tenants.trial_start) < interval '1 month' then 'free_trial'
        else 'paid'
      end
    )
  ) as arguments,
  (stripe.customers."invoice_settings/default_payment_method" is null) as firing
from tenants
  left join alert_subscriptions on alert_subscriptions.catalog_prefix ^@ tenants.tenant and email is not null
  left join stripe.customers on stripe.customers."name" = tenants.tenant
  -- Filter out sso users because auth.users is only guarinteed unique when that is false:
  -- CREATE UNIQUE INDEX users_email_partial_key ON auth.users(email text_ops) WHERE is_sso_user = false;
  left join auth.users on auth.users.email = alert_subscriptions.email and auth.users.is_sso_user is false
group by
    tenants.tenant,
    tenants.trial_start,
    alert_subscriptions.email,
    customers."invoice_settings/default_payment_method",
    users.raw_user_meta_data;

-- Have to update this to join in auth.users for full_name support
create or replace view internal.alert_data_movement_stalled as
select
  'data_movement_stalled' as alert_type,
  alert_data_processing.catalog_name as catalog_name,
  json_build_object(
    'bytes_processed', coalesce(sum(catalog_stats_hourly.bytes_written_by_me + catalog_stats_hourly.bytes_written_to_me + catalog_stats_hourly.bytes_read_by_me), 0)::bigint,
    'recipients', array_agg(json_build_object(
      'email', alert_subscriptions.email,
      'full_name', auth.users.raw_user_meta_data->>'full_name'
    )),
    'evaluation_interval', alert_data_processing.evaluation_interval,
    'spec_type', live_specs.spec_type
  ) as arguments,
  true as firing
from alert_data_processing
  left join live_specs on alert_data_processing.catalog_name = live_specs.catalog_name and live_specs.spec is not null and (live_specs.spec->'shards'->>'disable')::boolean is not true
  left join catalog_stats_hourly on alert_data_processing.catalog_name = catalog_stats_hourly.catalog_name and catalog_stats_hourly.ts >= date_trunc('hour', now() - alert_data_processing.evaluation_interval)
  left join alert_subscriptions on alert_data_processing.catalog_name ^@ alert_subscriptions.catalog_prefix and email is not null
  -- Filter out sso users because auth.users is only guarinteed unique when that is false:
  -- CREATE UNIQUE INDEX users_email_partial_key ON auth.users(email text_ops) WHERE is_sso_user = false;
  left join auth.users on auth.users.email = alert_subscriptions.email and auth.users.is_sso_user is false
where live_specs.created_at <= date_trunc('hour', now() - alert_data_processing.evaluation_interval)
group by
  alert_data_processing.catalog_name,
  alert_data_processing.evaluation_interval,
  alert_subscriptions.email,
  live_specs.spec_type,
  users.raw_user_meta_data
having coalesce(sum(catalog_stats_hourly.bytes_written_by_me + catalog_stats_hourly.bytes_written_to_me + catalog_stats_hourly.bytes_read_by_me), 0)::bigint = 0;

create or replace view alert_all as
  select row(internal.alert_free_trial.*)::alert_snapshot from internal.alert_free_trial
  union all select row(internal.alert_free_trial_ending.*)::alert_snapshot from internal.alert_free_trial_ending
  union all select row(internal.alert_free_trial_stalled.*)::alert_snapshot from internal.alert_free_trial_stalled
  union all select row(internal.alert_missing_payment_method.*)::alert_snapshot from internal.alert_missing_payment_method
  union all select row(internal.alert_data_movement_stalled.*)::alert_snapshot from internal.alert_data_movement_stalled;

-- Keep track of the alert arguments at the time it was resolved
alter table alert_history add column resolved_arguments jsonb;

create or replace function internal.evaluate_alert_events()
returns void as $$
begin

  -- Create alerts which have transitioned from !firing => firing
  with open_alerts as (
    select alert_type, catalog_name from alert_history
    where resolved_at is null
  )
  insert into alert_history (alert_type, catalog_name, fired_at, arguments)
    select alert_type, catalog_name, now(), arguments from alert_all
    where alert_all_firing.firing and
          (alert_type, catalog_name) not in (select * from open_alerts);

  -- Resolve alerts that have transitioned from firing => !firing
  with open_alerts as (
    select alert_type, catalog_name, fired_at from alert_history
    where resolved_at is null
  ),
  -- Find all open_alerts for which either there is not a row in alerts_all,
  -- or there is but its firing field is false.
  closing_alerts as (
    select
      alert_type,
      catalog_name,
      fired_at,
      coalesce(allert_all.arguments, null)
    from open_alerts
    left join alert_all on
      alert_all.alert_type = open_alerts.alert_type and
      alert_all.catalog_name = open_alerts.catalog_name
      and not alert_all.firing
    where
      -- The open alert is no longer in alert_all, therefore it's no longer firing
      alert_all.alert_type is null or
      -- The open is still tracked, but it has stopped firing
      not alert_all.firing
  )
  update alert_history
    set resolved_at = now(),
        resolved_arguments = closing_alerts.arguments
    from closing_alerts
    where alert_history.alert_type = closing_alerts.alert_type
      and alert_history.catalog_name = closing_alerts.catalog_name
      and alert_history.fired_at = closing_alerts.fired_at;

end;
$$ language plpgsql security definer;

create or replace function internal.send_alerts()
returns trigger as $trigger$
declare
  token text;
begin
  select decrypted_secret into token from vault.decrypted_secrets where name = 'alert-email-fn-shared-secret' limit 1;
    -- Skip all of the past events that got triggered when we added these new event types
    -- NOTE: Change this so that the date is the day (or time) that it's deployed
    -- so that only "real" events that happen after deployment get sent
    -- if new.fired_at > '2024-01-30'
  perform
    net.http_post(
      'http://host.docker.internal:5431/functions/v1/alerts',
      -- 'https://eyrcnmuzzyriypdajwdk.supabase.co/functions/v1/alerts',
      to_jsonb(new.*),
      headers:=format('{"Content-Type": "application/json", "Authorization": "Basic %s"}', token)::jsonb
    );
  -- end if;
  return null;
end;
$trigger$ LANGUAGE plpgsql;

create or replace trigger "Send email after alert fired" after insert on alert_history
  for each row execute procedure internal.send_alerts();

create or replace trigger "Send email after alert resolved" after update on alert_history
  for each row when (old.resolved_at is null and new.resolved_at is not null) execute procedure internal.send_alerts();

commit;