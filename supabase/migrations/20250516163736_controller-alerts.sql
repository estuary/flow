-- Migration in support of controller-based alerts, along with
-- performance improvements to existing alert views so that they
-- can be evaluated more frequently.
begin;

drop function internal.evaluate_alert_events;

create function internal.cleanup_old_alerts() returns integer
language sql security definer
as $$
   with d as (
        delete from public.alert_history
        where fired_at < (now() - '1 year'::interval) and resolved_at is not null
        returning resolved_at
   )
   select count(*) from d;
$$;
comment on function internal.cleanup_old_alerts() is
  'Removes resolved alert history records older than 1 year to prevent unbounded growth';

-- Run the function once now in order to cleanup the history before we alter the
-- table (which will require re-writing all the existing rows)
select internal.cleanup_old_alerts();

-- resolved_arguments was already jsonb, so this is just making it consistent.
-- Also, jsonb is _much_ more convenient when you need to use it in a `group by`.
alter table public.alert_history alter column arguments type jsonb using arguments::text::jsonb;

-- This index is intended for use by planned agent-api endpoints
create index alert_history_open_alerts on public.alert_history (catalog_name) where resolved_at is null;
comment on index public.alert_history_open_alerts is
  'Partial index for efficiently querying open alerts (where resolved_at is null)';

-- Add a computed column to controller_jobs that says whether any alert is
-- currently present in the status. This will be used to build a partial index
-- covering controllers that have alerts, to make the `controller_alerts` view
-- perfomant. Note that an alert may be present in the status, but be in a state
-- other than `firing`, and we need those to be included in the index. This
-- allows controller alerts to have separate `resolved_args` when an alert
-- resolves. We're relying on the serialization behavior of controller statuses,
-- which excludes the `alerts` property if it's empty (there's no easy way to check
-- whether the object has any properties without requiring a separate function).
alter table public.controller_jobs
add column has_alert boolean generated always as (jsonb_path_exists(status::jsonb, '$.alerts')) stored;
comment on column public.controller_jobs.has_alert is
'Computed column indicating whether any alerts are currently present for this
controller job. Note that a present alert may not necessarily be firing, but it
will at least need to be looked at by the evaluate_alert_events function as it
could provide resolved_arguments.';

create index controller_alerts_index on public.controller_jobs (live_spec_id)
where has_alert is true;
comment on index public.controller_alerts_index is
'Partial index for efficiently querying controller jobs that have firing alerts';

-- Drop and re-create all the existing alert views to:
-- - remove `recipients` from `arguments`, which will now be added by the `evaluate_alert_events` function
-- - Change the data type of `arguments` from `json` to `jsonb` to be consistent with `resolved_arguments`
-- - Add a new controller_alerts view
--
-- Apart from that, the individual alert views have the same common columns:
-- alert_type alert_type
-- catalog_name catalog_name
-- arguments jsonb
-- firing boolean
drop view public.alert_all;
drop view internal.alert_free_trial;
drop view internal.alert_free_trial_ending;
drop view internal.alert_free_trial_stalled;
drop view internal.alert_missing_payment_method;
drop view internal.alert_data_movement_stalled;

-- This view is essentially the same except for removing the `recipients` from
-- the arguments, and no longer needing to join alert_subscriptions or
-- auth.users.
create view internal.alert_data_movement_stalled as
 select 'data_movement_stalled'::public.alert_type as alert_type,
    alert_data_processing.catalog_name,
    jsonb_build_object(
      'bytes_processed', coalesce(
        sum(catalog_stats_hourly.bytes_written_by_me +
          catalog_stats_hourly.bytes_written_to_me +
          catalog_stats_hourly.bytes_read_by_me),
        0)::bigint,
      'evaluation_interval', alert_data_processing.evaluation_interval,
      'spec_type', live_specs.spec_type
    ) as arguments,
    true as firing
   from public.alert_data_processing
   left join public.live_specs
       on alert_data_processing.catalog_name = live_specs.catalog_name
         and live_specs.spec IS NOT NULL
         and (live_specs.spec->'shards'->>'disable')::boolean is not true
     left join public.catalog_stats_hourly
       on alert_data_processing.catalog_name = catalog_stats_hourly.catalog_name
       and catalog_stats_hourly.ts >= date_trunc('hour', now() - alert_data_processing.evaluation_interval)
  where live_specs.created_at <= date_trunc('hour', now() - alert_data_processing.evaluation_interval)
  group by alert_data_processing.catalog_name, alert_data_processing.evaluation_interval, live_specs.spec_type
 having coalesce(sum(catalog_stats_hourly.bytes_written_by_me + catalog_stats_hourly.bytes_written_to_me + catalog_stats_hourly.bytes_read_by_me), 0)::bigint = 0;

-- The separate tenant alert views for free_trial* alerts are here combined into
-- a single view. This was done in order to improve performance.
create view internal.tenant_alerts as
  with all_tenants as (
    select
      tenant,
      trial_start,
      bool_or(customers."invoice_settings/default_payment_method" IS NOT NULL or tenants.payment_provider = 'external') as has_payment_info
    from public.tenants
    left join stripe.customers on customers.name = tenants.tenant::text
    group by tenant, trial_start
  ),
  missing_payment_method as (
    select tenant, trial_start, not has_payment_info as firing
    from all_tenants
  ),
  free_trial as (
    select
      tenant,
      trial_start,
      has_payment_info,
      trial_start is not null and trial_start < now() and (now() - trial_start) < '1 month'::interval as firing
    from all_tenants
  ),
  trial_ending as (
    select
        tenant,
        trial_start,
        has_payment_info,
        case
        when (now() - trial_start) >= ('1 month'::interval - '5 days'::interval)
            and (now() - trial_start) < ('1 month'::interval - '4 days'::interval)
            then 'free_trial_ending'::public.alert_type
        when (now() - trial_start) >= ('1 month'::interval + '5 days'::interval)
            and not has_payment_info
            then 'free_trial_stalled'::public.alert_type
        else null
        end as alert_type
    from all_tenants
    where trial_start is not null and trial_start < now()
  ),
  alerting as (
    select tenant, trial_start, has_payment_info, alert_type, true as firing
    from trial_ending
    where alert_type is not null
    union all
    select
      tenant,
      trial_start,
      false as has_payment_info,
      'missing_payment_method'::public.alert_type as alert_type,
      firing
    from missing_payment_method
    union all
    select
      tenant,
      trial_start,
      has_payment_info,
      'free_trial'::public.alert_type as alert_type,
      firing
    from free_trial
  )
  select
    (alerting.tenant::text || 'alerts/' || alerting.alert_type)::public.catalog_name as catalog_name,
    alerting.alert_type as alert_type,
    jsonb_build_object(
      'tenant', alerting.tenant,
      'trial_start', alerting.trial_start,
      'trial_end', (alerting.trial_start + '1 mon'::interval),
      'has_credit_card', alerting.has_payment_info,
      'plan_state',
      case
        when alerting.trial_start is null then 'free_tier'
        when (now() - alerting.trial_start) < '1 month'::interval then 'free_trial'
        else 'paid'
      end
    ) as arguments,
    alerting.firing
  from alerting;
comment on view internal.tenant_alerts is
'View of tenant-level alerts including free trial notifications and missing payment method alerts.
These alerts are combined into a single query for performance reasons.';

-- Add the new view of alerts firing from controllers.
create view internal.controller_alerts as
select alert_status.key::public.alert_type as alert_type,
ls.catalog_name,
alert_status.value::jsonb as arguments,
alert_status.value->>'state' = 'firing' as firing
from public.controller_jobs cj
join public.live_specs ls on cj.live_spec_id = ls.id
join lateral (
  select key, value
  from json_each(json_extract_path(cj.status, 'alerts')) a_status
  -- Filter out any alerts from the status that aren't valid `alert_type`s.
  -- This allows us to deploy agent versions supporting new alert types before
  -- running a database migration to add to the `alert_type` enum.
  where a_status.key = any(enum_range(null::public.alert_type)::text[])
) alert_status on true
where cj.has_alert;
comment on view internal.controller_alerts is
'View of alerts from controller jobs, extracted from the status JSON.
Alerts in this view may be either firing or resolved.';

create view internal.alert_all AS
  select
    catalog_name,
    alert_type,
    arguments,
    firing
  from internal.tenant_alerts
  union all
    select
      catalog_name,
      alert_type,
      arguments,
      firing
    from internal.alert_data_movement_stalled
  union all
    select
      catalog_name,
      alert_type,
      arguments,
      firing
    from internal.controller_alerts;
comment on view internal.alert_all is
'Unified view of all alert types. Alerts in this view can be either firing or
not. This view is not intended to be queried by end users, as it can be rather
slow.';

-- Allow users to opt out of certain alert types. See:
-- https://github.com/estuary/flow/issues/2188
alter table public.alert_subscriptions add column exclude_alert_types public.alert_type[];
comment on column public.alert_subscriptions.exclude_alert_types IS 'Array of alert types that this subscription should exclude from receiving notifications';

create function internal.evaluate_alert_events() returns void
    language plpgsql security definer
    as $$
  begin

    with all_alerts as (
      select alert_type, catalog_name, arguments, firing
      from internal.alert_all
    ),
    current_alerts as (
      select alert_type, catalog_name, arguments, firing
      from all_alerts
      where firing = true
    ),
    open_alerts as (
      select alert_type, catalog_name, fired_at, arguments
      from public.alert_history
      where resolved_at is null
    ),
    new_alerts as (
      insert into public.alert_history (alert_type, catalog_name, fired_at, arguments)
      select
        ca.alert_type,
        ca.catalog_name,
        now(),
        jsonb_set(
          ca.arguments,
          '{recipients}',
          coalesce(
            jsonb_agg(
              jsonb_build_object(
                'email', asub.email,
                'full_name', au.raw_user_meta_data->>'full_name'
              ) order by asub.email
            ) filter (
              where asub.email is not null
              and (asub.exclude_alert_types is null or not ca.alert_type = any(asub.exclude_alert_types))
            ),
            '[]'::jsonb
          )
        )
      from current_alerts ca
      left join public.alert_subscriptions asub on starts_with(ca.catalog_name, asub.catalog_prefix)
      left join auth.users au on asub.email = au.email and au.is_sso_user is false
      left join open_alerts oa on
        ca.alert_type = oa.alert_type and
        ca.catalog_name = oa.catalog_name
      where oa.alert_type is null -- filter out alerts that are already firing
      group by ca.alert_type, ca.catalog_name, ca.arguments
      returning fired_at
    ),
    resolving_alerts as (
      select
        oa.alert_type,
        oa.catalog_name,
        oa.fired_at,
        jsonb_set(
          -- Prefer to use `alert_all.arguments` if present (which would be the
          -- case when the alert has a row present with `firing = false`). This
          -- is so that the resolved_arguments will be the most up-to-date
          -- arguments.
          coalesce(aa.arguments, oa.arguments),
          '{recipients}',
          coalesce(
            jsonb_agg(
              jsonb_build_object(
                'email', asub.email,
                'full_name', au.raw_user_meta_data->>'full_name'
              ) order by asub.email
            ) filter (
              where asub.email is not null
              and (asub.exclude_alert_types is null or not oa.alert_type = any(asub.exclude_alert_types))
            ),
            '[]'::jsonb
          )
        )::json as resolved_arguments
       from open_alerts oa
       left join all_alerts aa on oa.alert_type = aa.alert_type and oa.catalog_name = aa.catalog_name
       left join public.alert_subscriptions asub on starts_with(oa.catalog_name, asub.catalog_prefix)
       left join auth.users au on asub.email = au.email and au.is_sso_user is false
       where aa.alert_type is null or not aa.firing
       group by oa.alert_type, oa.catalog_name, oa.fired_at, aa.arguments, oa.arguments
     )
     -- Update alert_history to resolve alerts that are no longer firing
     update public.alert_history
       set resolved_at = now(), resolved_arguments = ra.resolved_arguments
       from resolving_alerts ra
       where public.alert_history.alert_type = ra.alert_type
         and public.alert_history.catalog_name = ra.catalog_name
         and public.alert_history.fired_at = ra.fired_at;

   end;
   $$;

comment on function internal.evaluate_alert_events() is
'Processes alert state changes: creates new alert history entries for newly
firing alerts and resolves alerts that are no longer firing. Adds recipient
information to both new and resolved alerts based on alert_subscriptions.';

-- Replace the trigger so that alerts get sent to the new cloudrun service.
-- The URL is the only thing that's changed here.
create or replace function internal.send_alerts() returns trigger
    language plpgsql
    as $$
declare
  token text;
begin
  select decrypted_secret into token from vault.decrypted_secrets where name = 'alert-email-fn-shared-secret' limit 1;
    perform
      net.http_post(
        'https://alerts-1084703453822.us-central1.run.app/',
        to_jsonb(new.*),
        headers:=format('{"Content-Type": "application/json", "Authorization": "Basic %s"}', token)::jsonb,
        timeout_milliseconds:=90000
      );
  return null;
end;
$$;

commit;
