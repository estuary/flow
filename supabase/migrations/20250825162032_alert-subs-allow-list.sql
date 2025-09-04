-- Changes the alert type filtering on `alert_subscriptions` to use an
-- allow-list instead of a deny-list. This allows us to beta test new alert
-- types by enabling notifications for a select subset of users.
begin;

-- Add the new include_alert_types column with default values
alter table public.alert_subscriptions
add column include_alert_types public.alert_type[] not null
default array['free_trial', 'free_trial_ending', 'free_trial_stalled', 'missing_payment_method', 'data_movement_stalled']::public.alert_type[];

comment on column public.alert_subscriptions.include_alert_types is
'Array of alert types that this subscription should include for receiving
notifications. Any alert type that is not included here will not result in a
notification being sent.';

-- Remove the exclude_alert_types column
alter table public.alert_subscriptions drop column exclude_alert_types;

-- Update the evaluate_alert_events function to use include_alert_types instead of exclude_alert_types
create or replace function internal.evaluate_alert_events() returns void
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
              and ca.alert_type = any(asub.include_alert_types)
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
              and oa.alert_type = any(asub.include_alert_types)
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

commit;
