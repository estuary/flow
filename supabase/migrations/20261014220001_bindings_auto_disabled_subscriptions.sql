-- Existing subscriptions do not inherit new default alert types.
--
-- Apply this only once agents which decode the `bindings_auto_disabled` alert
-- type are deployed: older agents fail to decode `include_alert_types`.

update public.alert_subscriptions
set include_alert_types = array_append(
    include_alert_types,
    'bindings_auto_disabled'::public.alert_type
)
where not ('bindings_auto_disabled'::public.alert_type = any(include_alert_types));
