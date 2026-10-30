-- Commit the new enum value before the subscription migration uses it.

alter type public.alert_type add value if not exists 'bindings_auto_disabled';
