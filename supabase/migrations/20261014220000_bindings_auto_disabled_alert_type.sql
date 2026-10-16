-- Adds the `bindings_auto_disabled` alert type, which materialization
-- controllers fire while bindings remain disabled due to
-- `onIncompatibleSchemaChange`. Existing alert subscriptions are subscribed to
-- it in the following migration: `alter type add value` cannot be used in the
-- same transaction that subsequently references the new value.

alter type public.alert_type add value if not exists 'bindings_auto_disabled';
