
-- Used by the UI in order to query alert subscriptions by tenant.
create index concurrently alert_subscriptions_catalog_prefix_idx on public.alert_subscriptions(catalog_prefix);
