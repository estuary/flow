-- Enables sending emails for shard_failed alerts.
-- Prior to this being run, these alerts will not be added to alert_history.
begin;

alter type public.alert_type add value 'shard_failed';

commit;
