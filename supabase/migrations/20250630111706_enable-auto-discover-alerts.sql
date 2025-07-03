-- Enables sending emails for auto_discover_failed alerts.
-- Prior to this being run, these alerts will not be added to alert_history.
begin;

alter type public.alert_type add value 'auto_discover_failed';

commit;
