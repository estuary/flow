begin;

alter table connector_tags add column default_capture_interval interval;
comment on column connector_tags.default_capture_interval is
  'The default value for the interval property for a Capture. This is normally used for non-streaming connectors';

alter table connector_tags add column disable_backfill boolean not null default false;
comment on column connector_tags.disable_backfill is
  'Controls if the UI will hide the backfill button for a connector';

commit;