begin;

alter table connector_tags add column capture_interval text;
comment on column connector_tags.capture_interval is
  'The default value for the interval property for a Capture. This is ONLY used for non-streaming connectors';
alter table connector_tags add constraint `capture_interval must be a number with the format following`
    check (capture_interval ~ '^\d+(s|m|h)$');

alter table connector_tags add column disable_backfill boolean not null default false;
comment on column connector_tags.disable_backfill is
  'Controls if the UI will hide the backfill button for a connector';

commit;