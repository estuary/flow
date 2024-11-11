--- This is required in order to enable the pg_cron extension when running locally.
--- It's separate from the `compacted` migration so that it doesn't get clobbered the
--- next time someone compacts the migrations. Just copy this file over separately.
begin;

-- Example: enable the "pg_cron" extension
create extension pg_cron with schema pg_catalog;

grant usage on schema cron to postgres;
grant all privileges on all tables in schema cron to postgres;

commit;
