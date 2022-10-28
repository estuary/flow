-- This migration creates the tables needed for materialization
-- of task stats into the control plane.

create type grain as enum ('monthly', 'daily', 'hourly');

-- The `catalog_stats` table is _not_ identical to what the connector would have created.
-- They have slightly different column types to make things a little more ergonomic and consistent.

create table catalog_stats (
    name                catalog_name not null,
    grain               timestamptz  not null,
    bytes_written_by    bigint       not null,
    bytes_read_by       bigint       not null,
    bytes_written_to    bigint       not null,
    bytes_read_from     bigint       not null,
    ts                  timestamptz  not null,
    flow_document       json         not null
) partition by list (substring(name for position('/' in name)));
alter table catalog_stats enable row level security;

create policy "Users must be authorized to the catalog name"
  on catalog_stats as permissive for select
  using (auth_catalog(name, 'admin'));
grant select on catalog_stats to authenticated;

-- TODO(whb): Finish these comments.
comment on table catalog_stats is
    'Statistics for catalogs and their shards';
comment on column catalog_stats.flow_document is
    'Aggregated statistics document for the given catalog name and grain';

do $$
begin
    if not exists (select from pg_catalog.pg_roles where rolname = 'stats_loader') then
        create role stats_loader with login password 'stats_loader_password';
   end if;
end
$$;

create schema catalog_stat_partitions;
comment on schema catalog_stat_partitions is
    'Private schema which holds per-tenant partitions of catalog_stats.';

grant create, usage on schema catalog_stat_partitions to stats_loader;
