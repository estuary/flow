-- This migration creates the tables needed for materialization
-- of task stats into the control plane.

create type task_type as enum ('capture', 'derivation', 'materialization');

-- The `task_stats` table is _not_ identical to what the connector would have created.
-- They have slightly different column types to make things a little more ergonomic and consistent.

create table task_stats (
    flow_document   json         not null,
    hourstamp       timestamptz  not null,
    shard_split     macaddr8     not null,
    task_name       catalog_name not null,
    task_type       task_type    not null
) partition by list (substring(task_name for position('/' in task_name)));
alter table task_stats enable row level security;

create policy "Users must be authorized to the catalog task name"
  on task_stats as permissive for select
  using (auth_catalog(task_name, 'admin'));
grant select on task_stats to authenticated;

comment on table task_stats is
    'Statistics for catalog tasks and their shards';
comment on column task_stats.flow_document is
    'Aggregated statistics document for the given shard split and hour';
comment on column task_stats.hourstamp is
    'The aggregated UTC hour of the stats';
comment on column task_stats.shard_split is '
Split of the catalog task shard.

Split values compose the beginning value of the shard''s key range
with the beginning value of the shard''s rClock range.
';
comment on column task_stats.task_name is
    'Name of the catalog task';
comment on column task_stats.task_type is '
The type of catalog task to which stats pertain.

One of "capture", "derivation", or "materialization".
';

do $$
begin
    if not exists (select from pg_catalog.pg_roles where rolname = 'stats_loader') then
        create role stats_loader with login password 'stats_loader_password';
   end if;
end
$$;

create schema task_stat_partitions;
comment on schema task_stat_partitions is
    'Private schema which holds per-tenant partitions of task_stats.';

grant create, usage on schema task_stat_partitions to stats_loader;
