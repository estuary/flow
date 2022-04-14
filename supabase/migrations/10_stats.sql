-- This migration creates the tables needed for materialization of task stats into the control
-- plane.

-- We start by creating the flow_checkpoints_v1 and flow_materializations_v2 tables that are used by
-- the materialization connector. We do this only so that we can restrict access to those tables
-- from within this migration, instead of needing to do that manually after the materialization is
-- applied. The create table statements were copied verbatim from the connector's output.

-- This table holds Flow processing checkpoints used for exactly-once processing of materializations
CREATE TABLE IF NOT EXISTS flow_checkpoints_v1 (
	-- The name of the materialization.
	materialization TEXT NOT NULL,
	-- The inclusive lower-bound key hash covered by this checkpoint.
	key_begin BIGINT NOT NULL,
	-- The inclusive upper-bound key hash covered by this checkpoint.
	key_end BIGINT NOT NULL,
	-- This nonce is used to uniquely identify unique process assignments of a shard and prevent them from conflicting.
	fence BIGINT NOT NULL,
	-- Checkpoint of the Flow consumer shard, encoded as base64 protobuf.
	checkpoint TEXT,

	PRIMARY KEY(materialization, key_begin, key_end)
);
-- RLS is enabled to prevent clients from viewing or altering the state of materializations.
alter table flow_checkpoints_v1 enable row level security;

-- This table is the source of truth for all materializations into this system.
CREATE TABLE IF NOT EXISTS flow_materializations_v2 (
	-- The name of the materialization.
	materialization TEXT NOT NULL,
	-- Version of the materialization.
	version TEXT NOT NULL,
	-- Specification of the materialization, encoded as base64 protobuf.
	spec TEXT NOT NULL,

	PRIMARY KEY(materialization)
);
-- RLS is enabled to prevent clients from viewing or altering the state of materializations.
alter table flow_materializations_v2 enable row level security;

create type task_type as enum ('capture', 'derivation', 'materialization');

-- The `task_stats_*` tables are _not_ identical to what the connector would have created.
-- They have slightly different column types to make things a little more ergonomic and consistent.

create table task_stats_by_minute (
    kind task_type not null,
    name catalog_name not null,
    key_begin char(8) not null,
    rclock_begin char(8) not null,
    ts timestamptz not null,
    -- We're using the JSON column type here instead of JSONB because our postgres materialization
    -- connector fails to insert into JSONB columns for some reason. See:
    -- https://github.com/estuary/connectors/issues/163
    -- In any case, plain JSON seems fine for now, since we are primarily just reading or writing
    -- the complete document. Note that we cannot use the json_obj domain type here because the Go
    -- postgres driver seems to choke on it for some reason.
    flow_document json not null,

    primary key (name, key_begin, rclock_begin, ts)
);

comment on table task_stats_by_minute is
    'stats for each task shard aggregated by the minute';
comment on column task_stats_by_minute.kind is
    'the type of task to which the stats pertain';
comment on column task_stats_by_minute.name is
    'Name of the task';
comment on column task_stats_by_minute.key_begin is
    'The beginning of the key range that is served by the shard';
comment on column task_stats_by_minute.rclock_begin is
    'The beginning of the rclock range that is served by the shard';
comment on column task_stats_by_minute.ts is
    'The UTC timestamp corresponding to the beginning of the time range for the stats document';
comment on column task_stats_by_minute.flow_document is
    'Complete stats document';

grant select on task_stats_by_minute to authenticated;

create table task_stats_by_hour (like task_stats_by_minute including all);
comment on table task_stats_by_hour is
    'stats for each task shard aggregated by the hour';
grant select on task_stats_by_hour to authenticated;

create table task_stats_by_day (like task_stats_by_minute including all);
comment on table task_stats_by_day is
    'stats for each task shard aggregated by the day';
grant select on task_stats_by_day to authenticated;

