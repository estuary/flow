begin;

create type flow_type as enum (
  -- These correspond 1:1 with top-level maps of models::Catalog.
  'capture',
  'collection',
  'materialization',
  'test',
  -- These do not
  'source_capture'
);

-- This works because the `flow_type` enum is a superset of the `catalog_spec_type` enum.
-- This approach was taken from:
-- https://www.munderwood.ca/index.php/2015/05/28/altering-postgresql-columns-from-one-enum-to-another/
alter table live_spec_flows
alter column flow_type set data type flow_type
using flow_type::text::flow_type;

-- We now allow live specs to be deleted even though they're still referenced by other specs.
-- This means we need to relax the foreign key constraints on `live_spec_flows`.
alter table live_spec_flows
drop constraint live_spec_flows_source_id_fkey;
alter table live_spec_flows
drop constraint live_spec_flows_target_id_fkey;

-- TODO: add index on `controller_jobs` to make it easier to deque jobs by `next_run < now()` or `controller_version`
alter table live_specs add column controller_next_run timestamptz;
comment on column live_specs.controller_next_run is 'The next time the controller for this spec should run.';

-- Create a partial covering index on live specs to facilitate querying for the next controller run.
-- This is used by `deque` in `agent-sql/src/controllers.rs`.
create index live_specs_controller_next_run on live_specs(controller_next_run)
include (id)
where controller_next_run is not null;



-- This constraint is removed because we're changing how we represent deleted specs, so that only
-- the `spec` column is null. Setting `spec_type` to null was unnecessary, and retaining it is
-- now necessary in order for `live_spec_flows` to stay consistent with `live_specs` in case of
-- spec deletions that don't draft all the connected specs.
alter table live_specs drop constraint "spec and spec_type must be consistent";

-- TODO: this needs more thought. The goal is to make `spec_type` non-nullsable.
-- Any deleted rows should be cleaned up automatically by the first controller run.
update live_specs set spec_type = 'test' where spec_type is null;
alter table live_specs alter column spec_type set not null;

-- Allow spec_type to remain non-null for deleted specs
alter table publication_specs drop constraint "spec and spec_type must be consistent";


create table controller_jobs (
    -- The name of the live spec that this pertains to
    live_spec_id flowid not null references live_specs (id) on delete cascade,
    -- The version of the controller that last updated this row. Used to identify controllers to run
    -- whenever we update the controller code. Is compared to the `agent::controllers::CONTROLLER_VERSION`
    -- constant.
    controller_version integer not null default 0,

    -- Arbitrary JSON that's updated by the controller. Can be used as state for the controller,
    -- and also for communicating status to end users.
    status json not null default '{}'::json,
    -- Informational only
    updated_at timestamptz not null default now(),

    -- Always use the same logs_token for each controller, so the logs from all runs are in one place
    logs_token uuid not null default gen_random_uuid(),

    -- Error handling still needs more consideration
    failures integer not null default 0,
    -- Errors executing the controller will be shown here
    error text,

    primary key (live_spec_id)
);


insert into controller_jobs (live_spec_id)
    select id from live_specs;

commit;
