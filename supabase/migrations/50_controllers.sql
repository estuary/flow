begin;


create type flow_type as enum (
  -- These correspond 1:1 with catalog_spec_type.
  'capture',
  'collection',
  'materialization',
  'test',
  -- These do not
  'source_capture'
);
comment on type flow_type is
  'Represents the type of a dependency of one spec on another. This enum is a
  strict superset of catalog_spec_type, for historical reasons.';

-- This cast, specifically the `as assignment`, is required to allow the old version
-- of control plane to continue to insert `catalog_spec_type`s into the `flow_type`
-- column.
create cast (catalog_spec_type as flow_type) with inout as assignment;

-- This works because the `flow_type` enum is a superset of the `catalog_spec_type` enum.
-- This approach was taken from:
-- https://www.munderwood.ca/index.php/2015/05/28/altering-postgresql-columns-from-one-enum-to-another/
alter table live_spec_flows
alter column flow_type set data type flow_type
using flow_type::flow_type;

-- Update live_spec_flows foreign keys to add `on delete cascade`
alter table live_spec_flows
drop constraint live_spec_flows_source_id_fkey;
alter table live_spec_flows
drop constraint live_spec_flows_target_id_fkey;

alter table live_spec_flows
add constraint live_spec_flows_source_id_fkey
foreign key(source_id) references live_specs(id) on delete cascade;
alter table live_spec_flows
add constraint live_spec_flows_target_id_fkey
foreign key(target_id) references live_specs(id) on delete cascade;

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
-- spec deletions that don't draft all the connected specs. Note that spec_type columns are still
-- nullable to maintain compatibility with old agent versions during the transition.
alter table live_specs drop constraint "spec and spec_type must be consistent";
alter table draft_specs drop constraint "spec and spec_type must be consistent";
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

comment on table controller_jobs is
  'Controller jobs reflect the state of the automated background processes that
  manage live specs. Controllers are responsible for things like updating
  inferred schemas, activating and deleting shard and journal specs in the data
  plane, and any other type of background automation.';

comment on column controller_jobs.live_spec_id is
  'The id of the live_specs row that this contoller job pertains to.';
comment on column controller_jobs.controller_version is
  'The version of the controller that last ran. This number only increases
  monotonically, and only when a breaking change to the controller status
  is released. Every controller_job starts out with a controller_version of 0,
  and will subsequently be upgraded to the current controller version by the
  first controller run.';
comment on column controller_jobs.status is
  'Contains type-specific information about the controller and the actions it
  has performed.';
comment on column controller_jobs.updated_at is
  'Timestamp of the last update to the controller_job.';
comment on column controller_jobs.logs_token is
  'Token that can be used to query logs from controller runs from
  internal.log_lines.';
comment on column controller_jobs.failures is
  'Count of consecutive failures of this controller. This is reset to 0 upon
  any successful controller run. If failures is > 0, then error will be set';
comment on column controller_jobs.error is 
  'The error from the most recent controller run, which will be null if the
  run was successful. If this is set, then failures will be > 0';

insert into controller_jobs (live_spec_id)
    select id from live_specs;

alter table controller_jobs enable row level security;

create policy "Users must be authorized to live specifications"
  on controller_jobs as permissive for select
  using (
    live_spec_id in (select id from live_specs)
  );
grant select on live_specs to authenticated;

commit;
