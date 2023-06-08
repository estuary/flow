

create view internal.next_auto_discovers as
select
  live_specs.id as capture_id,
  live_specs.catalog_name as capture_name,
  live_specs.spec->'endpoint' as endpoint_json,
  -- These properties default to false, which matches the behavior in the models crate.
  coalesce((live_specs.spec->'autoDiscover'->>'addNewBindings')::boolean, false) as add_new_bindings,
  coalesce((live_specs.spec->'autoDiscover'->>'evolveIncompatibleCollections')::boolean, false) as evolve_incompatible_collections,
  connector_tags.id as connector_tags_id,
  -- If there's not been any discovers, then we use the capture creation time as the starting point, so that we don't auto-discover
  -- immediately after a capture is created. This is also required in order to effectively disable auto-discover by setting the
  -- auto_discover_interval to a really large value. Note that this expression must be consistent with the 'having' clause.
  now() - coalesce(max(discovers.updated_at), live_specs.created_at) + connector_tags.auto_discover_interval as overdue_interval
from live_specs
left join discovers on live_specs.catalog_name = discovers.capture_name
-- We can only perform discovers if we have the connectors and tags rows present.
-- I'd consider it an improvement if we could somehow refactor this to log a warning in cases where there's no connector_tag
inner join connectors
  on split_part(live_specs.spec->'endpoint'->'connector'->>'image', ':', 1) = connectors.image_name
inner join connector_tags
  on connectors.id = connector_tags.connector_id
  and ':' || split_part(live_specs.spec->'endpoint'->'connector'->>'image', ':', 2) = connector_tags.image_tag
where
  live_specs.spec_type = 'capture'
  -- We don't want to discover if shards are disabled
  and not coalesce((live_specs.spec->'shards'->>'disabled')::boolean, false)
  -- Any non-null value for autoDiscover will enable it.
  and live_specs.spec->'autoDiscover' is not null
group by live_specs.id, connector_tags.id
-- See comment on overdue_interval above
having now() - coalesce(max(discovers.updated_at), live_specs.created_at) > connector_tags.auto_discover_interval
-- This ordering isn't strictly necessary, but it
order by overdue_interval desc;

comment on view internal.next_auto_discovers is
'A view of captures that are due for an automatic discovery operation.
This is determined by comparing the time of the last discover operation
against the curent time';

comment on column internal.next_auto_discovers.capture_id is 'Primary key of the live_specs row for the capture';
comment on column internal.next_auto_discovers.capture_name is 'Catalog name of the capture';
comment on column internal.next_auto_discovers.endpoint_json is
'The endpoint configuration of the capture, to use with the next discover.';
comment on column internal.next_auto_discovers.add_new_bindings is
'Whether to add newly discovered bindings. If false, then it will only update existing bindings.';
comment on column internal.next_auto_discovers.evolve_incompatible_collections is 
'Whether to automatically perform schema evolution in the event that the newly discovered collections are incompatble.';
comment on column internal.next_auto_discovers.connector_tags_id is 
'The id of the connector_tags row that corresponds to the image used by this capture.';


create or replace function internal.create_auto_discovers()
returns integer as $$
declare
  support_user_id uuid = (select id from auth.users where email = 'support@estuary.dev');
  next_row internal.next_auto_discovers;
  total_created integer := 0;
  tmp_draft_id flowid;
  tmp_discover_id flowid;
begin

for next_row in select * from internal.next_auto_discovers
loop
  -- Create a draft, which we'll discover into
  insert into drafts (user_id) values (support_user_id) returning id into tmp_draft_id;
  
  insert into discovers (capture_name, draft_id, connector_tag_id, endpoint_config, update_only, auto_publish, auto_evolve)
  values (
    next_row.capture_name,
    tmp_draft_id,
    next_row.connector_tags_id,
    next_row.endpoint_json,
    not next_row.add_new_bindings,
    true,
    next_row.evolve_incompatible_collections
  ) returning id into tmp_discover_id;

  -- This is just useful when invoking the function manually.
  total_created := total_created + 1;
end loop;

return total_created;
end;
$$ language plpgsql security definer;

comment on function internal.create_auto_discovers is
'Creates discovers jobs for each capture that is due for an automatic discover. Each disocver will have auto_publish
set to true. The update_only and auto_evolve columns of the discover will be set based on the addNewBindings and
evolveIncompatibleCollections fields in the capture spec. This function is idempotent. Once a discover is created by
this function, the next_auto_discovers view will no longer include that capture until its interval has passed again.
So its safe to call this function at basically any frequency. The return value of the function is the count of newly
created discovers jobs.';


-- The following enables the regularly scheduled function that creates
-- discover jobs for captures with autoDiscover enabled. It's left commented
-- out here because it's actually rather inconvenient to run during local 
-- development. If you want to enable it locally, then just uncomment this
-- or run it manually. More often, it's more convenient during local
-- development to manually trigger this by calling create_auto_discovers()
-- whenever you want to trigger it.

-- create extension pg_cron with schema extensions;
-- Sets up the periodic check for captures that need discovered
-- select cron.schedule (
--     'create-discovers', -- name of the cron job
--     '*/5 * * * *', -- every 5 minutes, check to see if a discover needs run
--     $$ select internal.create_auto_discovers() $$
-- );

