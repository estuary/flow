-- Introduces a trigger that runs collection controllers in response to an inferred schema
-- update, so that inferred schemas are published promptly in response.
begin;

create or replace function internal.on_inferred_schema_update()
returns trigger as $$
begin

-- The least function is necessary in order to avoid delaying a controller job in scenarios
-- where there is a backlog of controller runs that are due.
update live_specs set controller_next_run = least(controller_next_run, now())
where catalog_name = new.collection_name and spec_type = 'collection';

return null;
end;
$$ language plpgsql security definer;

comment on function internal.on_inferred_schema_update is
    'Schedules a run of the controller in response to an inferred_schemas change.';

-- We need two separate triggers because we want to reference the
-- `old` row so we can avoid triggering the controller in response to
-- no-op updates, which happen frequently.
create or replace trigger inferred_schema_controller_update
after update on inferred_schemas
for each row
when (old.md5 is distinct from new.md5)
execute function internal.on_inferred_schema_update();

create or replace trigger inferred_schema_controller_insert
after insert on inferred_schemas
for each row
execute function internal.on_inferred_schema_update();

-- There's no `on delete` trigger because we only delete inferred_schemas by cascade from the
-- live_specs deletion.
commit;
