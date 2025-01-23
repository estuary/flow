-- NOTE: Before this is run, we must ensure that all live_specs rows have an non-null controller_task_id, and
-- that no more legacy agents are running.
-- Completes the transition to automation-based controllers
-- and removes the old `live_specs.controller_next_run` column.
begin;

alter table public.live_specs
alter column controller_task_id
set
    not null;

alter table public.live_specs
alter column controller_task_id
    set
        default internal.id_generator();

alter table public.live_specs
drop column controller_next_run;


-- Update the inferred schema trigger function to drop support for legacy controller notifications.
CREATE or replace FUNCTION internal.on_inferred_schema_update() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare
    controller_task_id flowid;
begin

    -- There's no need to have a `for update of ls` clause here because
    -- `send_to_task` does not modify the `live_specs` table.
    select ls.controller_task_id into controller_task_id
    from public.live_specs ls
    where ls.catalog_name = new.collection_name and ls.spec_type = 'collection';
    if controller_task_id is not null then
        perform internal.send_to_task(
            controller_task_id,
            '00:00:00:00:00:00:00:00'::flowid,
            '{"type":"inferred_schema_updated"}'::json
        );
    end if;

return null;
end;
$$;

commit;
