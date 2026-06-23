-- Cutover step for `data_plane_private_links`: the data-plane controller now
-- reads desired links from the table directly, so the transition projection
-- into the `data_planes.private_links` column is no longer needed. Replace the
-- trigger function with a wake-only version that still sends the controller a
-- converge promptly on any link change.
--
-- The now-unused `private_links` column is intentionally left in place. Dropping
-- it together with the legacy `*_link_endpoints` columns (all four are projected
-- by the `data_planes_overview` view and the agent-api endpoint resolvers) is a
-- single follow-up cleanup, so the reporting view is recreated once rather than
-- per column.
--
-- Deploy ordering: roll out the controller binary that reads links from the
-- table before applying this migration, so a controller still reading the
-- column keeps seeing projected updates until it is replaced.

begin;

create or replace function internal.on_data_plane_private_links_change() returns trigger
    language plpgsql security definer
    set search_path to ''
    as $$
declare
    v_controller_task_id public.flowid;
begin
    select dp.controller_task_id into v_controller_task_id
    from public.data_planes dp
    where dp.id = coalesce(new.data_plane_id, old.data_plane_id);

    if v_controller_task_id is not null then
        perform internal.send_to_task(
            v_controller_task_id,
            '00:00:00:00:00:00:00:00'::public.flowid,
            '"converge"'::json
        );
    end if;

    return null;
end;
$$;

commit;
