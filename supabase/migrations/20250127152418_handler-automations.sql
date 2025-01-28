
begin;



create function internal.create_handler_task(task_id public.flowid, task_type integer) returns void
language plpgsql security definer
as $$
begin
    insert into internal.tasks (task_id, task_type)
    values (task_id, task_type);

    execute internal.send_to_task(task_id, '0000000000000000'::public.flowid, json_build_object(
        'type', 'queued'
    ));
end;
$$;


create function internal.create_publication_task() returns trigger
LANGUAGE plpgsql SECURITY DEFINER
AS $$
begin
    execute internal.create_handler_task(new.id, 3);
    return null;
end;
$$;
create trigger create_publication_task after insert or update on public.publications
for each row
when (new.job_status->>'type' = 'queued')
execute function internal.create_publication_task();

-- create function internal.create_discover_task() returns trigger
-- LANGUAGE plpgsql SECURITY DEFINER
-- AS $$
-- begin
--     execute internal.create_handler_task(new.id, 4);
--     return null;
-- end;
-- $$;
-- create trigger create_discover_task after insert or update on public.discovers
-- for each row
-- when (new.job_status->>'type' = 'queued')
-- execute function internal.create_discover_task();

-- create function internal.create_directive_task() returns trigger
-- LANGUAGE plpgsql SECURITY DEFINER
-- AS $$
-- begin
--     execute internal.create_handler_task(new.id, 5);
--     return null;
-- end;
-- $$;
-- create trigger create_directive_task after insert or update on public.applied_directives
-- for each row
-- when (new.job_status->>'type' = 'queued')
-- execute function internal.create_directive_task();

-- create function internal.create_connector_tag_task() returns trigger
-- LANGUAGE plpgsql SECURITY DEFINER
-- AS $$
-- begin
--     execute internal.create_handler_task(new.id, 6);
--     return null;
-- end;
-- $$;
-- create trigger create_connector_tag_task after insert or update on public.connector_tags
-- for each row
-- when (new.job_status->>'type' = 'queued')
-- execute function internal.create_connector_tag_task();


commit;
