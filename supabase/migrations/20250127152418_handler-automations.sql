begin;

create function internal.create_handler_task(task_id public.flowid, task_type integer) returns void
language plpgsql security definer
as $$
begin
    insert into internal.tasks (task_id, task_type, wake_at, inbox)
    values (task_id, task_type, now(), array[
        json_build_array('0000000000000000', json_build_object('type', 'queued'))
    ]);
end;
$$;
comment on function internal.create_handler_task(task_id public.flowid, task_type integer) is
'Create a task with the given task_id and task_type, which must not exist.
The task is initially queued. Raises an error if the task already exists.';

-- Create tasks for any existing jobs that are still queued.
select internal.create_handler_task(p.id, 3)
from public.publications p
where p.job_status->>'type' = 'queued';

select internal.create_handler_task(d.id, 4)
from public.discovers d
where d.job_status->>'type' = 'queued';

select internal.create_handler_task(e.id, 5)
from public.evolutions e
where e.job_status->>'type' = 'queued';

select internal.create_handler_task(ad.id, 6)
from public.applied_directives ad
where ad.job_status->>'type' = 'queued';

select internal.create_handler_task(ct.id, 7)
from public.connector_tags ct
where ct.job_status->>'type' = 'queued';


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

create function internal.create_discover_task() returns trigger
LANGUAGE plpgsql SECURITY DEFINER
AS $$
begin
    execute internal.create_handler_task(new.id, 4);
    return null;
end;
$$;
create trigger create_discover_task after insert or update on public.discovers
for each row
when (new.job_status->>'type' = 'queued')
execute function internal.create_discover_task();

create function internal.create_evolution_task() returns trigger
LANGUAGE plpgsql SECURITY DEFINER
AS $$
begin
    execute internal.create_handler_task(new.id, 5);
    return null;
end;
$$;
create trigger create_evolution_task after insert or update on public.evolutions
for each row
when (new.job_status->>'type' = 'queued')
execute function internal.create_evolution_task();

create function internal.create_directive_task() returns trigger
LANGUAGE plpgsql SECURITY DEFINER
AS $$
begin
    execute internal.create_handler_task(new.id, 6);
    return null;
end;
$$;
-- Note that the trigger for applied directives requires that user_claims have been set.
-- This is important because typically that column will be populated by a separate update
-- after the row was created.
create trigger create_directive_task after insert or update on public.applied_directives
for each row
when (new.job_status->>'type' = 'queued' and new.user_claims is not null)
execute function internal.create_directive_task();

create function internal.create_connector_tag_task() returns trigger
LANGUAGE plpgsql SECURITY DEFINER
AS $$
begin
    execute internal.create_handler_task(new.id, 7);
    return null;
end;
$$;
create trigger create_connector_tag_task after insert or update on public.connector_tags
for each row
when (new.job_status->>'type' = 'queued')
execute function internal.create_connector_tag_task();

commit;
