begin;

-- Updates the new tenant trigger to only add newly inserted tenants to the
-- `estuary_support/` role, so that existing tenants can be removed from that
-- role without automatically getting re-added.
drop trigger "Grant support role access to tenants" on public.tenants;
drop function internal.update_support_role;

create function internal.update_support_role() returns trigger
language plpgsql as $$
begin
    insert into role_grants (subject_role, object_role, capability, detail)
    values (
        'estuary_support/',
        NEW.tenant,
        'admin',
        'Automagically grant support role access to new tenant'
    );
    return null;
end;
$$;

create trigger "Grant support role access to tenants" after insert on public.tenants for each row execute function internal.update_support_role();

commit;
