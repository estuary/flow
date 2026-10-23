-- Wake a tenant's controller when its plan_kind, collection_method or
-- payment_provider changes. Together they decide whether the tenant needs a
-- payment method, which the controller's quota raise and alerts read. A tenant
-- that needs none may have a suspended controller, which this wake resumes
-- once the tenant needs one.

begin;

create or replace function internal.on_tenant_payment_provider_update()
returns trigger
language plpgsql
security definer
set search_path to ''
as $$
begin
    if (old.plan_kind, old.collection_method, old.payment_provider)
       is distinct from (new.plan_kind, new.collection_method, new.payment_provider) then
        perform internal.wake_tenant_controller(new.tenant);
    end if;
    return new;
end;
$$;

commit;
