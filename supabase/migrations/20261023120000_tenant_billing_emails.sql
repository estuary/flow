-- Store a list of billing contact emails per tenant.

begin;

alter table public.tenants rename column billing_email to billing_emails;

alter table public.tenants
    alter column billing_emails type text[]
        using case when billing_emails is null then '{}' else array[billing_emails] end,
    alter column billing_emails set default '{}',
    alter column billing_emails set not null;

comment on column public.tenants.billing_emails is
'Billing contact emails. The first is the Stripe customer email.';

create or replace function internal.on_tenant_billing_contact_update()
returns trigger
language plpgsql
security definer
set search_path to ''
as $$
begin
    if (old.billing_emails is distinct from new.billing_emails)
       or (old.billing_name is distinct from new.billing_name)
       or (old.billing_address is distinct from new.billing_address) then
        perform internal.wake_tenant_controller(new.tenant);
    end if;
    return new;
end;
$$;

commit;
