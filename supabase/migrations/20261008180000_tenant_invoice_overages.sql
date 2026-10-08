begin;

alter table public.tenants
    add column invoice_overages boolean not null default false;

comment on column public.tenants.invoice_overages is
    'Preference for invoicing usage overages instead of charging the payment method on file. Invoice collection does not yet consult this preference.';

commit;
