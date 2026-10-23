begin;

alter type public.payment_provider_type add value if not exists 'aws_marketplace';
alter type public.payment_provider_type add value if not exists 'gcp_marketplace';
alter type public.payment_provider_type add value if not exists 'snowflake_marketplace';

create type public.plan_kind_type as enum ('self_serve', 'contract', 'internal');

create type public.collection_method_type as enum ('charge_default_payment_method', 'send_invoice');

alter table public.tenants
    add column plan_kind public.plan_kind_type not null default 'self_serve',
    add column collection_method public.collection_method_type not null default 'charge_default_payment_method',
    -- `external` doesn't identify who bills a contract.
    add constraint "contracts have a payment provider other than external" check (
        plan_kind <> 'contract'
        or (payment_provider is not null and payment_provider <> 'external')
    );

comment on column public.tenants.plan_kind is '
Billing arrangement of the tenant:
- self_serve: billed for usage.
- contract: billed under a contract, including fixed plans purchased through a marketplace.
- internal: not billed.';

comment on column public.tenants.collection_method is 'How usage-based invoices are collected.';

commit;
