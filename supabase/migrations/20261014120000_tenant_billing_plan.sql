begin;

-- Marketplace providers sit alongside `external`, which remains valid for
-- self-serve tenants until each externally-paid tenant is classified into a
-- plan kind and provider.
alter type public.payment_provider_type add value if not exists 'aws_marketplace';
alter type public.payment_provider_type add value if not exists 'gcp_marketplace';
alter type public.payment_provider_type add value if not exists 'snowflake_marketplace';

create type public.plan_kind_type as enum ('self_serve', 'contract', 'internal');

create type public.collection_method_type as enum ('charge_default_payment_method', 'send_invoice');

alter table public.tenants
    add column plan_kind public.plan_kind_type not null default 'self_serve',
    add column collection_method public.collection_method_type not null default 'charge_default_payment_method',
    -- `external` doesn't identify who bills a contract, so classifying a
    -- tenant out of self-serve requires choosing a concrete provider.
    add constraint "non-self-serve plans have a classified payment provider" check (
        plan_kind = 'self_serve'
        or (payment_provider is not null and payment_provider <> 'external')
    );

comment on column public.tenants.plan_kind is '
Billing arrangement of the tenant:
- self_serve: billed for usage, with free-tier, trial, and paid phases defined by trial_start.
- contract: billed under a contract (including fixed plans purchased through a marketplace),
  with included usage given by zero-priced leading tiers of data_tiers and usage_tiers.
- internal: not billed.';

comment on column public.tenants.collection_method is '
How usage-based invoices are collected: every invoice of a self_serve tenant, and usage
overage invoices of a contract tenant. Stripe-billed contract fees are always sent as invoices.
- charge_default_payment_method: charge the default payment method, or send the invoice
  when there is no default payment method.
- send_invoice: send the invoice to be paid manually, even if a payment method is on file.';

commit;
