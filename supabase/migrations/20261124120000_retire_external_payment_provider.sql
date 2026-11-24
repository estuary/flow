-- Every tenant's billing arrangement is now recorded by `plan_kind` and
-- `collection_method`, so `external` no longer describes any of them.

begin;

alter table public.tenants
    drop constraint "contracts have a payment provider other than external",
    add constraint "contracts have a payment provider" check (
        plan_kind <> 'contract' or payment_provider is not null
    ),
    add constraint "payment provider is not external" check (
        payment_provider is distinct from 'external'
    );

commit;
