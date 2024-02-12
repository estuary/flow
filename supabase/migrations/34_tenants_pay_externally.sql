begin;

create type payment_provider_type as enum (
  'stripe',
  'external'
);

comment on type payment_provider_type is '
Enumeration of which payment provider this tenant is using.
';

alter table tenants add column "payment_provider" payment_provider_type default 'stripe';

commit;
