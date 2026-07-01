begin;

-- Service accounts: non-login identities used for programmatic access
-- (CI/CD, automation) and scoped, time-limited access grants.

create table internal.service_accounts (
  user_id       uuid primary key references auth.users (id),
  catalog_name  public.catalog_name not null,
  created_by    uuid not null references auth.users (id),
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now()
);

comment on table internal.service_accounts is
  'Non-login identities that authenticate via refresh tokens and are authorized through user_grants.';

create unique index service_accounts_catalog_name_key on internal.service_accounts
  (catalog_name);

create index service_accounts_catalog_name_spgist on internal.service_accounts
  using spgist ((catalog_name::text));

alter table public.refresh_tokens
  add column created_by uuid references auth.users (id);

comment on column public.refresh_tokens.created_by is
  'User who minted the token on another identity''s behalf (service-account '
  'credentials). Null for self-minted human tokens.';

commit;
