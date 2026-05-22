begin;

-- Service accounts: non-login identities that authenticate via API keys.
-- Used for programmatic access (CI/CD, automation) and scoped, time-limited
-- access grants.

create table internal.service_accounts (
  user_id       uuid primary key references auth.users (id),
  prefix        public.catalog_prefix not null,
  -- `capability` mirrors the legacy column on user_grants/role_grants and is
  -- slated for the same eventual retirement in favor of `bundles`. `'none'` is
  -- permitted so a service account can be authorized entirely by its bundles.
  capability    public.grant_capability not null
    constraint valid_capability check (
      capability = any (array[
        'none'::public.grant_capability,
        'read'::public.grant_capability,
        'write'::public.grant_capability,
        'admin'::public.grant_capability
      ])
    ),
  bundles       public.capability_bundle[] not null default '{}',
  display_name  text not null,
  created_by    uuid not null references auth.users (id),
  last_used_at  timestamptz,
  disabled_at   timestamptz,
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now()
);

comment on table internal.service_accounts is
  'Non-login identities that authenticate via API keys and are authorized through user_grants.';

-- The serviceAccounts query scopes results to a caller's admin prefixes with
-- `prefix::text ^@ ANY($1)`. SP-GiST natively supports the `^@` (starts-with)
-- operator; a btree index would not be used by it.
create index service_accounts_prefix_spgist on internal.service_accounts
  using spgist ((prefix::text));

-- API keys: long-lived credentials for service accounts, exchanged for short-lived JWTs
-- via the /api/v1/auth/token REST endpoint.

create table internal.api_keys (
  id                  public.flowid primary key not null default internal.id_generator(),
  service_account_id  uuid not null references internal.service_accounts (user_id),
  secret_hash         text not null,
  label               text not null,
  expires_at          timestamptz not null,
  created_by          uuid not null references auth.users (id),
  last_used_at        timestamptz,
  created_at          timestamptz not null default now()
);

create index api_keys_service_account_id on internal.api_keys (service_account_id);

comment on table internal.api_keys is
  'Long-lived credentials for service accounts, exchanged for short-lived JWTs via the token exchange endpoint.';

commit;
