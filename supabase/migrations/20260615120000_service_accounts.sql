begin;

-- Service accounts: non-login identities that authenticate via API keys.
-- Used for programmatic access (CI/CD, automation) and scoped, time-limited
-- access grants.

create table internal.service_accounts (
  user_id       uuid primary key references auth.users (id),
  -- `catalog_name` is a management anchor only: admins of a prefix covering
  -- this name may manage the service account (mint keys, revoke grants) and
  -- see it in listings. The account's *access* is determined solely by its
  -- user_grants rows, which are managed like any other user's and may span
  -- multiple prefixes.
  catalog_name  public.catalog_name not null,
  display_name  text not null,
  created_by    uuid not null references auth.users (id),
  last_used_at  timestamptz,
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now()
);

comment on table internal.service_accounts is
  'Non-login identities that authenticate via API keys and are authorized through user_grants.';

-- The serviceAccounts query scopes results to a caller's admin prefixes with
-- `catalog_name::text ^@ ANY($1)`. SP-GiST natively supports the `^@`
-- (starts-with) operator; a btree index would not be used by it.
create index service_accounts_catalog_name_spgist on internal.service_accounts
  using spgist ((catalog_name::text));

-- API keys: long-lived credentials for service accounts, presented directly
-- as Authorization: Bearer credentials. Keys are evaluated statefully ONLY:
-- every request re-verifies the key against this table, and a key is never
-- exchanged for a signed JWT — which is what makes revocation immediate.

create table internal.api_keys (
  id                  public.flowid primary key not null default internal.id_generator(),
  service_account_id  uuid not null references internal.service_accounts (user_id),
  -- Hex-encoded SHA-256 of the key secret. A fast hash (not bcrypt) is the
  -- right choice because stateful-only evaluation places verification in the
  -- per-request hot path, and a slow hash would buy nothing: secrets are
  -- high-entropy random values, so offline brute-force resistance is moot.
  -- See the minting site in graphql/service_accounts.rs.
  secret_hash         text not null,
  label               text not null,
  expires_at          timestamptz not null,
  created_by          uuid not null references auth.users (id),
  last_used_at        timestamptz,
  created_at          timestamptz not null default now(),
  -- Revocation stamps this rather than deleting the row, preserving the
  -- audit trail. Revoked keys are inert: excluded from bearer authentication
  -- and from listings.
  revoked_at          timestamptz
);

create index api_keys_service_account_id on internal.api_keys (service_account_id);

comment on table internal.api_keys is
  'Long-lived credentials for service accounts, verified statefully as bearer credentials on every request (never exchanged for JWTs).';

commit;
