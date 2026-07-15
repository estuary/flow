begin;

-- Service accounts: non-login identities used for programmatic access
-- (CI/CD, automation) and scoped, time-limited access grants.

-- The `manage_service_accounts` bundle confers the fine-grained service-account
-- capabilities (query, create, mint/revoke API keys) without full team-admin.
alter type capability_bundle add value if not exists 'manage_service_accounts';

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

-- The GraphQL mutations reject service-account callers (verify_not_service_account),
-- but this RPC reaches the same table over PostgREST, letting a service account
-- self-mint a token. Deny that; legitimate minting uses separate inserts.
-- (this function is going away soon as part of the postgrest -> gql migration)
CREATE OR REPLACE FUNCTION public.create_refresh_token(multi_use boolean, valid_for interval, detail text DEFAULT NULL::text) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare
  secret text;
  refresh_token_row refresh_tokens;
begin
  if exists (select 1 from internal.service_accounts where user_id = auth_uid()) then
    raise 'service accounts cannot mint their own refresh tokens';
  end if;

  secret = gen_random_uuid();

  insert into refresh_tokens (detail, user_id, multi_use, valid_for, hash)
  values (
    detail,
    auth_uid(),
    multi_use,
    valid_for,
    crypt(secret, gen_salt('bf'))
  ) returning * into refresh_token_row;

  return json_build_object(
    'id', refresh_token_row.id,
    'secret', secret
  );
commit;
end
$$;

commit;
