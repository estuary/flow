-- Automatically migrate grants when a social-auth user re-authenticates via SSO.
--
-- GoTrue creates a new auth.users record for SAML SSO logins (no auto-linking).
-- This trigger detects the new SSO identity, finds the prior social account by
-- email, transfers eligible grants, and suspends the old account.
--
-- Grants on tenants with a *different* sso_provider_id are not transferred,
-- because they belong to a different SSO domain and the new user would not
-- be able to satisfy that tenant's SSO requirements.

begin;

-- Add sso_provider_id to tenants (nullable FK to auth.sso_providers).
alter table public.tenants
  add column sso_provider_id uuid references auth.sso_providers(id);

comment on column public.tenants.sso_provider_id is
  'SSO provider that governs this tenant, if any. NULL means no SSO requirement.';

-- Trigger function: migrate grants from old social user to new SSO user.
create or replace function internal.on_sso_identity_insert()
returns trigger
language plpgsql
security definer
-- https://www.postgresql.org/docs/current/sql-createfunction.html#SQL-CREATEFUNCTION-SECURITY
set search_path to ''
as $$
declare
  new_user_email  text;
  old_user_id     uuid;
  grant_row       record;
begin
  -- Look up the new SSO user's email.
  select email into new_user_email
    from auth.users
    where id = NEW.user_id;

  if new_user_email is null then
    return null;
  end if;

  -- Find the prior social-auth user with a matching email. OAuth auto-linking
  -- means there's typically one social account per email, but limit 1 guards
  -- against edge cases (e.g. duplicates from disabled linking or data fixes).
  select id into old_user_id
    from auth.users
    where email = new_user_email
      and id <> NEW.user_id
      and (is_sso_user = false or is_sso_user is null)
    order by created_at desc
    limit 1;

  if old_user_id is null then
    return null;
  end if;

  -- Process each grant on the old user. Use a lateral join to find
  -- the most specific tenant governing each grant's prefix.
  for grant_row in
    select ug.object_role, ug.capability, ug.detail,
           t.sso_provider_id as tenant_sso_provider_id
    from public.user_grants ug
    left join public.tenants t
      on t.tenant = split_part(ug.object_role::text, '/', 1) || '/'
    where ug.user_id = old_user_id
  loop
    if grant_row.tenant_sso_provider_id is not null
       and grant_row.tenant_sso_provider_id is distinct from substring(NEW.provider from 5)::uuid
    then
      -- Tenant has a different SSO provider: skip.
      null;
    else
      -- Transfer: upsert into new user's grants (capability only upgrades).
      insert into public.user_grants (user_id, object_role, capability, detail)
      values (
        NEW.user_id,
        grant_row.object_role,
        grant_row.capability,
        coalesce(grant_row.detail, 'migrated from social auth')
      )
      -- defensive on conflict here in case the user account somehow already has grants
      -- before the user logs in and creates the identity (maybe possible with SCIM provisioning)
      on conflict (user_id, object_role) do update
        set capability = greatest(
              public.user_grants.capability,
              excluded.capability
            ),
            updated_at = now(),
            detail = case
              when excluded.capability > public.user_grants.capability
              then excluded.detail
              else public.user_grants.detail
            end;
    end if;
  end loop;

  -- Clean up old account: delete grants, tokens, sessions.
  -- delete from public.user_grants where user_id = old_user_id;
  delete from public.refresh_tokens where user_id = old_user_id;
  delete from auth.sessions where user_id = old_user_id;

  -- Ban old account in GoTrue so they can't sign in via social auth.
  update auth.users
    set banned_until = '2999-01-01'::timestamptz
    where id = old_user_id;

  return null;
end;
$$;

drop trigger if exists on_sso_identity_insert on auth.identities;

create trigger on_sso_identity_insert
  after insert on auth.identities
  for each row
  when (NEW.provider like 'sso:%')
  execute function internal.on_sso_identity_insert();

commit;
