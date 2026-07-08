begin;
-- Automatically migrate grants and refresh tokens when a social-auth user logs in via SSO for the first time.
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

  -- Transfer estuary refresh tokens to new account
  update public.refresh_tokens set user_id = NEW.user_id where user_id = old_user_id;

  return null;
end;
$$;

-- Ensure each SSO provider is linked to at most one tenant.
alter table public.tenants
  add constraint tenants_sso_provider_id_unique unique (sso_provider_id);

commit;
