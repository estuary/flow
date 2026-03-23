-- Automatically migrate grants when a social-auth user re-authenticates via SSO.
--
-- GoTrue creates a new auth.users record for SAML SSO logins (no auto-linking).
-- This trigger detects the new SSO identity, finds the prior social account by
-- email, transfers eligible grants, and suspends the old account.

begin;

-- Audit log for grant migrations.
create table internal.sso_grant_migrations (
    id                  bigint generated always as identity primary key,
    old_user_id         uuid not null,
    new_user_id         uuid not null,
    email               text not null,
    sso_provider_id     uuid not null,
    transferred_grants  jsonb not null default '[]',
    skipped_grants      jsonb not null default '[]',
    created_at          timestamptz not null default now()
);

comment on table internal.sso_grant_migrations is
  'Audit log of grant migrations when social-auth users re-authenticate via SSO';

-- Trigger function: migrate grants from old social user to new SSO user.
create or replace function internal.on_sso_identity_insert()
returns trigger
language plpgsql
security definer
set search_path to ''
as $$
declare
  new_user_email  text;
  old_user_id     uuid;
  transferred     jsonb = '[]'::jsonb;
  skipped         jsonb = '[]'::jsonb;
  grant_row       record;
begin
  -- Look up the new SSO user's email.
  select email into new_user_email
    from auth.users
    where id = NEW.user_id;

  if new_user_email is null then
    return NEW;
  end if;

  -- Find the most recent social-auth user with a matching email.
  select id into old_user_id
    from auth.users
    where email = new_user_email
      and id <> NEW.user_id
      and (is_sso_user = false or is_sso_user is null)
    order by created_at desc
    limit 1;

  if old_user_id is null then
    return NEW;
  end if;

  -- Process each grant on the old user. Use a lateral join to find
  -- the most specific tenant governing each grant's prefix.
  for grant_row in
    select ug.object_role, ug.capability, ug.detail,
           t.enforce_sso,
           t.sso_provider_id as tenant_sso_provider_id
    from public.user_grants ug
    left join lateral (
      select t2.enforce_sso, t2.sso_provider_id
      from public.tenants t2
      where t2.tenant ^@ ug.object_role
      order by length(t2.tenant::text) desc
      limit 1
    ) t on true
    where ug.user_id = old_user_id
  loop
    if grant_row.enforce_sso
       and grant_row.tenant_sso_provider_id is distinct from NEW.provider_id::uuid
    then
      -- SSO-enforced tenant with a different provider: skip.
      skipped = skipped || jsonb_build_array(jsonb_build_object(
        'object_role', grant_row.object_role,
        'capability', grant_row.capability,
        'reason', 'sso_provider_mismatch'
      ));
    else
      -- Transfer: upsert into new user's grants (capability only upgrades).
      insert into public.user_grants (user_id, object_role, capability, detail)
      values (
        NEW.user_id,
        grant_row.object_role,
        grant_row.capability,
        coalesce(grant_row.detail, 'migrated from social auth')
      )
      on conflict (user_id, object_role) do update
        set capability = greatest(
              public.user_grants.capability,
              excluded.capability
            ),
            updated_at = now(),
            detail = excluded.detail;

      transferred = transferred || jsonb_build_array(jsonb_build_object(
        'object_role', grant_row.object_role,
        'capability', grant_row.capability
      ));
    end if;
  end loop;

  -- Clean up old account: delete grants, tokens, sessions.
  delete from public.user_grants where user_id = old_user_id;
  delete from public.refresh_tokens where user_id = old_user_id;
  delete from auth.sessions where user_id = old_user_id;

  -- Suspend old account.
  insert into internal.account_suspensions (user_id, reason)
  values (old_user_id, 'Replaced by SSO account ' || NEW.user_id::text)
  on conflict (user_id) do nothing;

  -- Write audit log.
  insert into internal.sso_grant_migrations
    (old_user_id, new_user_id, email, sso_provider_id, transferred_grants, skipped_grants)
  values
    (old_user_id, NEW.user_id, new_user_email, NEW.provider_id::uuid, transferred, skipped);

  return NEW;
end;
$$;

create trigger on_sso_identity_insert
  after insert on auth.identities
  for each row
  when (NEW.provider = 'sso')
  execute function internal.on_sso_identity_insert();

commit;
