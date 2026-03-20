-- Add a customize_access_token hook that embeds SSO provider IDs into the JWT
-- when a non-SSO user has grants on tenants with SSO configured.
--
-- The dashboard reads the `sso_not_satisfied` claim on login/refresh and can show
-- an interstitial prompting the user to re-authenticate via SSO.
-- Only provider UUIDs are included — no tenant names — to avoid leaking
-- which tenants the user has grants on.
--
-- Keyed on sso_provider_id IS NOT NULL (not enforce_sso) so users get nudged
-- as soon as SSO is configured, giving them runway before hard enforcement.

begin;

create or replace function public.check_sso_requirement(event jsonb)
returns jsonb
language plpgsql
stable
security definer
set search_path to ''
as $$
declare
  claims         jsonb;
  target_user_id uuid;
  provider_id    uuid;
begin
  target_user_id = (event->>'user_id')::uuid;
  claims = event->'claims';

  -- Find the SSO provider for the tenant where this user has grants but
  -- lacks the matching SSO identity. We expect at most one SSO-enabled
  -- tenant per user; LIMIT 1 makes that assumption explicit.
  select t.sso_provider_id
    into provider_id
    from public.user_grants ug
    join public.tenants t on ug.object_role ^@ t.tenant
    where ug.user_id = target_user_id
      and t.sso_provider_id is not null
      and not exists (
        select 1 from auth.identities ai
        where ai.user_id = target_user_id
          and ai.provider = 'sso:' || t.sso_provider_id::text
      )
    limit 1;

  if provider_id is not null then
    claims = jsonb_set(claims, '{sso_not_satisfied}', to_jsonb(provider_id));
  else
    claims = claims - 'sso_not_satisfied';
  end if;

  event = jsonb_set(event, '{claims}', claims);
  return event;

exception when others then
  raise warning 'check_sso_requirement failed for user %: %', target_user_id, SQLERRM;
  return event;
end;
$$;

-- The hook is invoked by GoTrue as the supabase_auth_admin role.
grant usage on schema public to supabase_auth_admin;
grant execute on function public.check_sso_requirement(jsonb) to supabase_auth_admin;

-- The function reads from these tables.
grant select on public.user_grants to supabase_auth_admin;
grant select on public.tenants to supabase_auth_admin;
grant select on auth.identities to supabase_auth_admin;

-- Anon and authenticated roles have execute privileges by default - revoke them.
-- check_sso_requirement is exclusively for supabase_auth_admin.
revoke execute on function public.check_sso_requirement(jsonb) from authenticated, anon;

commit;
