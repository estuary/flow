-- Add a customize_access_token hook that embeds SSO provider IDs into the JWT
-- when a non-SSO user has grants being filtered by SSO enforcement.
--
-- The dashboard reads the `sso_required` claim on login/refresh and can show
-- an interstitial prompting the user to re-authenticate via SSO.
-- Only provider UUIDs are included — no tenant names — to avoid leaking
-- which tenants the user has grants on.

begin;

create or replace function public.custom_access_token_hook(event jsonb)
returns jsonb
language plpgsql
stable
security definer
set search_path to ''
as $$
declare
  claims         jsonb;
  target_user_id uuid;
  provider_ids   jsonb;
begin
  target_user_id = (event->>'user_id')::uuid;
  claims = event->'claims';

  -- Find distinct SSO provider IDs for tenants that enforce SSO and where
  -- this user has grants but lacks the matching SSO identity.
  select jsonb_agg(distinct t.sso_provider_id)
    into provider_ids
    from public.user_grants ug
    join public.tenants t on t.tenant ^@ ug.object_role
    where ug.user_id = target_user_id
      and t.enforce_sso
      and not exists (
        select 1 from auth.identities ai
        where ai.user_id = target_user_id
          and ai.provider = 'sso'
          and ai.provider_id = t.sso_provider_id::text
      );

  if provider_ids is not null then
    claims = jsonb_set(claims, '{sso_required}', provider_ids);
  else
    claims = claims - 'sso_required';
  end if;

  event = jsonb_set(event, '{claims}', claims);
  return event;
end;
$$;

-- The hook is invoked by GoTrue as the supabase_auth_admin role.
grant usage on schema public to supabase_auth_admin;
grant execute on function public.custom_access_token_hook(jsonb) to supabase_auth_admin;

-- The function reads from these tables.
grant select on public.user_grants to supabase_auth_admin;
grant select on public.tenants to supabase_auth_admin;
grant select on auth.identities to supabase_auth_admin;

-- Revoke from anon/authenticated — this is not a user-callable function.
revoke execute on function public.custom_access_token_hook(jsonb) from authenticated, anon;

commit;
