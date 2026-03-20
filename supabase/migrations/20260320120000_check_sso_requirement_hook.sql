-- Access token hook that blocks social login for users whose email domain
-- matches an SSO-enforcing tenant's configured domain.
--
-- When enforce_sso is true and the user's email domain appears in
-- auth.sso_domains for that tenant's provider, the hook refuses to mint
-- a token — returning an error that tells the frontend to redirect to SSO.
--
-- Users whose email domain does NOT match (e.g. contractors, partners)
-- are not blocked here; their grants wil be handled separately
-- (manually removed at first, perhaps automatically filtered later).

begin;

-- Add enforce_sso column to tenants. When true, SSO is required for
-- users whose email domain matches the tenant's SSO provider domains.
alter table public.tenants
  add column if not exists enforce_sso boolean not null default false;

comment on column public.tenants.enforce_sso is
  'When true, users whose email domain matches this tenant''s SSO provider '
  'domains must authenticate via SSO. Social login is blocked for these users.';

create or replace function public.check_sso_requirement(event jsonb)
returns jsonb
language plpgsql
stable
security definer
set search_path to ''
as $$
declare
  target_user_id uuid;
  user_email     text;
  user_domain    text;
  user_is_sso    boolean;
begin
  target_user_id = (event->>'user_id')::uuid;

  select u.email, u.is_sso_user
    into user_email, user_is_sso
    from auth.users u
    where u.id = target_user_id;

  -- SSO users pass through unconditionally.
  if user_is_sso then
    return event;
  end if;

  user_domain = split_part(user_email, '@', 2);

  -- Check whether this user's email domain matches an SSO-enforcing tenant.
  -- If so, block token issuance — the user should be logging in via SSO.
  if exists (
    select 1
      from auth.sso_domains sd
      join public.tenants t on t.sso_provider_id = sd.sso_provider_id
      where t.enforce_sso = true
        and sd.domain = user_domain
  ) then
    return jsonb_build_object(
      'error', jsonb_build_object(
        'http_code', 403,
        'message', 'sso_required:' || user_domain
      )
    );
  end if;

  return event;
end;
$$;

-- The hook is invoked by GoTrue as the supabase_auth_admin role.
grant usage on schema public to supabase_auth_admin;
grant execute on function public.check_sso_requirement(jsonb) to supabase_auth_admin;

-- The function reads from these tables.
grant select on public.tenants to supabase_auth_admin;
grant select on auth.users to supabase_auth_admin;
grant select on auth.sso_domains to supabase_auth_admin;

-- Anon and authenticated roles have execute privileges by default - revoke them.
-- check_sso_requirement is exclusively for supabase_auth_admin.
revoke execute on function public.check_sso_requirement(jsonb) from authenticated, anon;

commit;
