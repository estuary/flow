-- Phase 4d: Hard SSO enforcement at the authorization layer.
--
-- When a tenant has enforce_sso = true, grants on that tenant are excluded
-- unless the user has an SSO identity matching the tenant's configured provider.
-- This is enforced in the base case of internal.user_roles(), so transitive
-- grants through SSO-enforced tenants are also excluded.

begin;

alter table public.tenants
  add column enforce_sso boolean not null default false;

comment on column public.tenants.enforce_sso is
  'When true, only users with an SSO identity matching sso_provider_id may access this tenant''s resources';

-- Replace internal.user_roles to exclude grants on SSO-enforced tenants unless
-- the user authenticated via the tenant's specific SSO provider.
create or replace function internal.user_roles(
  target_user_id uuid,
  min_capability public.grant_capability default 'x_00'::public.grant_capability
)
returns table(role_prefix public.catalog_prefix, capability public.grant_capability)
language sql stable
as $$
  with recursive
  all_roles(role_prefix, capability) as (
      select object_role, capability from user_grants
      where user_id = target_user_id
        and capability >= min_capability
        -- Exclude grants on SSO-enforced tenants unless the user has an
        -- identity linked to that tenant's specific SSO provider.
        and not exists (
          select 1 from tenants t
          where t.tenant ^@ user_grants.object_role
            and t.enforce_sso
            and not exists (
              select 1 from auth.identities ai
              where ai.user_id = target_user_id
                and ai.provider = 'sso:' || t.sso_provider_id::text
            )
        )
    union
      -- Recursive case: for each object_role granted as 'admin',
      -- project through grants where object_role acts as the subject_role.
      select role_grants.object_role, role_grants.capability
      from role_grants, all_roles
      where role_grants.subject_role ^@ all_roles.role_prefix
        and role_grants.capability >= min_capability
        and all_roles.capability = 'admin'
  )
  select role_prefix, max(capability) from all_roles
  group by role_prefix
  order by role_prefix;
$$;

commit;
