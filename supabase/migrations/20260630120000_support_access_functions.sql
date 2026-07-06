begin;

-- Guarded, auto-expiring temporary support access to restricted tenants.
--
-- Restricted tenants are deliberately not attached to the estuary_support/ role,
-- so support cannot reach them by default. These objects
-- let an operator grant time-boxed support access to a single tenant WITHOUT
-- direct write access to public.role_grants (the system-wide authorization table):
-- operators receive EXECUTE on the functions below (granted out of band), and the
-- functions perform the single sanctioned role_grants write as their owner. A
-- caller therefore cannot write arbitrary grants and cannot escalate their own
-- access. See ADR estuary/security#746.
--
-- Every unrestricted tenant carries a permanent estuary_support/ grant (created
-- by the tenant-insert trigger). These functions must never delete such a grant,
-- so a role_grants row is treated as temporary -- and eligible for deletion --
-- only while an unrevoked internal.support_access row records ownership of it.

-- Source of truth for temporary grants, separate from role_grants so expiry can
-- never touch the permanent estuary_support/ grants every tenant receives.
create table internal.support_access (
  id          public.flowid primary key,        -- defaults via the flowid domain
  object_role public.catalog_prefix not null,    -- the tenant, e.g. 'acmeCo/'
  granted_by  text not null,                      -- the operator's own login role
  reason      text not null,
  granted_at  timestamptz not null default now(),
  expires_at  timestamptz not null,
  revoked_at  timestamptz,
  revoked_by  text
);

comment on table internal.support_access is
  'Audit log and lifecycle record for temporary estuary_support/ grants created via '
  'internal.grant_support_access(). Doubles as access-review evidence.';

-- Attach support to a tenant for a bounded window, and log it. session_user (the
-- caller''s own login role, preserved through SECURITY DEFINER) records who did it.
create function internal.grant_support_access(
  p_tenant   public.catalog_prefix,
  p_reason   text,
  p_duration interval default interval '24 hours'
)
returns internal.support_access
language plpgsql
security definer
-- https://www.postgresql.org/docs/current/sql-createfunction.html#SQL-CREATEFUNCTION-SECURITY
set search_path to ''
as $$
declare
  v_row internal.support_access;
begin
  if p_reason is null or btrim(p_reason) = '' then
    raise exception 'a reason is required for temporary support access';
  end if;

  if not exists (select 1 from public.tenants t where t.tenant = p_tenant) then
    raise exception 'unknown tenant: %', p_tenant;
  end if;

  -- The only role_grants write this function can make: attach support to the tenant.
  insert into public.role_grants (subject_role, object_role, capability, detail)
  values ('estuary_support/', p_tenant, 'admin',
          format('temporary support access by %s: %s', session_user, p_reason))
  on conflict (subject_role, object_role) do nothing;

  -- If the grant already existed, it is ours only when an unrevoked tracking row
  -- owns it (the caller is extending a temporary window). Otherwise it is the
  -- permanent grant of an unrestricted tenant: refuse, or the tracking row below
  -- would mark that permanent grant for deletion by expire_support_access().
  if not found and not exists (
    select 1 from internal.support_access
    where object_role = p_tenant and revoked_at is null
  ) then
    raise exception 'tenant % already has standing support access', p_tenant;
  end if;

  insert into internal.support_access (id, object_role, granted_by, reason, expires_at)
  values (internal.id_generator(), p_tenant, session_user, p_reason, now() + p_duration)
  returning * into v_row;

  raise log 'support_access granted: tenant=% by=% reason=%',
    p_tenant, session_user, p_reason;
  return v_row;
end;
$$;

comment on function internal.grant_support_access(public.catalog_prefix, text, interval) is
  'Attach estuary_support/ admin to a tenant for a bounded window and log it. '
  'Sanctioned alternative to direct role_grants writes; EXECUTE granted out of band.';

-- Detach support from a tenant early and close out its audit rows.
create function internal.revoke_support_access(
  p_tenant public.catalog_prefix
)
returns void
language plpgsql
security definer
set search_path to ''
as $$
begin
  -- Refuse tenants without an active tracking row: their estuary_support/ grant
  -- (if any) is the permanent one, which this function must never delete.
  if not exists (
    select 1 from internal.support_access
    where object_role = p_tenant and revoked_at is null
  ) then
    raise exception 'tenant % has no temporary support access to revoke', p_tenant;
  end if;

  delete from public.role_grants
  where subject_role = 'estuary_support/' and object_role = p_tenant;

  update internal.support_access
  set revoked_at = now(), revoked_by = session_user
  where object_role = p_tenant and revoked_at is null;

  raise log 'support_access revoked: tenant=% by=%', p_tenant, session_user;
end;
$$;

comment on function internal.revoke_support_access(public.catalog_prefix) is
  'Detach estuary_support/ from a tenant and mark its support_access rows revoked.';

-- Sweep expired grants. Only deletes role_grants rows owned by a support_access
-- record whose every window has closed, so it can never revoke the permanent
-- estuary_support/ grants. Intended to be run on a schedule; see the pg_cron
-- note at the end of this file.
create function internal.expire_support_access()
returns integer
language plpgsql
security definer
set search_path to ''
as $$
declare
  v_tenant public.catalog_prefix;
  v_count  integer := 0;
begin
  for v_tenant in
    select distinct object_role from internal.support_access
    where revoked_at is null and expires_at <= now()
  loop
    -- Overlapping windows (a grant extended before it lapsed) share one
    -- role_grants row, which must survive until the last window closes.
    continue when exists (
      select 1 from internal.support_access
      where object_role = v_tenant and revoked_at is null and expires_at > now()
    );

    delete from public.role_grants
    where subject_role = 'estuary_support/' and object_role = v_tenant;
    if found then
      v_count := v_count + 1;
    end if;
  end loop;

  update internal.support_access
  set revoked_at = now(), revoked_by = 'internal.expire_support_access'
  where revoked_at is null and expires_at <= now();

  return v_count;
end;
$$;

comment on function internal.expire_support_access() is
  'Removes temporary support grants whose expires_at has passed. Run on a schedule.';

-- Keep these off PUBLIC. EXECUTE is granted to specific operator roles out of band
-- (those roles are managed in a separate private repo, not by flow migrations).
revoke all on function internal.grant_support_access(public.catalog_prefix, text, interval) from public;
revoke all on function internal.revoke_support_access(public.catalog_prefix) from public;
revoke all on function internal.expire_support_access() from public;

-- Expiry enforcement runs via pg_cron, which exists only on the Supabase database
-- (not the sqlx test cluster), so it is NOT scheduled here. Register it once,
-- out of band, on the Supabase database:
--
--   select cron.schedule('expire-support-access', '* * * * *',
--                        $$ select internal.expire_support_access() $$);

commit;
