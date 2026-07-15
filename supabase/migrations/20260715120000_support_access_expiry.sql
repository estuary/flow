begin;

-- Declarative expiration timestamps on grants, enforced by a sweeper, and
-- guarded temporary support access built on them.
--
-- Grants in role_grants and user_grants gain a nullable expires_at. NULL means
-- permanent (every pre-existing row). A grant whose expires_at has passed is
-- deleted by internal.expire_support_access() on a schedule, so expiry
-- propagates to every consumer of these tables (RLS, the authorization
-- snapshot, publications) with no changes to any of them. Because the guard is
-- structural -- only rows with a non-NULL expires_at are ever deleted -- the
-- sweeper and revoke can never touch a permanent grant.
--
-- Restricted tenants are deliberately not attached to the estuary_support/
-- role, so support cannot reach them by default. The functions below let an
-- operator grant time-boxed support access to a single tenant WITHOUT direct
-- write access to role_grants (the system-wide authorization table): operators
-- receive EXECUTE on the functions (granted out of band), and the functions
-- perform the single sanctioned role_grants write as their owner. A caller
-- therefore cannot write arbitrary grants and cannot escalate their own
-- access. See ADR estuary/security#746.

alter table public.role_grants add column expires_at timestamptz;
alter table public.user_grants add column expires_at timestamptz;

comment on column public.role_grants.expires_at is
  'Time at which this grant lapses and is removed by the expiry sweep. NULL means the grant is permanent.';
comment on column public.user_grants.expires_at is
  'Time at which this grant lapses and is removed by the expiry sweep. NULL means the grant is permanent.';

-- These tables use column-scoped grants (see 20260511120000_orthogonal_capabilities.sql),
-- and PostgREST `return=representation` (`RETURNING *`) requires read access to
-- every column of rows the dashboard mutates. SELECT only: PostgREST-facing
-- roles may read expiry but must not set it -- expires_at is written solely by
-- SECURITY DEFINER functions.
grant select (expires_at) on public.role_grants to authenticated, marketplace_integration, reporting_user;
grant select (expires_at) on public.user_grants to authenticated, reporting_user;

-- Append-only audit log and access-review evidence for temporary support
-- grants: who granted or revoked what, when, and why. The grant rows
-- themselves (via expires_at) drive enforcement.
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
  'Append-only audit log for temporary estuary_support/ grants created via '
  'internal.grant_support_access(). Doubles as access-review evidence. '
  'revoked_at/revoked_by strictly record explicit revocation; a row with '
  'revoked_at NULL and a past expires_at lapsed on schedule.';

-- Attach support to a tenant for a bounded window, and log it. session_user
-- (the caller''s own login role, preserved through SECURITY DEFINER) records
-- who did it.
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
  v_expires timestamptz;
  v_row     internal.support_access;
begin
  if p_reason is null or btrim(p_reason) = '' then
    raise exception 'a reason is required for temporary support access';
  end if;

  -- A NULL duration would compute a NULL expires_at: a permanent-looking grant.
  -- (The audit insert's NOT NULL would abort it anyway, but fail clearly here.)
  if p_duration is null or p_duration <= interval '0' then
    raise exception 'support access duration must be positive';
  end if;

  if not exists (select 1 from public.tenants t where t.tenant = p_tenant) then
    raise exception 'unknown tenant: %', p_tenant;
  end if;

  -- The only role_grants write this function can make: attach support to the
  -- tenant with an expiry. An expires_at of NULL marks the permanent grant an
  -- unrestricted tenant carries, and the conflict guard refuses to touch it --
  -- without the guard, greatest(NULL, x) = x would silently convert a
  -- permanent grant into a temporary one. A prior temporary grant (live or
  -- lapsed-but-unswept) is extended instead: the later expiry wins.
  insert into public.role_grants (subject_role, object_role, capability, detail, expires_at)
  values ('estuary_support/', p_tenant, 'admin',
          format('temporary support access by %s: %s', session_user, p_reason),
          now() + p_duration)
  on conflict (subject_role, object_role) do update
    set expires_at = greatest(public.role_grants.expires_at, excluded.expires_at),
        detail = excluded.detail,
        updated_at = now()
    where public.role_grants.expires_at is not null
  returning expires_at into v_expires;

  if not found then
    raise exception 'tenant % already has permanent support access', p_tenant;
  end if;

  -- The audit row records the effective expiry (which may exceed the request,
  -- when a longer prior window is still open).
  insert into internal.support_access (id, object_role, granted_by, reason, expires_at)
  values (internal.id_generator(), p_tenant, session_user, p_reason, v_expires)
  returning * into v_row;

  raise log 'support_access granted: tenant=% by=% reason=% expires=%',
    p_tenant, session_user, p_reason, v_expires;
  return v_row;
end;
$$;

comment on function internal.grant_support_access(public.catalog_prefix, text, interval) is
  'Attach estuary_support/ admin to a tenant for a bounded window and log it. '
  'Sanctioned alternative to direct role_grants writes; EXECUTE granted out of band.';

-- Detach support from a tenant early and close out its audit rows. Deletes
-- immediately: with no read-side expiry filters, a lapsed-but-unswept row is
-- still live access, so revocation cannot wait for the sweep.
create function internal.revoke_support_access(
  p_tenant public.catalog_prefix
)
returns void
language plpgsql
security definer
set search_path to ''
as $$
begin
  -- expires_at IS NOT NULL is the structural guard: a permanent grant can
  -- never match, so this function cannot detach an unrestricted tenant.
  delete from public.role_grants
  where subject_role = 'estuary_support/' and object_role = p_tenant
    and expires_at is not null;

  if not found then
    raise exception 'tenant % has no temporary support access to revoke', p_tenant;
  end if;

  update internal.support_access
  set revoked_at = now(), revoked_by = session_user
  where object_role = p_tenant and revoked_at is null;

  raise log 'support_access revoked: tenant=% by=%', p_tenant, session_user;
end;
$$;

comment on function internal.revoke_support_access(public.catalog_prefix) is
  'Detach estuary_support/ from a tenant and mark its support_access rows revoked.';

-- Sweep lapsed grants from both tables. The structural guard (expires_at must
-- be non-NULL and past) makes it impossible to remove a permanent grant, and
-- overlapping support windows share one row whose expires_at is always the
-- latest window's, so nothing is removed while any window remains open.
-- Intended to run on a schedule; see the pg_cron note at the end of this file.
create function internal.expire_support_access()
returns integer
language plpgsql
security definer
set search_path to ''
as $$
declare
  v_count integer;
begin
  with role_swept as (
    delete from public.role_grants
    where expires_at is not null and expires_at <= now()
    returning 1
  ),
  user_swept as (
    delete from public.user_grants
    where expires_at is not null and expires_at <= now()
    returning 1
  )
  select (select count(*) from role_swept) + (select count(*) from user_swept)
  into v_count;

  -- The sweep does not touch internal.support_access: revoked_at/revoked_by
  -- strictly record explicit revocation by a named person. A lapsed window is
  -- already self-describing (revoked_at IS NULL and expires_at <= now()), and
  -- audit rows carry per-request expiries that an extension deliberately does
  -- not move, so stamping them here would mislabel subsumed windows as revoked.

  return v_count;
end;
$$;

comment on function internal.expire_support_access() is
  'Removes grants whose expires_at has passed. Run on a schedule.';

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
