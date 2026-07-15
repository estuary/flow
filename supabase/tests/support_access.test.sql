-- Tests for temporary support access (20260715120000_support_access_expiry.sql).
-- Covers: grant attaches a windowed estuary_support/ grant and logs an audit row;
-- input validation; refusal to touch a permanent (NULL expires_at) grant;
-- extension semantics (later expiry wins, one row per tenant); re-grant after a
-- lapsed-but-unswept window; early revoke deletes immediately; and the sweeper
-- removes lapsed grants from both tables while never touching permanent ones.
--
-- A "restricted" tenant is simulated by inserting a tenant and then deleting the
-- estuary_support/ grant the tenant-insert trigger auto-creates, so the baseline
-- is "support not attached". Expiry is always driven by direct UPDATE of
-- expires_at, never by sleeping.

create function tests.test_grant_support_access_attaches_and_logs()
returns setof text as $$
begin
  set role postgres;

  insert into tenants (tenant) values ('supportGrant/') on conflict (tenant) do nothing;
  delete from role_grants where subject_role = 'estuary_support/' and object_role = 'supportGrant/';

  return query select is(
    (select count(*)::int from role_grants
       where subject_role = 'estuary_support/' and object_role = 'supportGrant/'),
    0, 'baseline: support not attached to the restricted tenant');

  perform internal.grant_support_access('supportGrant/', 'ticket SUP-1', interval '2 hours');

  return query select is(
    (select count(*)::int from role_grants
       where subject_role = 'estuary_support/' and object_role = 'supportGrant/'
         and capability = 'admin' and expires_at > now()),
    1, 'grant attaches estuary_support/ admin with a future expiry');

  return query select ok(
    exists(select 1 from internal.support_access
             where object_role = 'supportGrant/' and revoked_at is null
               and reason = 'ticket SUP-1' and expires_at > now()),
    'grant logs an audit row with reason and a future expiry');
end;
$$ language plpgsql;


create function tests.test_grant_support_access_validates_input()
returns setof text as $$
begin
  set role postgres;
  insert into tenants (tenant) values ('supportValid/') on conflict (tenant) do nothing;

  return query select throws_ok(
    $q$ select internal.grant_support_access('supportValid/', '   ') $q$,
    null, 'a reason is required for temporary support access',
    'a blank reason is rejected');

  return query select throws_ok(
    $q$ select internal.grant_support_access('no-such-tenant/', 'valid reason') $q$,
    null, 'unknown tenant: no-such-tenant/',
    'an unknown tenant is rejected');

  return query select throws_ok(
    $q$ select internal.grant_support_access('supportValid/', 'valid reason', interval '-1 hours') $q$,
    null, 'support access duration must be positive',
    'a negative duration is rejected');

  return query select throws_ok(
    $q$ select internal.grant_support_access('supportValid/', 'valid reason', null) $q$,
    null, 'support access duration must be positive',
    'a null duration is rejected rather than creating a permanent-looking grant');
end;
$$ language plpgsql;


-- An unrestricted tenant carries the permanent (NULL expires_at) support grant.
-- Granting must refuse rather than convert it to a temporary window, and
-- revoking must refuse rather than delete it.
create function tests.test_support_access_refuses_permanent_grant()
returns setof text as $$
begin
  set role postgres;
  insert into tenants (tenant) values ('supportPermanent/') on conflict (tenant) do nothing;

  return query select throws_ok(
    $q$ select internal.grant_support_access('supportPermanent/', 'ticket') $q$,
    null, 'tenant supportPermanent/ already has permanent support access',
    'granting over a permanent grant is rejected');

  return query select throws_ok(
    $q$ select internal.revoke_support_access('supportPermanent/') $q$,
    null, 'tenant supportPermanent/ has no temporary support access to revoke',
    'revoking a tenant with only a permanent grant is rejected');

  return query select is(
    (select count(*)::int from role_grants
       where subject_role = 'estuary_support/' and object_role = 'supportPermanent/'
         and expires_at is null),
    1, 'the permanent grant is untouched and still permanent');
end;
$$ language plpgsql;


-- Overlapping windows share the single role_grants row; the later expiry wins,
-- and a shorter "extension" never shortens an open window. The audit rows
-- record the effective expiry of each request.
create function tests.test_grant_support_access_extends()
returns setof text as $$
declare
  v_first  internal.support_access;
  v_second internal.support_access;
  v_third  internal.support_access;
begin
  set role postgres;
  insert into tenants (tenant) values ('supportExtend/') on conflict (tenant) do nothing;
  delete from role_grants where subject_role = 'estuary_support/' and object_role = 'supportExtend/';

  v_first  := internal.grant_support_access('supportExtend/', 'first', interval '1 hour');
  v_second := internal.grant_support_access('supportExtend/', 'longer', interval '24 hours');
  v_third  := internal.grant_support_access('supportExtend/', 'shorter', interval '1 minute');

  return query select is(
    (select count(*)::int from role_grants
       where subject_role = 'estuary_support/' and object_role = 'supportExtend/'),
    1, 'overlapping windows share a single role_grants row');

  return query select is(
    (select expires_at from role_grants
       where subject_role = 'estuary_support/' and object_role = 'supportExtend/'),
    v_second.expires_at, 'the latest expiry wins; a shorter extension never shortens it');

  return query select ok(
    v_third.expires_at = v_second.expires_at and v_second.expires_at > v_first.expires_at,
    'audit rows record the effective expiry of each request');

  return query select is(
    (select count(*)::int from internal.support_access where object_role = 'supportExtend/'),
    3, 'every request is logged, including no-op extensions');
end;
$$ language plpgsql;


-- A lapsed-but-unswept window is still the tenant's one role_grants row;
-- re-granting extends it forward rather than colliding with the unique
-- constraint or waiting for the sweep.
create function tests.test_grant_support_access_after_lapse()
returns setof text as $$
begin
  set role postgres;
  insert into tenants (tenant) values ('supportLapsed/') on conflict (tenant) do nothing;
  delete from role_grants where subject_role = 'estuary_support/' and object_role = 'supportLapsed/';

  perform internal.grant_support_access('supportLapsed/', 'original window', interval '2 hours');
  update role_grants set expires_at = now() - interval '1 hour'
    where subject_role = 'estuary_support/' and object_role = 'supportLapsed/';

  perform internal.grant_support_access('supportLapsed/', 'fresh window', interval '2 hours');

  return query select is(
    (select count(*)::int from role_grants
       where subject_role = 'estuary_support/' and object_role = 'supportLapsed/'
         and expires_at > now()),
    1, 're-grant over a lapsed-but-unswept row moves its expiry forward');
end;
$$ language plpgsql;


-- Revoke deletes immediately: with no read-side expiry filters, waiting for
-- the sweep would leave access live.
create function tests.test_revoke_support_access_deletes_immediately()
returns setof text as $$
begin
  set role postgres;
  insert into tenants (tenant) values ('supportRevoke/') on conflict (tenant) do nothing;
  delete from role_grants where subject_role = 'estuary_support/' and object_role = 'supportRevoke/';
  perform internal.grant_support_access('supportRevoke/', 'ticket', interval '2 hours');

  perform internal.revoke_support_access('supportRevoke/');

  return query select is(
    (select count(*)::int from role_grants
       where subject_role = 'estuary_support/' and object_role = 'supportRevoke/'),
    0, 'revoke detaches support immediately');

  return query select ok(
    exists(select 1 from internal.support_access
             where object_role = 'supportRevoke/' and revoked_at is not null
               and revoked_by is not null),
    'revoke marks the audit row revoked');

  return query select throws_ok(
    $q$ select internal.revoke_support_access('supportRevoke/') $q$,
    null, 'tenant supportRevoke/ has no temporary support access to revoke',
    'a second revoke refuses');
end;
$$ language plpgsql;


-- The sweeper removes lapsed grants from BOTH tables, leaves still-open
-- windows attached, and can never touch permanent (NULL expires_at) grants.
create function tests.test_expire_support_access_sweeps_lapsed_grants()
returns setof text as $$
declare
  v_count int;
begin
  set role postgres;

  -- Restricted tenant with a lapsed temporary grant.
  insert into tenants (tenant) values ('supportExpired/') on conflict (tenant) do nothing;
  delete from role_grants where subject_role = 'estuary_support/' and object_role = 'supportExpired/';
  perform internal.grant_support_access('supportExpired/', 'expired', interval '2 hours');
  -- Simulate the window passing: both the grant row and its audit row carry
  -- the same expiry in production, so lapse both.
  update role_grants set expires_at = now() - interval '1 hour'
    where subject_role = 'estuary_support/' and object_role = 'supportExpired/';
  update internal.support_access set expires_at = now() - interval '1 hour'
    where object_role = 'supportExpired/';

  -- Restricted tenant with a still-open window.
  insert into tenants (tenant) values ('supportValidWindow/') on conflict (tenant) do nothing;
  delete from role_grants where subject_role = 'estuary_support/' and object_role = 'supportValidWindow/';
  perform internal.grant_support_access('supportValidWindow/', 'valid', interval '2 hours');

  -- Normal tenant: permanent auto-granted support access.
  insert into tenants (tenant) values ('supportPermSweep/') on conflict (tenant) do nothing;

  -- A lapsed user_grants row: the sweep covers both tables.
  delete from user_grants where user_id = '44444444-4444-4444-4444-444444444444';
  insert into user_grants (user_id, object_role, capability, expires_at) values
    ('44444444-4444-4444-4444-444444444444', 'daveCo/', 'admin', now() - interval '1 hour'),
    ('44444444-4444-4444-4444-444444444444', 'daveCo/windowed/', 'read', now() + interval '1 hour');

  select internal.expire_support_access() into v_count;

  return query select is(v_count, 2, 'the sweep counts one lapsed grant from each table');

  return query select is(
    (select count(*)::int from role_grants
       where subject_role = 'estuary_support/' and object_role = 'supportExpired/'),
    0, 'the lapsed temporary grant is detached');

  return query select ok(
    exists(select 1 from internal.support_access
             where object_role = 'supportExpired/' and revoked_at is null
               and expires_at <= now()),
    'the lapsed audit row stays unstamped: revoked_at means explicit revocation only');

  return query select is(
    (select count(*)::int from role_grants
       where subject_role = 'estuary_support/' and object_role = 'supportValidWindow/'),
    1, 'a still-open window is left attached');

  return query select is(
    (select count(*)::int from role_grants
       where subject_role = 'estuary_support/' and object_role = 'supportPermSweep/'),
    1, 'a permanent support grant is never swept');

  return query select results_eq(
    $q$ select object_role::text from user_grants
        where user_id = '44444444-4444-4444-4444-444444444444' $q$,
    $q$ values ('daveCo/windowed/') $q$,
    'the lapsed user_grants row is swept; the open window survives');
end;
$$ language plpgsql;
