-- Tests for temporary support access (20260630120000_support_access_functions.sql).
-- Covers: grant attaches estuary_support/ and logs a tracking row; input validation;
-- grant and revoke refuse unrestricted tenants (whose permanent estuary_support/
-- grant must never be deleted); the sweeper removes only expired, tracked grants
-- and leaves overlapping still-open windows attached; and early revoke.
--
-- A "restricted" tenant is simulated by inserting a tenant and then deleting the
-- estuary_support/ grant the tenant-insert trigger auto-creates, so the baseline
-- is "support not attached".

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
         and capability = 'admin'),
    1, 'grant attaches estuary_support/ admin to the tenant');

  return query select ok(
    exists(select 1 from internal.support_access
             where object_role = 'supportGrant/' and revoked_at is null
               and reason = 'ticket SUP-1' and expires_at > now()),
    'grant logs a tracking row with reason and a future expiry');
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
end;
$$ language plpgsql;


-- An unrestricted tenant already holds the permanent estuary_support/ grant.
-- Granting must refuse (or expiry would later delete the permanent grant), and
-- revoking must refuse (there is no temporary grant to detach).
create function tests.test_support_access_refuses_unrestricted_tenant()
returns setof text as $$
begin
  set role postgres;
  insert into tenants (tenant) values ('supportUnrestricted/') on conflict (tenant) do nothing;

  return query select throws_ok(
    $q$ select internal.grant_support_access('supportUnrestricted/', 'ticket') $q$,
    null, 'tenant supportUnrestricted/ already has standing support access',
    'granting over a permanent support grant is rejected');

  return query select throws_ok(
    $q$ select internal.revoke_support_access('supportUnrestricted/') $q$,
    null, 'tenant supportUnrestricted/ has no temporary support access to revoke',
    'revoking a tenant without a tracking row is rejected');

  return query select is(
    (select count(*)::int from role_grants
       where subject_role = 'estuary_support/' and object_role = 'supportUnrestricted/'),
    1, 'the permanent support grant is untouched');
end;
$$ language plpgsql;


create function tests.test_expire_support_access_sweeps_only_expired_tracked()
returns setof text as $$
declare
  v_count int;
begin
  set role postgres;

  -- Restricted tenant with an already-expired temporary grant.
  insert into tenants (tenant) values ('supportExpired/') on conflict (tenant) do nothing;
  delete from role_grants where subject_role = 'estuary_support/' and object_role = 'supportExpired/';
  perform internal.grant_support_access('supportExpired/', 'expired', interval '-1 hours');

  -- Restricted tenant with a still-valid temporary grant.
  insert into tenants (tenant) values ('supportValidWindow/') on conflict (tenant) do nothing;
  delete from role_grants where subject_role = 'estuary_support/' and object_role = 'supportValidWindow/';
  perform internal.grant_support_access('supportValidWindow/', 'valid', interval '2 hours');

  -- Normal tenant: permanent auto-granted support access, and NO tracking row.
  insert into tenants (tenant) values ('supportPermanent/') on conflict (tenant) do nothing;

  select internal.expire_support_access() into v_count;

  return query select is(v_count, 1, 'expire sweeps exactly the one expired grant');

  return query select is(
    (select count(*)::int from role_grants
       where subject_role = 'estuary_support/' and object_role = 'supportExpired/'),
    0, 'the expired temporary grant is detached');

  return query select ok(
    exists(select 1 from internal.support_access
             where object_role = 'supportExpired/' and revoked_at is not null
               and revoked_by = 'internal.expire_support_access'),
    'the expired tracking row is marked revoked by the sweeper');

  return query select is(
    (select count(*)::int from role_grants
       where subject_role = 'estuary_support/' and object_role = 'supportValidWindow/'),
    1, 'a still-valid temporary grant is left attached');

  return query select is(
    (select count(*)::int from role_grants
       where subject_role = 'estuary_support/' and object_role = 'supportPermanent/'),
    1, 'a permanent support grant (no tracking row) is never swept');
end;
$$ language plpgsql;


-- Overlapping windows: a grant extended before it lapsed shares one role_grants
-- row. Expiring the earlier window must not detach the still-open one.
create function tests.test_expire_support_access_keeps_overlapping_window()
returns setof text as $$
declare
  v_count int;
begin
  set role postgres;
  insert into tenants (tenant) values ('supportOverlap/') on conflict (tenant) do nothing;
  delete from role_grants where subject_role = 'estuary_support/' and object_role = 'supportOverlap/';

  perform internal.grant_support_access('supportOverlap/', 'first window', interval '-1 hours');
  perform internal.grant_support_access('supportOverlap/', 'extension', interval '2 hours');

  select internal.expire_support_access() into v_count;

  return query select is(v_count, 0, 'no grant is detached while a window remains open');

  return query select is(
    (select count(*)::int from role_grants
       where subject_role = 'estuary_support/' and object_role = 'supportOverlap/'),
    1, 'support stays attached until the last window closes');

  return query select ok(
    exists(select 1 from internal.support_access
             where object_role = 'supportOverlap/' and reason = 'first window'
               and revoked_at is not null),
    'the lapsed window is still marked revoked');

  return query select ok(
    exists(select 1 from internal.support_access
             where object_role = 'supportOverlap/' and reason = 'extension'
               and revoked_at is null),
    'the open window remains active');
end;
$$ language plpgsql;


create function tests.test_revoke_support_access_detaches_and_marks()
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
    0, 'revoke detaches support from the tenant');

  return query select ok(
    exists(select 1 from internal.support_access
             where object_role = 'supportRevoke/' and revoked_at is not null),
    'revoke marks the tracking row revoked');
end;
$$ language plpgsql;
