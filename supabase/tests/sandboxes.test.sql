-- Tests for internal.sandboxes (20260908120000_sandboxes.sql).
-- Covers: retired handles remain reserved and catalog names are unique among
-- each user's live sandboxes. The control plane enforces catalog name validation
-- and the per-user live-sandbox limit.

create function tests.test_sandbox_handles_are_never_reused()
returns setof text as $$
declare
  alice uuid := '11111111-1111-1111-1111-111111111111';
  first_id flowid;
begin
  set role postgres;

  insert into internal.sandboxes (user_id, handle, catalog_name) values (alice, 'sbx-first', 'dev')
    returning id into first_id;
  update internal.sandboxes set deleted_at = now() where id = first_id;

  return query select lives_ok(
    $q$ insert into internal.sandboxes (user_id, handle, catalog_name)
        values ('11111111-1111-1111-1111-111111111111', 'sbx-second', 'dev') $q$,
    'a retired sandbox leaves room for a fresh one under a new handle');

  return query select throws_ok(
    $q$ insert into internal.sandboxes (user_id, handle, catalog_name)
        values ('22222222-2222-2222-2222-222222222222', 'sbx-first', 'dev') $q$,
    '23505', null, 'a retired handle is never reused');
end;
$$ language plpgsql;


create function tests.test_sandbox_names_are_unique_among_a_users_live_sandboxes()
returns setof text as $$
declare
  alice uuid := '11111111-1111-1111-1111-111111111111';
  first_id flowid;
begin
  set role postgres;

  insert into internal.sandboxes (user_id, handle, catalog_name) values (alice, 'sbx-n1', 'staging')
    returning id into first_id;

  return query select throws_ok(
    $q$ insert into internal.sandboxes (user_id, handle, catalog_name)
        values ('11111111-1111-1111-1111-111111111111', 'sbx-n2', 'staging') $q$,
    '23505', null, 'a user cannot have two live sandboxes of one catalog name');

  return query select lives_ok(
    $q$ insert into internal.sandboxes (user_id, handle, catalog_name)
        values ('22222222-2222-2222-2222-222222222222', 'sbx-n3', 'staging') $q$,
    'another user may use the same catalog name');

  update internal.sandboxes set deleted_at = now() where id = first_id;

  return query select lives_ok(
    $q$ insert into internal.sandboxes (user_id, handle, catalog_name)
        values ('11111111-1111-1111-1111-111111111111', 'sbx-n4', 'staging') $q$,
    'a retired sandbox frees its catalog name');
end;
$$ language plpgsql;
