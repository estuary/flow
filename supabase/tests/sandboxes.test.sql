-- Tests for internal.sandboxes (20260908120000_sandboxes.sql).
-- Covers: handles and catalog names are globally unique, and deletion frees names.
-- The control plane enforces catalog name validation.

create function tests.test_sandbox_handles_are_unique()
returns setof text as $$
begin
  set role postgres;
  insert into internal.sandboxes (user_id, handle, catalog_name)
    values ('11111111-1111-1111-1111-111111111111', 'sbx-first', 'dev');

  return query select throws_ok(
    $q$ insert into internal.sandboxes (user_id, handle, catalog_name)
        values ('22222222-2222-2222-2222-222222222222', 'sbx-first', 'other') $q$,
    '23505', null, 'two sandboxes cannot share a provider handle');
end;
$$ language plpgsql;


create function tests.test_sandbox_names_are_globally_unique()
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

  return query select throws_ok(
    $q$ insert into internal.sandboxes (user_id, handle, catalog_name)
        values ('22222222-2222-2222-2222-222222222222', 'sbx-n3', 'staging') $q$,
    '23505', null, 'another user cannot use an existing live catalog name');

  delete from internal.sandboxes where id = first_id;

  return query select is((select count(*) from internal.sandboxes where id = first_id),
    0::bigint, 'deletion removes the sandbox record');

  return query select lives_ok(
    $q$ insert into internal.sandboxes (user_id, handle, catalog_name)
        values ('22222222-2222-2222-2222-222222222222', 'sbx-n4', 'staging') $q$,
    'a deleted sandbox frees its catalog name for any user');
end;
$$ language plpgsql;
