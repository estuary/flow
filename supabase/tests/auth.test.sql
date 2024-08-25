-- Note that seed.sql installs fitures into auth.users (alice, bob, carol, dave)
-- having UUIDs like 1111*, 2222*, 3333*, etc.
create function set_authenticated_context(test_user_id uuid)
returns void as $$
begin

  set role postgres;
  execute 'set session request.jwt.claim.sub to "' || test_user_id::text || '"';
  set role authenticated;

end
$$ language plpgsql;

-- Users can access their current authorization context.
create function tests.test_auth_uid()
returns setof text as $$
  select set_authenticated_context('11111111-1111-1111-1111-111111111111');
  select is(auth_uid(), '11111111-1111-1111-1111-111111111111', 'we''re authorized as alice');
$$ language sql;

-- We can resolve all roles granted with a minimum capability.
-- This is commonly used for row-level security checks.
create function tests.test_auth_roles()
returns setof text as $$

  -- Replace seed grants with fixtures for this test.
  delete from user_grants;
  insert into user_grants (user_id, object_role, capability) values
    ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin'),
    ('22222222-2222-2222-2222-222222222222', 'bobCo/', 'admin'),
    ('33333333-3333-3333-3333-333333333333', 'carolCo/', 'read'),
    ('44444444-4444-4444-4444-444444444444', 'daveCo/', 'admin'),
    ('44444444-4444-4444-4444-444444444444', 'aliceCo/dave-can-read/', 'read')
  ;

  delete from role_grants;
  insert into role_grants (subject_role, object_role, capability) values
    ('aliceCo/widgets/', 'bobCo/burgers/', 'admin'),
    ('aliceCo/anvils/', 'carolCo/paper/', 'write'),
    ('aliceCo/duplicate/', 'carolCo/paper/', 'read'),
    ('aliceCo/stuff/', 'carolCo/shared/', 'read'),
    ('bobCo/alice-vendor/', 'aliceCo/bob-shared/', 'admin'),
    ('carolCo/shared/', 'carolCo/hidden/', 'read'),
    ('daveCo/hidden/', 'carolCo/hidden/', 'admin'),
    ('carolCo/hidden/', 'carolCo/even/more/hidden/', 'read')
  ;

  -- Assert Alice's present roles.
  select set_authenticated_context('11111111-1111-1111-1111-111111111111');
  select results_eq(
    $i$ select role_prefix::text, capability::text from auth_roles() $i$,
    $i$ values  ('aliceCo/','admin'),
                ('bobCo/burgers/','admin'),
                ('carolCo/paper/','write'),
                ('carolCo/shared/', 'read')
    $i$,
    'alice roles'
  );

  -- Assert Bob's roles.
  select set_authenticated_context('22222222-2222-2222-2222-222222222222');
  select results_eq(
    $i$ select role_prefix::text, capability::text from auth_roles() $i$,
    $i$ values  ('aliceCo/bob-shared/','admin'), ('bobCo/','admin') $i$,
    'bob roles'
  );

  -- Assert Carol's.
  select set_authenticated_context('33333333-3333-3333-3333-333333333333');
  select results_eq(
    $i$ select role_prefix::text, capability::text from auth_roles() $i$,
    $i$ values  ('carolCo/','read') $i$,
    'carol roles'
  );

  -- And Dave's.
  select set_authenticated_context('44444444-4444-4444-4444-444444444444');
  select results_eq(
    $i$ select role_prefix::text, capability::text from auth_roles() $i$,
    $i$ values  ('aliceCo/dave-can-read/','read'),
                ('carolCo/even/more/hidden/', 'read'),
                ('carolCo/hidden/', 'admin'),
                ('daveCo/', 'admin')
    $i$,
    'dave roles'
  );

  -- Make Carol an admin of carolCo/.
  set role postgres;
  update user_grants set capability = 'admin' where object_role = 'carolCo/';

  -- Now Carol also receives the projected carolCo/hidden/ grant,
  -- which is technically redundant with her 'admin' grant.
  select set_authenticated_context('33333333-3333-3333-3333-333333333333');
  select results_eq(
    $i$ select role_prefix::text, capability::text from auth_roles() $i$,
    $i$ values  ('carolCo/','admin'),
                ('carolCo/even/more/hidden/','read'),
                ('carolCo/hidden/','read')
    $i$,
    'carol roles'
  );

  -- Assert Alice's user_grants visibility.
  select set_authenticated_context('11111111-1111-1111-1111-111111111111');
  select results_eq(
    $i$ select user_id::text, object_role::text, capability::text from user_grants
    $i$,
    $i$ values  ('11111111-1111-1111-1111-111111111111','aliceCo/','admin'),
                ('44444444-4444-4444-4444-444444444444','aliceCo/dave-can-read/','read')
    $i$,
    'alice user_grants visibility'
  );

  -- Assert Alice's role_grants visibility.
  select results_eq(
    $i$ select subject_role::text, object_role::text, capability::text from role_grants
    $i$,
    $i$ values  ('aliceCo/widgets/','bobCo/burgers/','admin'),
                ('aliceCo/anvils/','carolCo/paper/','write'),
                ('aliceCo/duplicate/','carolCo/paper/','read'),
                ('aliceCo/stuff/','carolCo/shared/','read'),
                ('bobCo/alice-vendor/','aliceCo/bob-shared/','admin')
    $i$,
    'alice role_grants visibility'
  );

  set role postgres;

  select results_eq(
    $i$ select role_prefix::text, capability::text from internal.task_roles('aliceCo/anvils/thing') $i$,
    $i$ values  ('carolCo/paper/','write')
    $i$,
    'aliceCo/anvils/thing roles'
  );
  select is_empty(
    $i$ select role_prefix::text, capability::text from internal.task_roles('aliceCo/anvils/thing', 'admin') $i$,
    'aliceCo/anvils/thing roles when admin'
  );
  select results_eq(
    $i$ select role_prefix::text, capability::text from internal.task_roles('daveCo/hidden/task/') $i$,
    $i$ values  ('carolCo/even/more/hidden/','read'),
                ('carolCo/hidden/','admin')
    $i$,
    'daveCo/hidden/task roles'
  );


$$ language sql;

create function tests.test_user_info_summary()
returns setof text as $$
    select set_authenticated_context('11111111-1111-1111-1111-111111111111');

    select is(
        (select user_info_summary())::jsonb,
        '{"hasDemoAccess": false, "hasSupportAccess": false, "hasAnyAccess": false}'::jsonb
    );

    insert into user_grants (user_id, object_role, capability) values (
        '11111111-1111-1111-1111-111111111111' ,
        'alice/',
        'read'
    );

    select is(
        (select user_info_summary())::jsonb,
        '{"hasDemoAccess": false, "hasSupportAccess": false, "hasAnyAccess": true}'::jsonb
    );

    insert into user_grants (user_id, object_role, capability) values (
        '11111111-1111-1111-1111-111111111111',
        'alice_admin/',
        'admin'
    );

    insert into role_grants (subject_role, object_role, capability) values (
        'alice_admin/',
        'demo/',
        'read'
    );

    select is(
        (select user_info_summary())::jsonb,
        '{"hasDemoAccess": true, "hasSupportAccess": false, "hasAnyAccess": true}'::jsonb
    );

    insert into role_grants (subject_role, object_role, capability) values (
        'alice_admin/',
        'estuary_support/',
        'admin'
    );

    select is(
        (select user_info_summary())::jsonb,
        '{"hasDemoAccess": true, "hasSupportAccess": true, "hasAnyAccess": true}'::jsonb
    );

$$ language sql;

create function tests.test_gateway_auth_token_generation()
returns setof text as $$
  select set_authenticated_context('11111111-1111-1111-1111-111111111111');

  -- Replace seed grants with fixtures for this test.
  delete from user_grants;
  insert into user_grants (user_id, object_role, capability) values
    ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin');

  delete from role_grants;
  insert into role_grants (subject_role, object_role, capability) values
    ('aliceCo/widgets/', 'bobCo/foo/', 'admin'),
    ('aliceCo/', 'carolCo/bar/', 'admin');

  -- Request valid scopes, and assert that the prefixes of the token match what we requested
  -- The duplication of aliceCo/a/ is here to exercise the deduplication logic, which was in place
  -- prior to me refactoring that function. IDK if that's important, but it's still there.
  select results_eq(
    $i$ select t.gateway_url as gurl, v.payload->>'prefixes' as pres
          from gateway_auth_token('aliceCo/a/', 'aliceCo/b/', 'bobCo/foo/', 'carolCo/bar/baz/', 'aliceCo/a/') t
          cross join verify(t.token, 'supersecret') v
    $i$,
    $i$ values (
          'https://localhost:28318/',
          '["aliceCo/a/","aliceCo/b/","bobCo/foo/","carolCo/bar/baz/"]'
        )
    $i$,
    'good gateway token'
  );

  -- Request invalid scopes, and expect an error
  select throws_ok(
    $i$ select * from gateway_auth_token('notauthorized/') $i$
  );

  -- Request a mix of valid and invalid scopes, and expect an error
  select throws_ok(
    $i$ select * from gateway_auth_token('aliceCo/a/', 'bobCo/notauthorized/') $i$
  );

$$ language sql;
