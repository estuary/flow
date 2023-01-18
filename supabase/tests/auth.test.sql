create function tests.startup_auth_as_alice()
returns setof text as $$
begin

  -- Note that seed.sql installs fitures into auth.users (alice, bob, carol)
  -- as well as user_grants (to aliceCo/, bobCo/, carolCo/).
  set request.jwt.claim.sub to '11111111-1111-1111-1111-111111111111';

end;
$$ language plpgsql;

-- Users can access their current authorization context.
create function tests.test_auth_uid()
returns setof text as $$
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
    ('33333333-3333-3333-3333-333333333333', 'carolCo/', 'read')
  ;

  delete from role_grants;
  insert into role_grants (subject_role, object_role, capability) values
    ('aliceCo/widgets/', 'bobCo/burgers/', 'admin'),
    ('aliceCo/anvils/', 'carolCo/paper/', 'write'),
    ('aliceCo/duplicate/', 'carolCo/paper/', 'read'),
    ('aliceCo/stuff/', 'carolCo/shared/', 'read'),
    ('carolCo/shared/', 'carolCo/hidden/', 'read')
  ;

  select results_eq(
    $i$ select role_prefix::text, capability::text
        from auth_roles()
        order by role_prefix, capability
    $i$,
    $i$ VALUES  ('aliceCo/','admin'),
                ('bobCo/burgers/','admin'),
                ('carolCo/paper/','read'),
                ('carolCo/paper/','write'),
                ('carolCo/shared/', 'read')
    $i$,
    'alice roles'
  );

  select results_eq(
    $i$ select role_prefix::text, capability::text from
        internal.user_roles('22222222-2222-2222-2222-222222222222')
        order by role_prefix, capability
    $i$,
    $i$ VALUES  ('bobCo/','admin')
    $i$,
    'bob roles'
  );

  select results_eq(
    $i$ select role_prefix::text, capability::text from
        internal.user_roles('33333333-3333-3333-3333-333333333333')
        order by role_prefix, capability
    $i$,
    $i$ VALUES  ('carolCo/','read') $i$,
    'carol roles'
  );

  -- Make Carol an admin of caroCo/.
  update user_grants
  set capability = 'admin'
  where object_role = 'carolCo/';

  -- Now Carol also receives the projected carolCo/hidden/ grant,
  -- which is technically redundant with her 'admin' grant.
  select results_eq(
    $i$ select role_prefix::text, capability::text from
        internal.user_roles('33333333-3333-3333-3333-333333333333')
        order by role_prefix, capability
    $i$,
    $i$ VALUES  ('carolCo/','admin'), ('carolCo/hidden/','read') $i$,
    'carol roles'
  );

  select ok(auth_catalog('aliceCo/some/thing', 'write'));
  select ok(auth_catalog('aliceCo/other/thing/', 'admin'));
  select ok(auth_catalog('bobCo/burgers/time/', 'admin'));
  select ok(auth_catalog('carolCo/paper/company', 'write'));
  select ok(auth_catalog('carolCo/shared/thing', 'read'));

  select ok(not auth_catalog('carolCo/shared/thing', 'write'));
  select ok(not auth_catalog('carolCo/hidden/thing', 'read'));
  select ok(not auth_catalog('carolCo/paper/company', 'admin'));

$$ language sql;
