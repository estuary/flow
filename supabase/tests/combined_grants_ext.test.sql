create function tests.test_combined_grants_ext()
returns setof text as $$

  delete from user_grants;
  insert into user_grants (user_id, object_role, capability) values
    ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin'),
    ('22222222-2222-2222-2222-222222222222', 'aliceCo/bob-stuff/', 'admin'),
    ('22222222-2222-2222-2222-222222222222', 'bobCo/', 'admin'),
    ('33333333-3333-3333-3333-333333333333', 'carolCo/', 'read'),
    ('44444444-4444-4444-4444-444444444444', 'daveCo/', 'admin')
  ;

  delete from role_grants;
  insert into role_grants (subject_role, object_role, capability) values
    ('aliceCo/anvils/', 'carolCo/paper/', 'write'),
    ('aliceCo/duplicate/', 'carolCo/paper/', 'read'),
    ('aliceCo/stuff/', 'carolCo/shared/', 'read'),
    ('bobCo/burgers/', 'aliceCo/widgets/', 'read'),
    ('carolCo/shared/', 'carolCo/hidden/', 'read'),
    ('daveCo/hidden/', 'carolCo/hidden/', 'admin')
  ;
  select set_authenticated_context('11111111-1111-1111-1111-111111111111');

  -- Assert expectations of Alice's present roles.
  select results_eq(
    $i$ select role_prefix::text, capability::text
        from auth_roles()
        order by role_prefix, capability
    $i$,
    $i$ values  ('aliceCo/','admin'),
                ('carolCo/paper/','write'),
                ('carolCo/shared/', 'read')
    $i$,
    'alice roles'
  );

  -- Assert Alice's combined_grants_ext visibility.
  select results_eq(
    $i$
      select
        user_id::text,
        subject_role::text,
        object_role::text,
        capability::text
      from combined_grants_ext
    $i$,
    $i$
      values (null, 'aliceCo/anvils/', 'carolCo/paper/', 'write'),
             (null, 'aliceCo/duplicate/', 'carolCo/paper/', 'read'),
             (null, 'aliceCo/stuff/', 'carolCo/shared/', 'read'),
             (null, 'bobCo/burgers/', 'aliceCo/widgets/', 'read'),
             ('11111111-1111-1111-1111-111111111111', null, 'aliceCo/', 'admin'),
             ('22222222-2222-2222-2222-222222222222', null, 'aliceCo/bob-stuff/', 'admin')
    $i$,
    'alice combined grants'
  );

$$ language sql;
