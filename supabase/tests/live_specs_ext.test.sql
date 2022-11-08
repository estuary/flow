-- We can resolve all roles granted with a minimum capability.
-- This is commonly used for row-level security checks.
create function tests.test_live_specs_ext()
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

  -- seed live_specs, publication_specs, connectors and connector_tags 
  delete from publications;
  insert into publications (id, user_id) values
    ('0101010101010101', '11111111-1111-1111-1111-111111111111');

  delete from live_specs;
  insert into live_specs (id, catalog_name, last_build_id, last_pub_id) values
    ('0202020202020202', 'aliceCo/widgets/test1', '0101010101010101', '0101010101010101'),
    -- alice is authorised to access carolCo through two different roles_grants, but
    -- must only be returned once in live_specs_ext
    ('0303030303030303', 'carolCo/paper/test1', '0101010101010101', '0101010101010101'),
    -- alice is not authorised to access unknownCo
    ('0404040404040404', 'unknownCo/foo/bar', '0101010101010101', '0101010101010101')
  ;

  delete from publication_specs;
  insert into publication_specs (live_spec_id, pub_id, user_id) values
    ('0202020202020202', '0101010101010101', '11111111-1111-1111-1111-111111111111')
  ;

  select results_eq(
    $i$ select catalog_name from
        live_specs_ext
    $i$,
    $i$ VALUES  ('aliceCo/widgets/test1'::catalog_name), ('carolCo/paper/test1'::catalog_name)
    $i$,
    'alice live specs'
  );

$$ language sql;
