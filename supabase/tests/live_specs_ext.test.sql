create function tests.test_live_specs_ext()
returns setof text as $$

  -- Replace seed grants with fixtures for this test.
  delete from user_grants;
  insert into user_grants (user_id, object_role, capability) values
    ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin'),
    ('33333333-3333-3333-3333-333333333333', 'carolCo/', 'read')
  ;

  delete from role_grants;
  insert into role_grants (subject_role, object_role, capability) values
    ('aliceCo/anvils/', 'carolCo/paper/', 'write'),
    ('aliceCo/duplicate/', 'carolCo/paper/', 'read'),
    ('carolCo/shared/', 'carolCo/hidden/', 'read')
  ;

  -- seed live_specs, publication_specs, connectors and connector_tags
  delete from publications;
  insert into publications (id, user_id) values
    ('0101010101010101', '11111111-1111-1111-1111-111111111111');

  delete from live_specs;
  with insert_live as (
    insert into live_specs (id, catalog_name, last_build_id, last_pub_id, spec_type) values
        ('0202020202020202', 'aliceCo/widgets/test1', '0101010101010101', '0101010101010101', 'test'),
        -- alice is authorised to access carolCo through two different roles_grants, but
        -- must only be returned once in live_specs_ext
        ('0303030303030303', 'carolCo/paper/test1', '0101010101010101', '0101010101010101', 'test'),
        -- alice is not authorised to access unknownCo
        ('0404040404040404', 'unknownCo/foo/bar', '0101010101010101', '0101010101010101', 'collection')
    returning controller_task_id
  )
  insert into internal.tasks (task_id, task_type)
  select controller_task_id, 2 from insert_live;

  delete from publication_specs;
  insert into publication_specs (live_spec_id, pub_id, user_id) values
    ('0202020202020202', '0101010101010101', '11111111-1111-1111-1111-111111111111')
  ;

  select set_authenticated_context('11111111-1111-1111-1111-111111111111');

  -- Assert expectations of Alice's present roles.
  select results_eq(
    $i$ select role_prefix::text, capability::text
        from auth_roles()
        order by role_prefix, capability
    $i$,
    $i$ values  ('aliceCo/','admin'),
                ('carolCo/paper/','write')
    $i$,
    'alice roles'
  );

  -- Assert Alice's live_specs_ext visibility.
  select results_eq(
    $i$ select catalog_name::text from live_specs_ext order by catalog_name $i$,
    $i$ values  ('aliceCo/widgets/test1'), ('carolCo/paper/test1')
    $i$,
    'alice live specs'
  );

$$ language sql;
