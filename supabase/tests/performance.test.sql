
-- Use a name that orders after other tests so that logged timings
-- are more easily surfaced by test runs.
create function tests.test_zzz_performance()
returns setof text as $$
declare
  user_index integer;
  user_id uuid;
  user_email text;
  tenant text;
  started_at timestamp;
begin

  started_at = clock_timestamp(); -- Time fixture creation.

  -- Generate a complex fixture of user and role grants,
  -- where _many_ prefixes are shared with Alice but not Dave.
  delete from user_grants;
  delete from role_grants;

  insert into user_grants (user_id, object_role, capability) values
    ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin'),
    ('44444444-4444-4444-4444-444444444444', 'daveCo/', 'admin');

  insert into role_grants (subject_role, object_role, capability) values
    ('aliceCo/', 'aliceCo/', 'admin'),
    ('daveCo/', 'daveCo/', 'admin'),
    ('aliceCo/', 'support/', 'admin');

  with insert_live as (
    insert into live_specs (catalog_name, spec_type, spec)
    values
        ('daveCo/the/capture', 'capture', '{}'),
        ('daveCo/and/collection', 'collection', '{}')
    returning controller_task_id
  )
  insert into internal.tasks (task_id, task_type)
  select controller_task_id, 2 from insert_live;

  -- Now create a slew of tenants, each with role & user grants
  -- and multiple live specs.

  for user_index in
    select s from generate_series(1000, 4999) s
  loop

    user_id = gen_random_uuid();
    user_email = 'email' || user_index || '@foo.com';
    tenant = 'tenant_' || user_index || '/';

    insert into auth.users (id, email)
    values (user_id, user_email);

    insert into user_grants (user_id, object_role, capability)
    values (user_id, tenant, 'admin');

    insert into role_grants (subject_role, object_role, capability)
    values  (tenant, tenant, 'admin'),
            (tenant, 'ops/' || tenant, 'read'),
            (tenant, 'shared/public/', 'read'),
            ('support/', tenant, 'admin'),
            (tenant, 'aliceCo/shared/', 'read');

    with insert_live as (
        insert into live_specs (catalog_name, spec_type, spec)
        values (tenant || 'my-cool/capture', 'capture', '{}'),
            (tenant || 'my-cool/collection-a', 'collection', '{}'),
            (tenant || 'my-cool/collection-b', 'collection', '{}'),
            (tenant || 'my-cool/collection-c', 'collection', '{}'),
            (tenant || 'my-important/derivation.v1', 'collection', '{}'),
            (tenant || 'my-important/derivation.v2', 'collection', '{}'),
            (tenant || 'my-first/materialization', 'materialization', '{}'),
            (tenant || 'my-second/materialization', 'materialization', '{}')
        returning controller_task_id
    )
    insert into internal.tasks (task_id, task_type)
    select controller_task_id, 2 from insert_live;

  end loop;

  analyze;
  return query select diag('fixture creation took: ' || (clock_timestamp() - started_at));

  -- Drop priviledge and evaluate as Alice.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  /* -- Uncomment & modify me to view an EXPLAIN ANALYZE execution plan.
  return query select is_empty(
    $i$ explain analyze select count(*) from combined_grants_ext $i$,
    'explain analyze'
  );
  */

  started_at = clock_timestamp();
  return query select results_eq(
    $i$ select count(*) from auth_roles() $i$,
    $i$ values (8004::bigint) $i$,
    'alice auth_roles'
  );
  return query select diag('alice auth_roles took: ' || (clock_timestamp() - started_at));

  started_at = clock_timestamp();
  return query select results_eq(
    $i$ select count(*) from live_specs_ext $i$,
    $i$ values (32000::bigint) $i$,
    'alice live_specs_ext'
  );
  return query select diag('alice live_specs_ext took: ' || (clock_timestamp() - started_at));

  started_at = clock_timestamp();
  return query select results_eq(
    $i$ select count(*) from combined_grants_ext $i$,
    $i$ values (24003::bigint) $i$,
    'alice combined_grants_ext'
  );
  return query select diag('alice combined_grants_ext took: ' || (clock_timestamp() - started_at));

  -- Now evaluate as Dave, who has far less visibility.
  perform set_authenticated_context('44444444-4444-4444-4444-444444444444');

  started_at = clock_timestamp();
  return query select results_eq(
    $i$ select count(*) from auth_roles() $i$,
    $i$ values (1::bigint) $i$,
    'dave auth_roles'
  );
  return query select diag('dave auth_roles took: ' || (clock_timestamp() - started_at));

  started_at = clock_timestamp();
  return query select results_eq(
    $i$ select count(*) from live_specs_ext $i$,
    $i$ values (2::bigint) $i$,
    'dave live_specs_ext'
  );
  return query select diag('dave live_specs_ext took: ' || (clock_timestamp() - started_at));

  started_at = clock_timestamp();
  return query select results_eq(
    $i$ select count(*) from combined_grants_ext $i$,
    $i$ values (2::bigint) $i$,
    'dave combined_grants_ext'
  );
  return query select diag('dave combined_grants_ext took: ' || (clock_timestamp() - started_at));

end
$$ language plpgsql;
