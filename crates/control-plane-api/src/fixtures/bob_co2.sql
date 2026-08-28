-- The `bobCo2/` tenant: two users and one private data plane.
--
-- Its name deliberately shares a prefix with `bobCo/`, so tests that filter or
-- authorize by tenant have a neighbour to be wrong about: a check that matches
-- on `bobCo` rather than the full `bobCo/` path segment sees this tenant's
-- plane too. Load it alongside `bob_co.sql` for that pairing.
--
--   * bob holds the same permissions here as in `bobCo/`: he reads every plane
--     the tenant can reach, and manages its private one.
--   * carol reads only public data planes. Her tenant grant is `read`, which
--     conveys the `Viewer` bundle without `Delegate`, so authorization stops at
--     `bobCo2/` and never chains through the `role_grants` edges out to the
--     private plane. Public planes come from a `user_grants` row addressed
--     directly at `ops/dp/public/`, which needs no chaining to apply.
do $$
declare
  bob_uid uuid := '22222222-2222-2222-2222-222222222222';
  carol_uid uuid := '33333333-3333-3333-3333-333333333333';
begin

  insert into public.tenants (id, tenant) values
    (internal.id_generator(), 'bobCo2/')
  ;

  -- Mirrors the single-plane shape of `bob_co.sql`, down to the region and
  -- cluster tag, so the tenant path is the only thing that distinguishes this
  -- plane's name from `ops/dp/private/bobCo/aws-us-east-1-c1`.
  insert into public.data_planes (
    id,
    data_plane_name,
    data_plane_fqdn,
    hmac_keys,
    broker_address,
    reactor_address,
    ops_logs_name,
    ops_stats_name,
    ops_l1_events_name,
    ops_l1_inferred_name,
    ops_l1_stats_name,
    ops_l2_events_transform,
    ops_l2_inferred_transform,
    ops_l2_stats_transform,
    enable_l2,
    cidr_blocks
  )
  select
    dp.id::flowid,
    dp.name::catalog_name,
    'dp.' || dp.slug,
    '{c2VjcmV0,b3RoZXI=}'::text[],
    'broker.dp.' || dp.slug,
    'reactor.dp.' || dp.slug,
    ('ops/tasks/' || dp.slug || '/logs')::catalog_name,
    ('ops/tasks/' || dp.slug || '/stats')::catalog_name,
    ('ops/rollups/L1/' || dp.slug || '/events')::catalog_name,
    ('ops/rollups/L1/' || dp.slug || '/inferred')::catalog_name,
    ('ops/rollups/L1/' || dp.slug || '/stats')::catalog_name,
    'from.dp.' || dp.slug,
    'from.dp.' || dp.slug,
    'from.dp.' || dp.slug,
    false,
    '{10.30.0.0/16}'::cidr[]
  from (values
    ('667777777777', 'ops/dp/private/bobCo2/aws-us-east-1-c1',   'private/bobCo2-one')
  ) as dp(id, name, slug)
  ;

  insert into public.user_grants (user_id, object_role, capability) values
    (bob_uid, 'bobCo2/', 'admin'),
    (carol_uid, 'bobCo2/', 'read'),
    -- Carol's only data-plane visibility. Granted at the public prefix itself
    -- because her tenant grant, lacking `Delegate`, reaches no plane at all.
    (carol_uid, 'ops/dp/public/', 'read')
  ;

  -- The tenant reads its own private plane and the shared public ones. `read`
  -- conveys the `Viewer` bundle, which includes `ViewDataPlanePrivateNetworking`
  -- but stops short of modifying it.
  insert into public.role_grants (subject_role, object_role, capability) values
    ('bobCo2/', 'ops/dp/public/', 'read'),
    ('bobCo2/', 'ops/dp/private/bobCo2/', 'read')
  ;

  -- Bob alone manages the private plane, exactly as in `bob_co.sql`.
  insert into public.user_grants (user_id, object_role, capability, bundles) values
    (bob_uid, 'ops/dp/private/bobCo2/', 'read', '{manage_data_plane}')
  ;

end
$$;
