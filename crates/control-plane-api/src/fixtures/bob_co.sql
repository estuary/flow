-- The `bobCo/` tenant: two users and three private data planes.
--
-- It exists so tests can exercise queries that must distinguish one
-- tenant's data planes from another's.
--
--   * alice and bob both read every plane bobCo/ can reach. Their tenant-level
--     `admin` grant carries `Delegate`, so authorization chains through the
--     `role_grants` edges below and out to each plane.
--   * only bob manages bobCo's private planes. The `manage_data_plane` bundle
--     rides on a `user_grants` row addressed to bob, rather than on the
--     tenant-wide role edge, precisely so it does not reach alice.
do $$
declare
  alice_uid uuid := '11111111-1111-1111-1111-111111111111';
  bob_uid uuid := '22222222-2222-2222-2222-222222222222';
begin

  insert into public.tenants (id, tenant) values
    (internal.id_generator(), 'bobCo/')
  ;

  -- `hmac_keys` is populated directly (rather than via `encrypted_hmac_keys`)
  -- so the snapshot loader takes these rows as live without exercising the
  -- SOPS decrypt path, which `data_planes.sql` already covers. Every column
  -- that isn't a plane's identity is derived from its slug: fixtures assert
  -- over names and capabilities, and uniform addresses keep the per-plane
  -- rows to a line each.
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
    '{10.20.0.0/16}'::cidr[]
  from (values
    ('663333333333', 'ops/dp/private/bobCo/aws-us-east-1-c1',    'private/bobCo-one'),
    ('664444444444', 'ops/dp/private/bobCo/gcp-us-central1-c1',  'private/bobCo-two'),
    ('665555555555', 'ops/dp/private/bobCo/az-eastus-c1',        'private/bobCo-three')
  ) as dp(id, name, slug)
  ;

  insert into public.user_grants (user_id, object_role, capability) values
    (alice_uid, 'bobCo/', 'admin'),
    (bob_uid, 'bobCo/', 'admin')
  ;

  -- The tenant reads its own private planes and the shared public ones. `read`
  -- conveys the `Viewer` bundle, which includes `ViewDataPlanePrivateNetworking`
  -- but stops short of modifying it.
  insert into public.role_grants (subject_role, object_role, capability) values
    ('bobCo/', 'ops/dp/public/', 'read'),
    ('bobCo/', 'ops/dp/private/bobCo/', 'read')
  ;

  -- Bob alone manages the private planes. Mirrors what `create_data_plane.rs`
  -- installs at provisioning time (legacy `read` for RLS/`user_roles()`, plus
  -- the `ManageDataPlane` bundle for the capability bits), but addressed to a
  -- user rather than the tenant role.
  insert into public.user_grants (user_id, object_role, capability, bundles) values
    (bob_uid, 'ops/dp/private/bobCo/', 'read', '{manage_data_plane}')
  ;

end
$$;
