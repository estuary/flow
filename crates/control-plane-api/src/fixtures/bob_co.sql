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
  -- SOPS-encrypted HMAC keys, as in `data_planes.sql`.
  encrypted_keys json := $sops${"hmac_keys":["ENC[AES256_GCM,data:kgzvBY4pczo=,iv:lA0c8tGWTC866p25il82vspY50Kl8oGWqJzNaw/zWyc=,tag:xA68U8wdFjlixqI8/zjQkA==,type:str]","ENC[AES256_GCM,data:q5Y5mbUzwRQ=,iv:bla/xToMmLdlf9N7z9Z2pxyWmpa0lQf9wizKspnyaTU=,tag:CR2QyhoO5crrDM6ZRoPPIw==,type:str]"],"sops":{"age":[{"recipient":"age1z2qskpk8ww2rx5frxzykryufkfm9sj03v56m3qu54y0lgndytp3sdg24vs","enc":"-----BEGIN AGE ENCRYPTED FILE-----\nYWdlLWVuY3J5cHRpb24ub3JnL3YxCi0+IFgyNTUxOSBtZ2ZwQks2T2Zna3h4OFov\nVjJTSzl4TGVyRnIrVk40bndWNVVmZFJQcXdZCkZpY3BlYnRKWUhZYlFnNk50dHh3\nbHlNY0R2bFI1S3pKVkJhbURHM3hYZFEKLS0tIDR0OTMrZEJkRHhKb0FaaXV1K2Uy\nNFd0bHd4TGFMK0xFUUFQVUtta3JHTlEKFBocCSHvSypsA7UcD7emwNDDS4Q3g2si\nqG+pyMMp2yObmT5iN/VAodv4un8Vr1LvcsKxEGXwTOjOnFCAk+I1Kw==\n-----END AGE ENCRYPTED FILE-----\n"}],"lastmodified":"2026-01-15T01:29:57Z","mac":"ENC[AES256_GCM,data:LTZlM4OPKZRu4J9HYUHn5fF6OBP8APXibP47sa/ADpMPA09h1q7Ivg+EAwJlqX9Ee9h8B7iEZD6CMxEyuz94T+nqiklBt4jZIQYq9GPd4xBp1EUvzSITmkvILYkkHh+A+vYU0cH/CemPv/w87wNt+QeNtsQj2xowufsr7RXCyXQ=,iv:z7t+spQvkzLMZZ3LA3eWmtZZdWKPVghKCP5M3HsQ6og=,tag:zrgfUxvTWsRuC8/2kEu6zw==,type:str]","unencrypted_suffix":"_unencrypted","version":"3.11.0"}}$sops$;
begin

  insert into public.tenants (id, tenant) values
    (internal.id_generator(), 'bobCo/')
  ;

  -- Every column that isn't a plane's identity is derived from its slug:
  -- fixtures assert over names and capabilities, and uniform addresses keep
  -- the per-plane rows to a line each.
  insert into public.data_planes (
    id,
    data_plane_name,
    data_plane_fqdn,
    hmac_keys,
    encrypted_hmac_keys,
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
    '{}',
    encrypted_keys,
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
