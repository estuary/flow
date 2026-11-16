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
--
-- The tenant is also flagged `sensitive`, while `bobCo/` keeps the default, so
-- tests can compare a sensitive tenant against one that isn't.
do $$
declare
  bob_uid uuid := '22222222-2222-2222-2222-222222222222';
  carol_uid uuid := '33333333-3333-3333-3333-333333333333';
  -- SOPS-encrypted HMAC keys, as in `data_planes.sql`.
  encrypted_keys json := $sops${"hmac_keys":["ENC[AES256_GCM,data:kgzvBY4pczo=,iv:lA0c8tGWTC866p25il82vspY50Kl8oGWqJzNaw/zWyc=,tag:xA68U8wdFjlixqI8/zjQkA==,type:str]","ENC[AES256_GCM,data:q5Y5mbUzwRQ=,iv:bla/xToMmLdlf9N7z9Z2pxyWmpa0lQf9wizKspnyaTU=,tag:CR2QyhoO5crrDM6ZRoPPIw==,type:str]"],"sops":{"age":[{"recipient":"age1z2qskpk8ww2rx5frxzykryufkfm9sj03v56m3qu54y0lgndytp3sdg24vs","enc":"-----BEGIN AGE ENCRYPTED FILE-----\nYWdlLWVuY3J5cHRpb24ub3JnL3YxCi0+IFgyNTUxOSBtZ2ZwQks2T2Zna3h4OFov\nVjJTSzl4TGVyRnIrVk40bndWNVVmZFJQcXdZCkZpY3BlYnRKWUhZYlFnNk50dHh3\nbHlNY0R2bFI1S3pKVkJhbURHM3hYZFEKLS0tIDR0OTMrZEJkRHhKb0FaaXV1K2Uy\nNFd0bHd4TGFMK0xFUUFQVUtta3JHTlEKFBocCSHvSypsA7UcD7emwNDDS4Q3g2si\nqG+pyMMp2yObmT5iN/VAodv4un8Vr1LvcsKxEGXwTOjOnFCAk+I1Kw==\n-----END AGE ENCRYPTED FILE-----\n"}],"lastmodified":"2026-01-15T01:29:57Z","mac":"ENC[AES256_GCM,data:LTZlM4OPKZRu4J9HYUHn5fF6OBP8APXibP47sa/ADpMPA09h1q7Ivg+EAwJlqX9Ee9h8B7iEZD6CMxEyuz94T+nqiklBt4jZIQYq9GPd4xBp1EUvzSITmkvILYkkHh+A+vYU0cH/CemPv/w87wNt+QeNtsQj2xowufsr7RXCyXQ=,iv:z7t+spQvkzLMZZ3LA3eWmtZZdWKPVghKCP5M3HsQ6og=,tag:zrgfUxvTWsRuC8/2kEu6zw==,type:str]","unencrypted_suffix":"_unencrypted","version":"3.11.0"}}$sops$;
begin

  insert into public.tenants (id, tenant, sensitive) values
    (internal.id_generator(), 'bobCo2/', true)
  ;

  -- Mirrors the single-plane shape of `bob_co.sql`, down to the region and
  -- cluster tag, so the tenant path is the only thing that distinguishes this
  -- plane's name from `ops/dp/private/bobCo/aws-us-east-1-c1`.
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
