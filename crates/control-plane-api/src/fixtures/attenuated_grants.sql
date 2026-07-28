-- Grant paths whose *raw* legacy capability reaches a data-plane prefix with
-- `admin`, but whose *effective* (attenuated) authority differs. Used to pin
-- that data-plane visibility is decided by effective authority — the exact
-- regression where a filter consults the raw legacy capability of the edge
-- which reached the prefix.
--
-- Both users traverse the same 2-hop shape through `sharedCo/`:
--
--   user_grant(user, 'sharedCo/', C, B) -> role_grant('sharedCo/' -> 'ops/dp/public/', 'admin')
--
-- The role_grant node's effective bits are `admin`'s bits intersected with
-- what the parent may delegate:
--
--  * erin:  'none' + '{editor}' delegates CatalogRead|JournalRead|SpecEdit|Delegate,
--    which misses ViewDataPlanePrivateNetworking — so she fails the Viewer
--    requirement of `Capability::Read` despite the raw `admin` edge.
--  * frank: 'read' + '{delegate}' delegates the full Viewer set, so the same
--    path *does* authorize him: the positive control proving the traversal
--    works and only attenuation blocks erin.
--
-- Their own tenants deliberately hold no role_grants: a second path to
-- `ops/dp/public/` would union its bits into the plane node and mask the
-- attenuation under test.
insert into auth.users (id, email) values
  ('55555555-5555-5555-5555-555555555555', 'erin@example.com'),
  ('66666666-6666-6666-6666-666666666666', 'frank@example.com')
;
insert into public.user_grants (user_id, object_role, capability, bundles) values
  ('55555555-5555-5555-5555-555555555555', 'erinCo/', 'admin', '{}'),
  ('55555555-5555-5555-5555-555555555555', 'sharedCo/', 'none', '{editor}'),
  ('66666666-6666-6666-6666-666666666666', 'frankCo/', 'admin', '{}'),
  ('66666666-6666-6666-6666-666666666666', 'sharedCo/', 'read', '{delegate}')
;
insert into public.role_grants (subject_role, object_role, capability, bundles) values
  ('sharedCo/', 'ops/dp/public/', 'admin', '{}')
;
