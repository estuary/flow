-- Backfill the `ManageDataPlane` bundle onto the per-tenant role_grants that
-- `create_data_plane.rs` installs at private data-plane provisioning time.
-- Without this, tenant admins of already-provisioned private data planes
-- cannot exercise the `ModifyDataPlanePrivateNetworking` capability bit.

update role_grants
set bundles = array_append(bundles, 'manage_data_plane'::capability_bundle)
where object_role::text = 'ops/dp/private/' || subject_role::text
  and capability = 'read'
  and not (bundles @> array['manage_data_plane'::capability_bundle]);
