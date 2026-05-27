-- Adds the `ManageDataPlane` capability bundle. Backfill onto the per-tenant
-- role_grants installed by `create_data_plane.rs` at private data-plane
-- provisioning time happens in the following migration; `alter type add
-- value` cannot be used in the same transaction that subsequently references
-- the new value.

alter type capability_bundle add value if not exists 'manage_data_plane';
