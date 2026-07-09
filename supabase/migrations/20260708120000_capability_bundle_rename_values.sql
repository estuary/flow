begin;

-- Align the `capability_bundle` enum labels with the renamed
-- `models::authz::CapabilityBundle` variants. `ALTER TYPE ... RENAME VALUE`
-- relabels each value in place, so existing `user_grants.bundles` and
-- `role_grants.bundles` arrays carry over without a backfill. No SQL
-- functions, policies, or check constraints reference these literals.
alter type capability_bundle rename value 'viewer' to 'view';
alter type capability_bundle rename value 'writer' to 'write';
alter type capability_bundle rename value 'editor' to 'edit';
alter type capability_bundle rename value 'billing' to 'manage_billing';
alter type capability_bundle rename value 'team_admin' to 'manage_users';
alter type capability_bundle rename value 'manage_data_plane' to 'manage_data_planes';

commit;
