-- Independent bundles for the data-plane private networking permissions, which
-- the dashboard checks to decide whether private links can be viewed or changed.
alter type capability_bundle add value if not exists 'view_data_plane_private_networking';
alter type capability_bundle add value if not exists 'modify_data_plane_private_networking';
