-- Independent bundles for viewing and modifying data-plane private networking.
alter type capability_bundle add value if not exists 'view_data_plane_private_networking';
alter type capability_bundle add value if not exists 'modify_data_plane_private_networking';
