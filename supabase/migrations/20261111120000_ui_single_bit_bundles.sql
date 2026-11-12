-- Independent bundles for the permissions used by UI component visibility checks.
alter type capability_bundle add value if not exists 'view_billing';
alter type capability_bundle add value if not exists 'edit_billing';
alter type capability_bundle add value if not exists 'edit_secret';
