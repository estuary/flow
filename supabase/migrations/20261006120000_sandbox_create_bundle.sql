-- Sandbox creation is opt-in, separate from the Admin bundle.
alter type capability_bundle add value if not exists 'sandbox_create';
