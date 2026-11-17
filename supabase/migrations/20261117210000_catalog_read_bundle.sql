-- Lets clients check for catalog read access after token masking.
alter type capability_bundle add value if not exists 'catalog_read';
