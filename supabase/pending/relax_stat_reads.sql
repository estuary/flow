alter policy "Users must be authorized to the catalog name"
  on catalog_stats
  using (auth_catalog(catalog_name, 'read'));