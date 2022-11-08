
-- Storage mappings of catalog prefixes.
create table storage_mappings (
  like internal._model including all,

  catalog_prefix    catalog_prefix unique not null,
  spec              json not null
);
alter table storage_mappings enable row level security;

create policy "Users must be authorized to the specification catalog prefix"
  on storage_mappings as permissive for select
  using (auth_catalog(catalog_prefix, 'read'));
grant select on storage_mappings to authenticated;

comment on table storage_mappings is
  'Storage mappings which are applied to published specifications';
comment on column storage_mappings.catalog_prefix is
  'Catalog prefix which this storage mapping prefixes';
comment on column storage_mappings.spec is
  'Specification of this storage mapping';