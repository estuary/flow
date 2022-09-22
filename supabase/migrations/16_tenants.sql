
create table tenants (
  like internal._model including all,

  -- TODO(johnny): In the future, we expect to hang billing
  -- and data-plane assignment onto this record.
  tenant      catalog_tenant unique not null
);
alter table tenants enable row level security;

create policy "Users must be authorized to their catalog tenant"
  on tenants as permissive for select
  using (auth_catalog(tenant, 'admin'));
grant select on tenants to authenticated;

comment on table tenants is '
A tenant is the top-level unit of organization in the Flow catalog namespace.
';
comment on column tenants.tenant is
  'Catalog tenant identified by this record';