
create table tenants (
  like internal._model including all,

  -- TODO(johnny): In the future, we expect to hang billing
  -- and data-plane assignment onto this record.
  tenant                  catalog_tenant unique not null,

  captures_quota          integer        not null DEFAULT 10,
  derivations_quota       integer        not null DEFAULT 10,
  materializations_quota  integer        not null DEFAULT 10,
  collections_quota       integer        not null DEFAULT 100,

  captures_used          integer        not null DEFAULT 0,
  derivations_used       integer        not null DEFAULT 0,
  materializations_used  integer        not null DEFAULT 0,
  collections_used       integer        not null DEFAULT 0
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