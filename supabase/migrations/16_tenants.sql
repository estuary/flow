
create table tenants (
  like internal._model including all,

  -- TODO(johnny): In the future, we expect to hang billing
  -- and data-plane assignment onto this record.
  tenant                  catalog_tenant unique not null,

  tasks_quota             integer        not null default 10,
  collections_quota       integer        not null default 100
);
alter table tenants enable row level security;

create policy "Users must be authorized to their catalog tenant"
  on tenants as permissive for select
  using (exists(
    select 1 from auth_roles('admin') r where tenant ^@ r.role_prefix
  ));
grant select on tenants to authenticated;

comment on table tenants is '
A tenant is the top-level unit of organization in the Flow catalog namespace.
';
comment on column tenants.tenant is
  'Catalog tenant identified by this record';


create table internal.illegal_tenant_names (
  name catalog_tenant unique not null primary key
);

comment on table internal.illegal_tenant_names is
  'Illegal tenant names which are not allowed to be provisioned by users';

create function internal.update_support_role() returns trigger as $trigger$
begin
  insert into role_grants (
    detail,
    subject_role,
    object_role,
    capability
  )
  select
    'Automagically grant support role access to new tenant',
    'estuary_support/',
    tenants.tenant,
    'admin'
  from tenants
  left join role_grants on
    role_grants.object_role = tenants.tenant and
    role_grants.subject_role = 'estuary_support/'
  where role_grants.id is null and
  tenants.tenant not in ('ops/', 'estuary/');

  return null;
END;
$trigger$ LANGUAGE plpgsql;

create trigger "Grant support role access to tenants"
after insert on tenants
for each statement execute function internal.update_support_role();