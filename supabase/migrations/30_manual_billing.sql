begin;

create table manual_bills (
  tenant       catalog_tenant not null references tenants(tenant),
  usd_cents    integer        not null,
  description  text           not null,
  date_start   date           not null,
  date_end     date           not null
);

alter table manual_bills enable row level security;

create policy "Users must be authorized to their catalog tenant"
  on manual_bills as permissive for select
  using (exists(
    select 1 from auth_roles('admin') r where tenant ^@ r.role_prefix
  ));
grant select on manual_bills to authenticated;

comment on table manual_bills is
  'Manually entered bills that span an arbitrary date range';

commit;