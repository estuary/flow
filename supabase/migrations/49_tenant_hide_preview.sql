begin;

alter table tenants add column hide_preview boolean not null default false;

comment on column tenants.hide_preview is '
Hide data preview in the collections page for this tenant, used as a measure for preventing users with access to this tenant from viewing sensitive data in collections
';

commit;
