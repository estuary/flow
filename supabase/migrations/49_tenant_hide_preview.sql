begin;

alter table tenants add column hide_preview boolean not null default false;

commit;
