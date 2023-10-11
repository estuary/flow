begin;

alter table tenants add column "pays_externally" boolean default false;

commit;