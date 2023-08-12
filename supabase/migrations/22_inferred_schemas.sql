begin;

create table inferred_schemas (
    collection_name  catalog_name not null,
    "schema"         json not null,
    flow_document    json not null,
    primary key (collection_name)
);
alter table inferred_schemas enable row level security;

create policy "Users must be authorized to the collection name"
  on inferred_schemas as permissive for select
  using (exists(
    select 1 from auth_roles('read') r where collection_name ^@ r.role_prefix
  ));
grant select on inferred_schemas to authenticated;

comment on table inferred_schemas is
    'Inferred schemas of Flow collections';
comment on column inferred_schemas.collection_name is
    'Collection which is inferred';
comment on column inferred_schemas.schema is
    'Inferred JSON schema of collection documents.';

-- stats_loader loads directly to the inferred_schemas table.
alter table inferred_schemas owner to stats_loader;

commit;