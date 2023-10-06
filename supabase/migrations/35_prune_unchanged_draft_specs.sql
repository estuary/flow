
begin;

alter table inferred_schemas add column "md5" text generated always as (md5(trim("schema"::text))) stored;

alter table live_specs add column inferred_schema_md5 text;

comment on column inferred_schemas.md5 is
  'hash of the inferred schema json, which is used to identify changes';
comment on column live_specs.inferred_schema_md5 is
  'The md5 sum of the inferred schema that was published with this spec';

create function prune_unchanged_draft_specs(draft_id flowid)
returns setof text as $$
  delete from draft_specs ds
    where ds.draft_id = draft_id
      and md5(trim(ds.spec::text)) = (
        select ls.md5 from live_specs ls where ls.catalog_name = ds.catalog_name
      )
      and (
        -- either it's not a collection or it doesn't use the inferred schema
        (ds.spec_type != 'collection' or ds.spec::text not like '%flow://inferred-schema%')
          -- or the inferred schema hasn't changed since the last publication
          or (
            select md5 from inferred_schemas i
              where i.collection_name = ds.catalog_name
          ) is not distinct from (
            select inferred_schema_md5
              from live_specs where catalog_name = ds.catalog_name
          )
      )
      returning ds.catalog_name
$$ language sql security invoker;

comment on function prune_unchanged_draft_specs is
  'Deletes draft_specs belonging to the given draft_id that are identical
 to the published live_specs. For collection specs that use inferred schemas,
 draft_specs will only be deleted if the inferred schema also remains identical.';

commit;
