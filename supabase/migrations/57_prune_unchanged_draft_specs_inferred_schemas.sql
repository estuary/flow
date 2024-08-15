begin;

create or replace view unchanged_draft_specs as
  select
    draft_id,
    catalog_name,
    spec_type,
    live_spec_md5,
    draft_spec_md5,
    inferred_schema_md5,
    live_inferred_schema_md5
  from draft_specs_ext d
    where draft_spec_md5 = live_spec_md5;
grant select on unchanged_draft_specs to authenticated;
comment on view unchanged_draft_specs is
  'View of `draft_specs_ext` that is filtered to only include specs that are identical to the
 current `live_specs`.';

commit;
