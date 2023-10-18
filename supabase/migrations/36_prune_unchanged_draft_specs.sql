
begin;

alter table inferred_schemas add column "md5" text generated always as (md5(trim("schema"::text))) stored;
comment on column inferred_schemas.md5 is
  'hash of the inferred schema json, which is used to identify changes';

alter table live_specs add column inferred_schema_md5 text;
comment on column live_specs.inferred_schema_md5 is
  'The md5 sum of the inferred schema that was published with this spec';

-- Re-define the live_specs_ext view to include the inferred_schema_md5 column,
-- which will be needed by the unchanged_draft_specs view. Drop and re-create
-- the views because selecting * changes the order of some columns.
drop view draft_specs_ext;
drop view live_specs_ext;

-- l.* expands to an additional column now, but columns are otherwise identical to the previous view definition
-- Extended view of live catalog specifications.
create view live_specs_ext as
select
  l.*,
  c.external_url as connector_external_url,
  c.id as connector_id,
  c.title as connector_title,
  c.short_description as connector_short_description,
  c.logo_url as connector_logo_url,
  c.recommended as connector_recommended,
  t.id as connector_tag_id,
  t.documentation_url as connector_tag_documentation_url,
  p.detail as last_pub_detail,
  p.user_id as last_pub_user_id,
  u.avatar_url as last_pub_user_avatar_url,
  u.email as last_pub_user_email,
  u.full_name as last_pub_user_full_name
from live_specs l
left outer join publication_specs p on l.id = p.live_spec_id and l.last_pub_id = p.pub_id
left outer join connectors c on c.image_name = l.connector_image_name
left outer join connector_tags t on c.id = t.connector_id and l.connector_image_tag = t.image_tag
left outer join internal.user_profiles u on u.user_id = p.user_id
-- This first condition allows superusers to query the view. The second is the normal RLS policy,
-- but implemented here in a way that is more efficient when querying for large sets of specs.
where exists(select 1 from pg_roles where rolname = current_role and rolbypassrls = true)
  or l.id in (
    -- User must be able to read catalog_name. Compare to select RLS policy.
    select l.id from auth_roles('read') r, live_specs l
      where l.catalog_name ^@ r.role_prefix
  )
;
-- live_specs_ext includes its own authorization checks.
grant select on live_specs_ext to authenticated;

comment on view live_specs_ext is
  'View of `live_specs` extended with metadata of its last publication';

-- Extended view of user draft specifications.
create view draft_specs_ext  as
select
  d.*,
  l.last_pub_detail,
  l.last_pub_id,
  l.last_pub_user_id,
  l.last_pub_user_avatar_url,
  l.last_pub_user_email,
  l.last_pub_user_full_name,
  l.spec as live_spec,
  l.spec_type as live_spec_type,
  -- new columns below
  s.md5 as inferred_schema_md5,
  l.inferred_schema_md5 as live_inferred_schema_md5,
  l.md5 as live_spec_md5,
  md5(trim(d.spec::text)) as draft_spec_md5
from draft_specs d
left outer join live_specs_ext l
  on d.catalog_name = l.catalog_name
left outer join inferred_schemas s
  on s.collection_name = l.catalog_name
where exists(select 1 from pg_roles where rolname = current_role and rolbypassrls = true)
  or d.draft_id in (select id from drafts where user_id = auth.uid())
;
grant select on draft_specs_ext to authenticated;

comment on view draft_specs_ext is
  'View of `draft_specs` extended with metadata of its live specification';

create view unchanged_draft_specs as
  select
    draft_id,
    catalog_name,
    spec_type,
    live_spec_md5,
    draft_spec_md5,
    inferred_schema_md5,
    live_inferred_schema_md5
  from draft_specs_ext d
    where draft_spec_md5 = live_spec_md5
    and (
      -- either it's not a collection or it doesn't use the inferred schema
      (spec_type != 'collection' or spec::text not like '%flow://inferred-schema%')
      -- or the inferred schema hasn't changed since the last publication
      or inferred_schema_md5 is not distinct from live_inferred_schema_md5
    );
grant select on unchanged_draft_specs to authenticated;
comment on view unchanged_draft_specs is
  'View of `draft_specs_ext` that is filtered to only include specs that are identical to the
 current `live_specs`. For collection specs that use schema inference, this will only include
 them if the `inferred_schema_md5` matches the `live_inferred_schema_md5`';

create function prune_unchanged_draft_specs(prune_draft_id flowid)
returns table(
  catalog_name catalog_name,
  spec_type catalog_spec_type,
  live_spec_md5 text,
  draft_spec_md5 text,
  inferred_schema_md5 text,
  live_inferred_schema_md5 text
) as $$
  with to_prune as (
    select * from unchanged_draft_specs u where u.draft_id = prune_draft_id
  ),
  del as (
    delete from draft_specs ds
      where ds.draft_id = prune_draft_id
        and ds.catalog_name in (select catalog_name from to_prune)
  )
  select
    catalog_name,
    spec_type,
    live_spec_md5,
    draft_spec_md5,
    inferred_schema_md5,
    live_inferred_schema_md5
  from to_prune
$$ language sql security invoker;

comment on function prune_unchanged_draft_specs is
  'Deletes draft_specs belonging to the given draft_id that are identical
 to the published live_specs. For collection specs that use inferred schemas,
 draft_specs will only be deleted if the inferred schema also remains identical.';

commit;
