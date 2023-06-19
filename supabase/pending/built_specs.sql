begin;

alter table live_specs add column built_spec json;
comment on column live_specs.built_spec is
  'Built specification for this catalog';

alter table draft_specs add column built_spec json;
alter table draft_specs add column validated json;
comment on column draft_specs.built_spec is
  'Built specification for this catalog';
comment on column draft_specs.validated is
  'Serialized response from the connector Validate RPC as populated by a dry run of this draft specification';

-- The live_specs_ext and draft_specs_ext views must be dropped & re-created to include their new
-- columns (built_spec for live_specs_ext; built_spec & validated for draft_specs_ext) since these
-- columns are included in the views from their "base" table through the "*" part of select l.*/d.*

drop view draft_specs_ext;
drop view live_specs_ext;

-- Below here (down to the "commit;") is copied verbatim from 10_spec_ext.sql for creating the
-- live_specs_ext & draft_specs_ext view.

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
where l.id in (
  -- User must admin catalog_name. Compare to select RLS policy.
  select l.id from auth_roles('read') r, live_specs l
    where l.catalog_name ^@ r.role_prefix
)
;
-- live_specs_ext includes its own authorization checks.
grant select on live_specs_ext to authenticated;

comment on view live_specs_ext is
  'View of `live_specs` extended with metadata of its last publication';

-- Extended view of user draft specifications.
create view draft_specs_ext as
select
  d.*,
  l.last_pub_detail,
  l.last_pub_id,
  l.last_pub_user_id,
  l.last_pub_user_avatar_url,
  l.last_pub_user_email,
  l.last_pub_user_full_name,
  l.spec as live_spec,
  l.spec_type as live_spec_type
from draft_specs d
left outer join live_specs_ext l
  on d.catalog_name = l.catalog_name
;
alter view draft_specs_ext owner to authenticated;

comment on view draft_specs_ext is
  'View of `draft_specs` extended with metadata of its live specification';

commit;
