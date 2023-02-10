-- Run this after deploying the updated agent.

begin;

drop table ops_catalog_template;
drop function internal.create_ops_publication(tenant_prefix catalog_tenant, ops_user_id uuid);
alter table live_specs add md5 text generated always as (md5(trim(spec::text))) stored;
alter table tenants add l1_stat_rollup integer not null default 0;

-- Clear out the existing catalog_stats table and materialization metadata tables. The new
-- ops-catalog materialization will create new metadata tables. The catalog_stats table should be
-- created anew by re-running its migration prior to the new ops-catalog being deployed since the
-- connector would create the table differently than we want.
drop schema catalog_stat_partitions;
drop table catalog_stats, flow_checkpoints_v1, flow_materializations_v2;

-- Create a publication that will delete all the existing per-tenant ops catalogs.
do $$
declare
    ops_user_id uuid;
    current_tenant tenants;
    new_draft_id flowid := internal.id_generator();
    publication_id flowid := internal.id_generator();
begin

    select id into strict ops_user_id from auth.users where email = 'support@estuary.dev';
    insert into drafts (id, user_id, detail) values
    (new_draft_id, ops_user_id, 're-publishing ops catalog');
    insert into publications (id, user_id, draft_id) values
    (publication_id, ops_user_id, new_draft_id);

    for current_tenant in
        select * from tenants
    loop
        insert into draft_specs (draft_id, catalog_name, spec_type, spec)
        select new_draft_id, catalog_name, null, null
        from live_specs
        where catalog_name in (
            'ops/' || current_tenant.tenant || 'catalog-stats-view',
            'ops/' || current_tenant.tenant || 'tests/catalog-stats',
            'ops/' || current_tenant.tenant || 'catalog-stats'
        );
    end loop;

end
$$ language plpgsql;

-- The live_specs_ext view must be dropped & re-created to include the new md5 column from
-- live_specs. draft_specs_ext depends on that view, so it must also be dropped & re-created to
-- allow this to happen.

drop view draft_specs_ext;
drop view live_specs_ext;

-- Below here (down to the "commit;") is copied verbatim from 10_sect_ext.sql for creating the
-- live_specs_ext & draft_specs_ext view.

-- Extended view of live catalog specifications.
create view live_specs_ext as
with user_read_access AS (
    select
        distinct role_prefix
        from internal.user_roles(auth_uid())
    where
       capability >= 'read'
)
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
where exists(
  select 1 from user_read_access r where starts_with(l.catalog_name, r.role_prefix)
)
;
-- Using `grant select` is discouraged because it allows the view to query the
-- table as the user 'postgres' which bypasses RLS policies. However in this
-- case, we are inlining the policy as a join in the query for performance
-- reasons, and the join with internal.user_roles ensures that the rows returned
-- are ones accessible by the authenticated user.
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

