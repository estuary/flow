begin;

-- Add the `controller_task_id` column to the `live_specs` table, which will
-- allow new agent versions to publish specs and use automations for controllers
-- of the new specs, while still allowing legacy agents to run using the old
-- `controller_next_run` column and handler.
-- The explicit default is necessary in order to disable the implicit default of
-- generating a random flowid, which is defined as part of the flowid domain.
alter table public.live_specs
add column controller_task_id public.flowid references internal.tasks(task_id) default null;

comment on column public.live_specs.controller_task_id is 'The task id of the controller task that is responsible for this spec';

create unique index live_specs_controller_task_id_uindex on public.live_specs (controller_task_id);


-- Update the inferred schema trigger function to support both the new and
-- legacy controller notification mechanisms. This allows both the new and
-- legacy agents to coexist and run controllers.
create or replace function internal.on_inferred_schema_update() returns trigger
    language plpgsql security definer
    as $$
declare
    controller_task_id flowid;
begin

    select ls.controller_task_id into controller_task_id
    from public.live_specs ls
    where ls.catalog_name = new.collection_name and ls.spec_type = 'collection';
    if controller_task_id is not null then
        perform internal.send_to_task(
            controller_task_id,
            '00:00:00:00:00:00:00:00'::flowid,
            '{"type":"inferred_schema_updated"}'
        );
    else
        -- Legacy controller notification code, to be removed once the rollout is complete.
        -- The least function is necessary in order to avoid delaying a controller job in scenarios
        -- where there is a backlog of controller runs that are due.
        update live_specs set controller_next_run = least(controller_next_run, now())
        where catalog_name = new.collection_name and spec_type = 'collection';
    end if;

return null;
end;
$$;

-- The existing `live_specs_ext` view needs to be updated because it used the
-- old `controller_next_run` column. It never actually needed that column, so
-- I'm removing it rather than updating it to use `internal.tasks.wake_at`. The
-- folling eye-rolling BS is necessary because postgres disallows dropping
-- columns from a view, and also disallows deleting a view that's used by
-- another view. The _only_ change among all these views is the removal of
-- `live_specs_ext.controller_next_run`.
drop view public.unchanged_draft_specs;
drop view public.draft_specs_ext;
drop view public.live_specs_ext;

create view public.live_specs_ext as
 with authorized_specs as (
         select l_1.id
           from public.auth_roles('read'::public.grant_capability) r(role_prefix, capability),
            public.live_specs l_1
          where ((l_1.catalog_name)::text ^@ (r.role_prefix)::text)
        )
 select l.created_at,
    l.detail,
    l.id,
    l.updated_at,
    l.catalog_name,
    l.connector_image_name,
    l.connector_image_tag,
    l.last_pub_id,
    l.reads_from,
    l.spec,
    l.spec_type,
    l.writes_to,
    l.last_build_id,
    l.md5,
    l.built_spec,
    l.inferred_schema_md5,
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
    u.full_name as last_pub_user_full_name,
    l.journal_template_name,
    l.shard_template_id,
    l.data_plane_id,
    d.broker_address,
    d.data_plane_name,
    d.reactor_address
   from (((((public.live_specs l
     left join public.publication_specs p on ((((l.id)::macaddr8 = (p.live_spec_id)::macaddr8) and ((l.last_pub_id)::macaddr8 = (p.pub_id)::macaddr8))))
     left join public.connectors c on ((c.image_name = l.connector_image_name)))
     left join public.connector_tags t on ((((c.id)::macaddr8 = (t.connector_id)::macaddr8) and (l.connector_image_tag = t.image_tag))))
     left join internal.user_profiles u on ((u.user_id = p.user_id)))
     left join public.data_planes d on (((d.id)::macaddr8 = (l.data_plane_id)::macaddr8)))
  where ((exists ( select 1
           from pg_roles
          where ((pg_roles.rolname = current_role) and (pg_roles.rolbypassrls = true)))) or ((l.id)::macaddr8 in ( select authorized_specs.id
           from authorized_specs)));


CREATE VIEW public.draft_specs_ext AS
 WITH authorized_drafts AS (
         SELECT drafts.id
           FROM public.drafts
          WHERE (drafts.user_id = ( SELECT auth.uid() AS uid))
        )
 SELECT d.created_at,
    d.detail,
    d.id,
    d.updated_at,
    d.draft_id,
    d.catalog_name,
    d.expect_pub_id,
    d.spec,
    d.spec_type,
    d.built_spec,
    d.validated,
    l.last_pub_detail,
    l.last_pub_id,
    l.last_pub_user_id,
    l.last_pub_user_avatar_url,
    l.last_pub_user_email,
    l.last_pub_user_full_name,
    l.spec AS live_spec,
    l.spec_type AS live_spec_type,
    s.md5 AS inferred_schema_md5,
    l.inferred_schema_md5 AS live_inferred_schema_md5,
    l.md5 AS live_spec_md5,
    md5(TRIM(BOTH FROM (d.spec)::text)) AS draft_spec_md5
   FROM ((public.draft_specs d
     LEFT JOIN public.live_specs_ext l ON (((d.catalog_name)::text = (l.catalog_name)::text)))
     LEFT JOIN public.inferred_schemas s ON (((s.collection_name)::text = (l.catalog_name)::text)))
  WHERE ((EXISTS ( SELECT 1
           FROM pg_roles
          WHERE ((pg_roles.rolname = CURRENT_ROLE) AND (pg_roles.rolbypassrls = true)))) OR ((d.draft_id)::macaddr8 IN ( SELECT authorized_drafts.id
           FROM authorized_drafts)));

CREATE VIEW public.unchanged_draft_specs AS
 SELECT d.draft_id,
    d.catalog_name,
    d.spec_type,
    d.live_spec_md5,
    d.draft_spec_md5,
    d.inferred_schema_md5,
    d.live_inferred_schema_md5
   FROM public.draft_specs_ext d
  WHERE (d.draft_spec_md5 = d.live_spec_md5);

commit;
