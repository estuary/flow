
begin;

drop view combined_grants_ext;
drop view draft_specs_ext;
drop view live_specs_ext;

drop policy "Users must be authorized to their catalog tenant" on tenants;
drop policy "Users can access and change directives which they administer" on directives;
drop policy "Users must be authorized to the specification catalog prefix" on storage_mappings;
drop policy "Users must be authorized to the catalog name" on catalog_stats;
drop policy "Users must be read-authorized to the specification catalog name" on live_specs;

drop policy "Users select role grants they recieve or admin the object" on role_grants;
drop policy "Users insert role grants where they admin the object" on role_grants;
drop policy "Users update role grants where they admin the object" on role_grants;
drop policy "Users delete role grants where they admin the object or subject" on role_grants;

drop policy "Users select user grants they admin or are the subject" on user_grants;
drop policy "Users insert user grants they admin" on user_grants;
drop policy "Users update user grants they admin" on user_grants;
drop policy "Users delete user grants they admin or are the subject" on user_grants;

drop index idx_role_grants_sub_obj;
drop index idx_live_specs_catalog_name;

drop function auth_catalog(name_or_prefix text, min_cap grant_capability);
drop function auth_roles();
drop function internal.user_roles(target_user_id uuid);


alter table live_specs add constraint live_specs_catalog_name_key unique(catalog_name);
alter table role_grants add constraint role_grants_subject_role_object_role_key unique(subject_role, object_role);
create index idx_role_grants_subject_role_spgist on role_grants using spgist (subject_role);
create index idx_role_grants_object_role_spgist on role_grants using spgist (object_role);
create index idx_user_grants_object_role_spgist on user_grants using spgist (object_role);
create index idx_live_specs_catalog_name_spgist on live_specs using spgist (catalog_name);


create function internal.user_roles(
  target_user_id uuid,
  min_capability grant_capability default 'x_00'
)
returns table (role_prefix catalog_prefix, capability grant_capability) as $$

  with recursive
  all_roles(role_prefix, capability) as (
      select object_role, capability from user_grants
      where user_id = target_user_id
        and capability >= min_capability
    union
      -- Recursive case: for each object_role granted as 'admin',
      -- project through grants where object_role acts as the subject_role.
      select role_grants.object_role, role_grants.capability
      from role_grants, all_roles
      where role_grants.subject_role ^@ all_roles.role_prefix
        and role_grants.capability >= min_capability
        and all_roles.capability = 'admin'
  )
  select role_prefix, max(capability) from all_roles
  group by role_prefix
  order by role_prefix;

$$ language sql stable;


create function auth_roles(min_capability grant_capability default 'x_00')
returns table (role_prefix catalog_prefix, capability grant_capability) as $$
  select role_prefix, capability from internal.user_roles(auth_uid(), min_capability)
$$ language sql stable security definer;
comment on function auth_roles is
  'auth_roles returns all roles and associated capabilities of the user';




-- Policy permissions for user_grants.
create policy "Users select user grants they admin or are the subject"
  on user_grants as permissive for select
  using (exists(
    select 1 from auth_roles('admin') r where object_role ^@ r.role_prefix
  ) or user_id = auth.uid());
create policy "Users insert user grants they admin"
  on user_grants as permissive for insert
  with check (exists(
    select 1 from auth_roles('admin') r where object_role ^@ r.role_prefix
  ));
create policy "Users update user grants they admin"
  on user_grants as permissive for update
  using (exists(
    select 1 from auth_roles('admin') r where object_role ^@ r.role_prefix
  ));
create policy "Users delete user grants they admin or are the subject"
  on user_grants as permissive for delete
  using (exists(
    select 1 from auth_roles('admin') r where object_role ^@ r.role_prefix
  ) or user_id = auth.uid());
create policy "Users select role grants where they admin the subject or object"
  on role_grants as permissive for select
  using (exists(
    select 1 from auth_roles('admin') r
    where (object_role ^@ r.role_prefix or subject_role ^@ r.role_prefix)
  ));
create policy "Users insert role grants where they admin the object"
  on role_grants as permissive for insert
  with check (exists(
    select 1 from auth_roles('admin') r where object_role ^@ r.role_prefix
  ));
create policy "Users update role grants where they admin the object"
  on role_grants as permissive for update
  using (exists(
    select 1 from auth_roles('admin') r where object_role ^@ r.role_prefix
  ));
create policy "Users delete role grants where they admin the object or subject"
  on role_grants as permissive for delete
  using (exists(
    select 1 from auth_roles('admin') r
    where (object_role ^@ r.role_prefix or subject_role ^@ r.role_prefix)
  ));
create policy "Users must be read-authorized to the specification catalog name"
  on live_specs as permissive for select
  using (exists(
    select 1 from auth_roles('read') r where catalog_name ^@ r.role_prefix
  ));
create policy "Users must be authorized to the catalog name"
  on catalog_stats as permissive for select
  using (exists(
    select 1 from auth_roles('read') r where catalog_name ^@ r.role_prefix
  ));
create policy "Users must be authorized to the specification catalog prefix"
  on storage_mappings as permissive for select
  using (exists(
    select 1 from auth_roles('read') r where catalog_prefix ^@ r.role_prefix
  ));
create policy "Users can access and change directives which they administer"
  on directives as permissive
  using (exists(
    select 1 from auth_roles('admin') r where catalog_prefix ^@ r.role_prefix
  ));
create policy "Users must be authorized to their catalog tenant"
  on tenants as permissive for select
  using (exists(
    select 1 from auth_roles('admin') r where tenant ^@ r.role_prefix
  ));

-- Re-create views:

create view combined_grants_ext as
with admin_roles as (
  -- Extract into CTE so it's evaluated once, not twice.
  -- This is only required because of the union, which produces
  -- entirely separate evaluation nodes within the query plan
  -- that naievely don't share the auth_roles() result.
  select role_prefix from auth_roles('admin')
)
select
  g.capability,
  g.created_at,
  g.detail,
  g.id,
  g.object_role,
  g.updated_at,
  --
  g.subject_role,
  --
  null as user_avatar_url,
  null as user_email,
  null as user_full_name,
  null as user_id
from role_grants g
where g.id in (
  -- User must admin subject or object role. Compare to select RLS policy.
  select g.id from admin_roles r, role_grants g
    where g.subject_role ^@ r.role_prefix or g.object_role ^@ r.role_prefix
)
union all
select
  g.capability,
  g.created_at,
  g.detail,
  g.id,
  g.object_role,
  g.updated_at,
  --
  null as subject_role,
  --
  u.avatar_url as user_avatar_url,
  u.email as user_email,
  u.full_name as user_full_name,
  g.user_id as user_id
from user_grants g
left outer join internal.user_profiles u on u.user_id = g.user_id
where g.id in (
  -- User must admin object role or be the user. Compare to select RLS policy.
  select g.id from admin_roles r, user_grants g
  where g.user_id = auth.uid() or g.object_role ^@ r.role_prefix
)
;
-- combined_grants_ext includes its own authorization checks.
grant select on combined_grants_ext to authenticated;

comment on view combined_grants_ext is
  'Combined view of `role_grants` and `user_grants` extended with user metadata';

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