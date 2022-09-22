/*
This source contains extended metadata views for a number of tables.
We adopt a convention of always naming these tables `${table}_ext`,
and including all columns of the base table with their original names.

IMPORTANT: Do NOT "grant select" on views. Instead change the owner to `authenticated`.
Reason: when created, views are owned by the "postgres" user, to which
row-level security policies don't apply. "grant select" then grants to
`authenticated` the same access that "postgres" has. Changing the owner to
`authenticated` causes the view to be evaluated under their RLS policies.
*/

create type user_profile as (
  user_id    uuid,
  email      text,
  full_name  text,
  avatar_url text
);

-- Provide API clients a way to map a User ID to a user profile.
-- `bearer_user_id` is a UUID ID of the auth.users table and is treated as a bearer token:
-- Anyone able to identify a UUID is able to retrieve their profile.
create function view_user_profile(bearer_user_id uuid)
returns user_profile as $$
  select
    id as user_id,
    email,
    coalesce(raw_user_meta_data->>'full_name', raw_user_meta_data->>'name') as full_name,
    coalesce(raw_user_meta_data->>'picture', raw_user_meta_data->>'avatar_url') as avatar_url
  from auth.users where id = bearer_user_id;
$$ language sql stable security definer;

comment on function view_user_profile is
  'view_user_profile returns the profile of the given user ID';


-- Extended view of combined `user_grants` and `role_grants`.
create view combined_grants_ext as
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
from user_grants g,
lateral view_user_profile(g.user_id) u
;
alter view combined_grants_ext owner to authenticated;

comment on view combined_grants_ext is
  'Combined view of `role_grants` and `user_grants` extended with user metadata';


-- Extended view of live catalog specifications.
create view live_specs_ext as
select
  l.*,
  c.external_url as connector_external_url,
  c.id as connector_id,
  c.open_graph as connector_open_graph, -- To be removed
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
left outer join connector_tags t on c.id = t.connector_id and l.connector_image_tag = t.image_tag,
lateral view_user_profile(p.user_id) u
;
alter view live_specs_ext owner to authenticated;

comment on view live_specs_ext is
  'View of `live_specs` extended with metadata of its last publication';


-- Extended view of specification publication history.
create view publication_specs_ext as
select
  p.*,
  l.catalog_name,
  l.last_pub_id,
  u.email as user_email,
  u.full_name as user_full_name,
  u.avatar_url as user_avatar_url
from publication_specs p
join live_specs l on p.live_spec_id = l.id,
lateral view_user_profile(p.user_id) u
;
alter view publication_specs_ext owner to authenticated;

comment on view publication_specs_ext is
  'View of `publication_specs` extended with metadata of its user';


-- Extended view of drafts.
create view drafts_ext as
select
  d.*,
  s.num_specs
from drafts d,
lateral (select count(*) num_specs from draft_specs where draft_id = d.id) s
;
alter view drafts_ext owner to authenticated;

comment on view drafts_ext is
  'View of `drafts` extended with metadata of its specifications';


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