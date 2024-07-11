begin;

-- Updates RLS policies to use `(select auth.uid())` as recommended by:
-- https://supabase.com/docs/guides/database/postgres/row-level-security#call-functions-with-select
alter policy "Users can access only their created drafts"
  on drafts
  using (user_id = (select auth.uid()));


alter policy "Users select user grants they admin or are the subject"
  on user_grants
  using (exists(
    select 1 from auth_roles('admin') r where object_role ^@ r.role_prefix
  ) or user_id = (select auth.uid()));

alter policy "Users delete user grants they admin or are the subject"
  on user_grants
  using (exists(
    select 1 from auth_roles('admin') r where object_role ^@ r.role_prefix
  ) or user_id = (select auth.uid()));

alter policy "Users can access only their initiated publish operations"
  on publications
  using (user_id = (select auth.uid()));

alter policy "Users can access only their initiated evolution operations"
  on evolutions
  using (user_id = (select auth.uid()));

alter policy "Users can access only their applied directives"
  on applied_directives
  using (user_id = (select auth.uid()));

alter policy "Users can access their own refresh tokens"
  on refresh_tokens
  using (user_id = (select auth.uid()));

-- Althought the `user_id` condition here is not necessary for correctness,
-- making it explicit in the query helps the query planner
alter policy "Users can access and delete errors of their drafts"
  on draft_errors
  using (draft_id in (select id from drafts where user_id = (select auth.uid()) ));

alter policy "Users access their draft specs"
  on draft_specs
  using (draft_id in (select id from drafts where user_id = (select auth.uid()) ));

-- Changing the return type of the id_generator function was recommended by supabase support,
-- as a way of mitigating issues with pg-dump|restore due to recursion in the schema.
alter domain flowid drop default;
drop function internal.id_generator();

create function internal.id_generator()
  returns macaddr8 as $$
  declare
      -- This procedure generates unique 64-bit integers
      -- with the following bit layout:
      --
      --   0b00000010100000101011010111111000100000101010100100011111100011100
      --     |--         Timestamp Millis           --||-- SeqNo --||- Shard-|
      --
      -- Estuary epoch is the first representable timestamp in generated IDs.
      -- This could be zero, but subtracting |estuary_epoch| results in the
      -- high bit being zero for the next ~34 years,
      -- making ID representations equivalent for both signed and
      -- unsigned 64-bit integers.
      estuary_epoch bigint := 1600000000;
      -- The id of this parallizable ID generation shard.
      -- ID's generated inside of PostgreSQL always use |shard_id| zero.
      -- We reserve other shard IDs for future parallized ID generation.
      -- The allowed range is [0, 1024) (10 bits).
      shard_id int := 0;
      -- Sequence number is a monotonic tie-breaker for IDs generated
      -- within the same millisecond.
      -- The allowed range is [0, 8192) (13 bits).
      seq_no bigint;
      -- Current timestamp, as Unix millis since |estuary_epoch|.
      now_millis bigint;
  begin
      -- We have 13 low bits of sequence ID, which allow us to generate
      -- up to 8,192 unique IDs within each given millisecond.
      select nextval('internal.shard_0_id_sequence') % 8192 into seq_no;

      select floor((extract(epoch from clock_timestamp()) - estuary_epoch) * 1000) into now_millis;
      return lpad(to_hex((now_millis << 23) | (seq_no << 10) | (shard_id)), 16, '0')::macaddr8;
  end;
  $$ language plpgsql
  security definer;

  comment on function internal.id_generator is '
  id_generator produces 64bit unique, non-sequential identifiers. They:
    * Have fixed storage that''s 1/2 the size of a UUID.
    * Have a monotonic generation order.
    * Embed a wall-clock timestamp than can be extracted if needed.
    * Avoid the leaky-ness of SERIAL id''s.

  Adapted from: https://rob.conery.io/2014/05/29/a-better-id-generator-for-postgresql/
  Which itself was inspired by http://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram
  ';

  alter domain flowid set default internal.id_generator();

-- Create indexes to help with dequeuing pending publications, discovers, and evolutions
create index publications_queued on publications(id) where job_status->>'type' = 'queued';
create index discovers_queued on discovers(id) where job_status->>'type' = 'queued';
create index evolutions_queued on evolutions(id) where job_status->>'type' = 'queued';


-- update combined_grants_ext to use `(select auth.uid())` just like in the RLS policies
create or replace view combined_grants_ext as
  with admin_roles as (
    -- Extract into CTE so it's evaluated once, not twice.
    -- This is only required because of the union, which produces
    -- entirely separate evaluation nodes within the query plan
    -- that naievely don't share the auth_roles() result.
    select role_prefix from auth_roles('admin')
  ),
  user_id(id) as (
    -- Also to ensure that it's evaluated once instead of for each row
    select auth.uid()
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
    where g.user_id = (select id from user_id) or g.object_role ^@ r.role_prefix
  )
  ;
  -- combined_grants_ext includes its own authorization checks.
  grant select on combined_grants_ext to authenticated;

comment on view combined_grants_ext is
  'Combined view of `role_grants` and `user_grants` extended with user metadata';


-- Re-define the live_specs_ext view to hoist the `auth_roles` call into a CTE so that it gets
-- evaluated only once. Drop and re-create the views because `select *` resolves to different
-- columns. This means that we also need to drop and re-create all dependent views :/
drop view unchanged_draft_specs;
drop view draft_specs_ext;
drop view live_specs_ext;

-- l.* expands to an additional column now, but columns are otherwise identical to the previous view definition
-- Extended view of live catalog specifications.
create view live_specs_ext as
with authorized_specs as (
    -- User must be able to read catalog_name. Compare to select RLS policy.
    select l.id from auth_roles('read') r, live_specs l
    where l.catalog_name ^@ r.role_prefix
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
-- This first condition allows superusers to query the view. The second is the normal RLS policy,
-- but implemented here in a way that is more efficient when querying for large sets of specs.
where exists(select 1 from pg_roles where rolname = current_role and rolbypassrls = true)
  or l.id in (select id from authorized_specs)
;
-- live_specs_ext includes its own authorization checks.
grant select on live_specs_ext to authenticated;

comment on view live_specs_ext is
  'View of `live_specs` extended with metadata of its last publication';

-- Extended view of user draft specifications.
create view draft_specs_ext  as
with authorized_drafts as (
  select id from drafts where user_id = (select auth.uid())
)
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
  or d.draft_id in (select id from authorized_drafts)
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

commit;
