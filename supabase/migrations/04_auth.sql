-- Roles which are created by supabase:
--   create role if not exists anon;
--   create role if not exists authenticated;

-- A new supabase installation grants all in public to anon & authenticated.
-- We elect to NOT do this, instead explicitly granting access to the tables
-- and functions which uses should be able to access.
alter default privileges in schema public revoke all on tables from anon, authenticated;
alter default privileges in schema public revoke all on routines from anon, authenticated;
alter default privileges in schema public revoke all on sequences from anon, authenticated;

-- Provide API clients a way to determine their effective user_id.
create function auth_uid()
returns uuid as $$
  select auth.uid()
$$ language sql stable;
comment on function auth_uid is
  'auth_uid returns the user ID of the authenticated user';


-- Enumeration of capabilities that can be granted.
create type grant_capability as enum (
  'x_00',
  'x_01',
  'x_02',
  'x_03',
  'x_04',
  'x_05',
  'x_06',
  'x_07',
  'x_08',
  'x_09',
  'read', -- Tag: 10
  'x_11',
  'x_12',
  'x_13',
  'x_14',
  'x_15',
  'x_16',
  'x_17',
  'x_18',
  'x_19',
  'write', -- Tag: 20
  'x_21',
  'x_22',
  'x_23',
  'x_24',
  'x_25',
  'x_26',
  'x_27',
  'x_28',
  'x_29',
  'admin' -- Tag: 30
);
comment on type grant_capability is '
grant_capability is an ordered enumeration of grant capabilities
bestowed upon a grantee by a grantor. Higher enumerated values
imply all of the capabilities of lower enum values.

Enum values beginning with "x_" are placeholders for possible
future extension of the set of granted capabilities.

A "read" capability allows a user or catalog specifications to
read from collections.

A "write" capability allows a user or catalog specification to
write data into collections.

The "admin" capability allows for creating, updating, and deleting
specifications. Unlike "read" or "write", this capability also recursively
grants the bearer all capabilities of the object_role. Put differently,
a user capable of changing a catalog specification is also granted the
capabilities which that specification itself uses to read and write data.
';


-- Grants of users to roles.
create table user_grants (
  like internal._model including all,

  user_id      uuid references auth.users(id) not null,
  object_role  catalog_prefix   not null,
  capability   grant_capability not null,

  unique(user_id, object_role)
);
alter table user_grants enable row level security;

comment on table user_grants is
  'Roles and capabilities that the user has been granted';
comment on column user_grants.user_id is
  'User who has been granted a role';
comment on column user_grants.object_role is
  'Role which is granted to the user';
comment on column user_grants.capability is
  'Capability which is granted to the user';


-- Grants of roles to other roles.
create table role_grants (
  like internal._model including all,

  subject_role catalog_prefix   not null,
  object_role  catalog_prefix   not null,
  capability   grant_capability not null
);
alter table role_grants enable row level security;

-- text_pattern_ops enables the index to accelerate prefix lookups.
-- starts_with() is common when searching role_grants and we don't use
-- ordering operators ( >, >=, <) on subject or object roles.
-- See: https://www.postgresql.org/docs/current/indexes-opclass.html
create unique index idx_role_grants_sub_obj on role_grants
  (subject_role text_pattern_ops, object_role text_pattern_ops);

comment on table role_grants is
  'Roles and capabilities that roles have been granted to other roles';
comment on column role_grants.subject_role is
  'Role which has been granted a capability to another role';
comment on column role_grants.object_role is
  'Role to which a capability has been granted';
comment on column role_grants.capability is
  'Capability which is granted to the subject role';


create function internal.user_roles(target_user_id uuid)
returns table (role_prefix catalog_prefix, capability grant_capability) as $$

  with recursive
  all_roles(role_prefix, capability) as (
      select object_role, capability from user_grants
      where user_id = target_user_id
    union
      -- Recursive case: for each object_role granted as 'admin',
      -- project through grants where object_role acts as the subject_role.
      select role_grants.object_role, role_grants.capability
      from role_grants, all_roles
      where starts_with(role_grants.subject_role, all_roles.role_prefix)
        and all_roles.capability = 'admin'
  )
  select role_prefix, capability from all_roles;

$$ language sql stable security definer;


create function auth_roles()
returns table (role_prefix catalog_prefix, capability grant_capability) as $$
  select role_prefix, capability from internal.user_roles(auth_uid())
$$ language sql stable security definer;
comment on function auth_roles is
  'auth_roles returns all roles and associated capabilities of the user';


create function auth_catalog(name_or_prefix text, min_cap grant_capability)
returns boolean as $$
  select exists(
    select 1 from auth_roles() r
    where starts_with(name_or_prefix, r.role_prefix) and r.capability >= min_cap
  )
$$ language sql stable;
comment on function auth_catalog is
  'auth_catalog returns true if the user has at least `min_cap` capability to the desired catalog `name_or_prefix`';


-- Policy permissions for user_grants.
create policy "Users select user grants they admin or are the subject"
  on user_grants as permissive for select
  using (auth_catalog(object_role, 'admin') or user_id = auth.uid());
create policy "Users insert user grants they admin"
  on user_grants as permissive for insert
  with check (auth_catalog(object_role, 'admin'));
create policy "Users update user grants they admin"
  on user_grants as permissive for update
  using (auth_catalog(object_role, 'admin'));
create policy "Users delete user grants they admin or are the subject"
  on user_grants as permissive for delete
  using (auth_catalog(object_role, 'admin') or user_id = auth.uid());

grant all on user_grants to authenticated;


-- Policy permissions for role_grants.
create policy "Users select role grants they recieve or admin the object"
  on role_grants as permissive for select
  using (auth_catalog(object_role, 'admin') or auth_catalog(subject_role, 'x_00'));
create policy "Users insert role grants where they admin the object"
  on role_grants as permissive for insert
  with check (auth_catalog(object_role, 'admin'));
create policy "Users update role grants where they admin the object"
  on role_grants as permissive for update
  using (auth_catalog(object_role, 'admin'));
create policy "Users delete role grants where they admin the object or subject"
  on role_grants as permissive for delete
  using (auth_catalog(object_role, 'admin') or auth_catalog(subject_role, 'admin'));

grant all on role_grants to authenticated;
