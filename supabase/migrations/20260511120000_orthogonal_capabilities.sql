begin;

create type orthogonal_capability as enum (
    'read',
    'write',
    'admin',
    'billing',
    'team_admin',
    'delegate',
    'assume'
);

alter table user_grants
    add column capabilities orthogonal_capability[] not null default '{}';

alter table role_grants
    add column capabilities orthogonal_capability[] not null default '{}';

-- Revoke broad table-level grants and re-add column-level grants that
-- exclude `capabilities`. Only service_role (the control plane) may
-- read or write the new column; PostgREST-facing roles must not.

revoke all on role_grants from authenticated, marketplace_integration;
revoke all on role_grants from reporting_user;

grant select (id, created_at, updated_at, detail, subject_role, object_role, capability),
      insert (detail, subject_role, object_role, capability),
      update (detail, subject_role, object_role, capability),
      delete
  on role_grants to authenticated, marketplace_integration;

grant select (id, created_at, updated_at, detail, subject_role, object_role, capability)
  on role_grants to reporting_user;

revoke all on user_grants from authenticated;
revoke all on user_grants from reporting_user;

grant select (id, created_at, updated_at, detail, user_id, object_role, capability),
      insert (detail, user_id, object_role, capability),
      update (detail, user_id, object_role, capability),
      delete
  on user_grants to authenticated;

grant select (id, created_at, updated_at, detail, user_id, object_role, capability)
  on user_grants to reporting_user;

commit;
