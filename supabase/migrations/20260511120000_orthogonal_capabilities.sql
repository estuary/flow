begin;

-- Rename the first placeholder value to 'none', used for grants whose
-- authorization comes entirely from the bundles array column.
alter type grant_capability rename value 'x_00' to 'none';

-- Relax check constraints to allow the new 'none' capability value.
alter table role_grants drop constraint valid_capability;
alter table role_grants add constraint valid_capability check (
    capability = any (array[
        'none'::grant_capability,
        'read'::grant_capability,
        'write'::grant_capability,
        'admin'::grant_capability
    ])
);

alter table user_grants drop constraint valid_capability;
alter table user_grants add constraint valid_capability check (
    capability = any (array[
        'none'::grant_capability,
        'read'::grant_capability,
        'write'::grant_capability,
        'admin'::grant_capability
    ])
);

create type capability_bundle as enum (
    'viewer',
    'writer',
    'editor',
    'admin',
    'billing',
    'team_admin',
    'delegate',
    'assume'
);

alter table user_grants
    add column bundles capability_bundle[] not null default '{}';

alter table role_grants
    add column bundles capability_bundle[] not null default '{}';

-- Revoke broad table-level grants and re-add column-level grants that
-- exclude `bundles`. Only service_role (the control plane) may
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
