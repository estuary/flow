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

commit;
