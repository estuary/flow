begin;


create table data_planes (
  like internal._model including all,

  data_plane_name  catalog_name not null,
  data_plane_fqdn  text not null,

  ops_logs_name        catalog_name not null,
  ops_stats_name       catalog_name not null,

  ops_l1_inferred_name catalog_name not null,
  ops_l1_stats_name    catalog_name not null,
  ops_l2_inferred_transform    text not null,
  ops_l2_stats_transform       text not null,

  broker_address    text not null,
  reactor_address   text not null,

  config        json not null default '{}'::json,
  status        json not null default '{}'::json,
  logs_token    uuid not null default gen_random_uuid(),
  hmac_keys     text[] not null,

  unique (data_plane_name),
  unique (data_plane_fqdn)
);
alter table data_planes enable row level security;

create policy "Users must be read-authorized to data planes"
  on data_planes as permissive for select
  using (exists(
    select 1 from auth_roles('read') r where data_plane_name ^@ r.role_prefix
  ));

grant select (
  id,
  data_plane_name,
  data_plane_fqdn,
  ops_logs_name,
  ops_stats_name,
  created_at,
  updated_at,
  broker_address,
  reactor_address,
  config,
  status
)
on data_planes to authenticated;

create or replace function internal.task_roles(
  task_name_or_prefix text,
  min_capability grant_capability default 'x_00'
)
returns table (role_prefix catalog_prefix, capability grant_capability) as $$

  with recursive
  all_roles(role_prefix, capability) as (
      select g.object_role, g.capability from role_grants g
      where starts_with(task_name_or_prefix, g.subject_role)
        and g.capability >= min_capability
    union
      -- Recursive case: for each object_role granted as 'admin',
      -- project through grants where object_role acts as the subject_role.
      select g.object_role, g.capability
      from role_grants g, all_roles a
      where starts_with(a.role_prefix, g.subject_role)
        and g.capability >= min_capability
        and a.capability = 'admin'
  )
  select role_prefix, max(capability) from all_roles
  group by role_prefix
  order by role_prefix;

$$ language sql stable;



alter table discovers add column data_plane_name text not null default 'ops/dp/public/gcp-us-central1-c1';
alter table publications add column data_plane_name text not null default 'ops/dp/public/gcp-us-central1-c1';


do $$
declare
    cronut_id flowid;
begin

    insert into data_planes (
        data_plane_name,
        data_plane_fqdn,
        ops_logs_name,
        ops_stats_name,
        ops_l1_inferred_name,
        ops_l1_stats_name,
        ops_l2_inferred_transform,
        ops_l2_stats_transform,
        broker_address,
        reactor_address,
        hmac_keys,
    ) values (
        'ops/dp/public/gcp-us-central1-c1',
        'gcp-us-central1-c1.dp.estuary-data.com',
        'ops.us-central1.v1/logs',
        'ops.us-central1.v1/stats',
        'ops.us-central1.v1/inferred-schemas/L1',
        'ops.us-central1.v1/catalog-stats-L1',
        'from-ops.us-central1.v1'
        'fromOps.us-central1.v1',
        'http://localhost:8080', -- TODO(johnny): K8s service
        'http://localhost:9000', -- TODO(johnny): K8s service
        'c2VjcmV0,AA=='          -- TODO(johnny): replace with actual secret.
    );

    select id into cronut_id from data_planes where data_plane_name = 'ops/dp/public/gcp-us-central1-c1';

    alter table live_specs add column data_plane_id flowid not null default cronut_id;


end $$;


commit;