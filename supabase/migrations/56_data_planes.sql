
create table data_planes (
  like internal._model including all,

  data_plane_name  catalog_name not null,
  ops_logs_name  catalog_name not null,
  ops_stats_name catalog_name not null,

  fqdn              text not null,

  broker_address    text not null,
  reactor_address   text not null,

  config        json,
  status        json not null default '{}'::json,
  logs_token    uuid not null default gen_random_uuid(),
  error         text,
  hmac_key      text not null,

  unique (data_plane_name),
  unique (fqdn)
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
  fqdn,
  created_at,
  updated_at,
  broker_address,
  reactor_address,
  config,
  status
)
on data_planes to authenticated;


alter table discovers add column data_plane_name text not null default '';

alter table publications add column data_plane_name text not null default '';

alter table live_specs add column data_plane_id flowid not null default '00:00:00:00:00:00:00:00';