begin;

-- The image each role should run, per band of tiers. A matching
-- data_plane_releases row wins, to route through an intermediate image.
create table public.data_plane_targets (
    created_at timestamp with time zone default now() not null,
    active boolean default true not null,
    data_plane_id public.flowid not null,
    role text not null,
    max_tier smallint not null,
    image text not null,
    step integer not null,

    constraint data_plane_targets_pkey primary key (active, data_plane_id, role, max_tier),
    constraint data_plane_targets_role check (role in ('etcd', 'gazette', 'reactor', 'bastion', 'dekaf')),
    constraint data_plane_targets_max_tier_range check (max_tier >= 0 and max_tier <= 100),
    constraint data_plane_targets_step_nonzero check (step <> 0),
    -- A pin with a lower max_tier would silently not apply to higher tiers.
    constraint data_plane_targets_pin_max_tier check (
        data_plane_id = '00:00:00:00:00:00:00:00' or max_tier = 100
    )
);

comment on table public.data_plane_targets is
  'Images which deployments of data planes should run, by role and tier band';
comment on column public.data_plane_targets.active is
  'Inactive targets are kept only as a record';
comment on column public.data_plane_targets.data_plane_id is
  'Data plane this target pins, with max_tier 100, or 00:00:00:00:00:00:00:00 for all. Pins win';
comment on column public.data_plane_targets.max_tier is
  'Applies to deployments with tier <= max_tier. The smallest applicable max_tier wins';
comment on column public.data_plane_targets.step is
  'Rollout step: positive surges, negative replaces';

-- Supabase's default privileges grant new tables and functions to API roles.
revoke all on table public.data_plane_targets from anon, authenticated;
grant select, insert, delete, update on table public.data_plane_targets to data_plane_releases_ci;

-- As replace_data_plane_releases. `where true` satisfies sql_safe_updates,
-- which PostgREST enables.
create function public.replace_data_plane_targets(payload jsonb) returns integer
    language plpgsql
    as $$
declare
  inserted integer;
begin
  delete from data_plane_targets where true;

  insert into data_plane_targets (active, data_plane_id, role, max_tier, image, step)
    select
      (e->>'active')::boolean,
      (e->>'data_plane_id')::public.flowid,
      e->>'role',
      (e->>'max_tier')::smallint,
      e->>'image',
      (e->>'step')::integer
    from jsonb_array_elements(payload) as e;

  get diagnostics inserted = row_count;
  return inserted;
end
$$;

comment on function public.replace_data_plane_targets(jsonb) is
  'Atomically replace all data_plane_targets rows from a JSON array.';

revoke execute on function public.replace_data_plane_targets(jsonb) from public, anon, authenticated;
grant execute on function public.replace_data_plane_targets(jsonb) to data_plane_releases_ci;

commit;
