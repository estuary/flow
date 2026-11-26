-- Tests can't assume data_plane_releases_ci, so its grants are checked instead.

create function tests.test_replace_data_plane_targets()
returns setof text as $$
declare
  inserted integer;
begin
  insert into public.data_plane_targets (data_plane_id, role, max_tier, image, step)
  values ('00:00:00:00:00:00:00:00', 'gazette', 100, 'ghcr.io/gazette/broker:stale', 5);

  inserted = replace_data_plane_targets('[
      {"active": true, "data_plane_id": "00:00:00:00:00:00:00:00", "role": "reactor",
       "max_tier": 0, "image": "ghcr.io/estuary/reactor:v0.6.15", "step": 1},
      {"active": true, "data_plane_id": "00:00:00:00:00:00:00:00", "role": "reactor",
       "max_tier": 100, "image": "ghcr.io/estuary/reactor:v0.6.14", "step": 5}
    ]'::jsonb);

  return query select is(inserted, 2, 'replace inserts every row of the payload');

  return query select results_eq(
    $q$ select role, max_tier::integer, image from public.data_plane_targets order by max_tier $q$,
    $q$ values
      ('reactor', 0, 'ghcr.io/estuary/reactor:v0.6.15'),
      ('reactor', 100, 'ghcr.io/estuary/reactor:v0.6.14') $q$,
    'replace removes rows absent from the payload');

  return query select throws_like(
    $q$ insert into public.data_plane_targets (data_plane_id, role, max_tier, image, step)
        values ('00:00:00:00:00:00:00:00', 'brokers', 100, 'x', 5) $q$,
    '%data_plane_targets_role%', 'role must be a data-plane role');

  return query select throws_like(
    $q$ insert into public.data_plane_targets (data_plane_id, role, max_tier, image, step)
        values ('00:00:00:00:00:00:00:00', 'gazette', 100, 'x', 0) $q$,
    '%data_plane_targets_step_nonzero%', 'step must be non-zero');

  return query select throws_like(
    $q$ insert into public.data_plane_targets (data_plane_id, role, max_tier, image, step)
        values ('12:34:56:78:9a:bc:de:f0', 'gazette', 10, 'x', 5) $q$,
    '%data_plane_targets_pin_max_tier%', 'a plane-specific target must cover every tier');

  return query select lives_ok(
    $q$ insert into public.data_plane_targets (data_plane_id, role, max_tier, image, step)
        values ('12:34:56:78:9a:bc:de:f0', 'gazette', 100, 'x', 5) $q$,
    'a plane-specific target may have max_tier 100');

  return query select ok(
    has_function_privilege('data_plane_releases_ci', 'public.replace_data_plane_targets(jsonb)', 'execute'),
    'data_plane_releases_ci may replace targets');
  return query select ok(
    has_table_privilege('data_plane_releases_ci', 'public.data_plane_targets', 'insert, delete'),
    'data_plane_releases_ci may insert and delete targets');
  return query select ok(
    not has_function_privilege('anon', 'public.replace_data_plane_targets(jsonb)', 'execute')
    and not has_function_privilege('authenticated', 'public.replace_data_plane_targets(jsonb)', 'execute'),
    'API callers may not replace targets');
  return query select ok(
    not has_table_privilege('anon', 'public.data_plane_targets', 'select, insert, update, delete')
    and not has_table_privilege('authenticated', 'public.data_plane_targets', 'select, insert, update, delete'),
    'API callers may not read or write targets');
end;
$$ language plpgsql;
