-- Live specs with *deserializable* models and built specs, for tests that
-- actually load them into a `tables::LiveCatalog` (rather than only reading
-- their names or authorization). `alice.sql` deliberately stores `'{}'` specs,
-- which is enough for name-and-authorization tests but fails to deserialize.
--
-- `carol` is admin of `carolCo/`; `dan` exists with no grants at all and so
-- models an unauthorized caller.
do $$
declare
  data_plane_one_id flowid := '111111111111';

  carol_uid uuid := '33333333-3333-3333-3333-333333333333';
  dan_uid uuid := '44444444-4444-4444-4444-444444444444';

  -- A flowid's high 41 bits are milliseconds since the Estuary epoch, and
  -- authorization staleness is decided against that embedded timestamp. These
  -- ids are therefore chosen to sit a few days *after* the epoch, so that a
  -- Snapshot taken shortly before them is still comfortably after the zero id
  -- that a not-yet-published spec resolves to. Spell them out in full: a
  -- 12-hex-digit literal is widened to macaddr8 by inserting FF:FE in the
  -- middle, which would scramble the timestamp.
  collection_id flowid := '00:08:00:00:00:00:04:01';
  capture_id flowid := '00:08:00:00:00:00:04:02';
  materialization_id flowid := '00:08:00:00:00:00:04:03';
  last_pub_id flowid := '00:08:00:00:00:00:00:00';

begin

  insert into auth.users (id, email) values
    (carol_uid, 'carol@example.com'),
    (dan_uid, 'dan@example.com')
  ;
  -- Dan administers his own tenant but is granted nothing else — not even the
  -- shared data-plane — so he is unauthorized to everything under `carolCo/`.
  insert into public.user_grants (user_id, object_role, capability) values
    (carol_uid, 'carolCo/', 'admin'),
    (dan_uid, 'danCo/', 'admin')
  ;
  -- `carolCo/in/` may write to `carolCo/data/`; `carolCo/out/` is deliberately
  -- granted nothing, so a spec under it fails its own read authorization.
  insert into public.role_grants (subject_role, object_role, capability) values
    ('carolCo/in/', 'carolCo/data/', 'write'),
    ('carolCo/', 'ops/dp/public/', 'read')
  ;

  perform internal.create_task(collection_id, 1::smallint, '00:00:00:00:00:00:00:00'::flowid);
  perform internal.create_task(capture_id, 1::smallint, '00:00:00:00:00:00:00:00'::flowid);
  perform internal.create_task(materialization_id, 1::smallint, '00:00:00:00:00:00:00:00'::flowid);

  insert into public.live_specs (
    id, controller_task_id, catalog_name, last_pub_id, spec_type, spec, built_spec, data_plane_id
  ) values (
    collection_id,
    collection_id,
    'carolCo/data/foo',
    last_pub_id,
    'collection',
    '{"schema": {"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]}, "key": ["/id"]}',
    '{"name": "carolCo/data/foo", "writeSchemaJson": "{}", "key": ["/id"], "partitionTemplate": {"name": "carolCo/data/foo/gen1234"}}',
    data_plane_one_id
  ), (
    capture_id,
    capture_id,
    'carolCo/in/capture-foo',
    last_pub_id,
    'capture',
    '{"endpoint": {"connector": {"image": "source/test:test", "config": {}}}, "bindings": []}',
    '{"name": "carolCo/in/capture-foo", "shardTemplate": {"id": "capture/carolCo/in/capture-foo/gen5678"}}',
    data_plane_one_id
  ), (
    materialization_id,
    materialization_id,
    'carolCo/out/materialize-bar',
    last_pub_id,
    'materialization',
    '{"endpoint": {"connector": {"image": "materialize/test:test", "config": {}}}, "bindings": []}',
    '{"name": "carolCo/out/materialize-bar", "shardTemplate": {"id": "materialization/carolCo/out/materialize-bar/gen9012"}}',
    data_plane_one_id
  );

  -- The capture writes to the collection, which is what makes it reachable
  -- from it via `fetch_expanded_live_specs`.
  insert into public.live_spec_flows (source_id, target_id, flow_type) values
    (capture_id, collection_id, 'capture')
  ;

end
$$;
