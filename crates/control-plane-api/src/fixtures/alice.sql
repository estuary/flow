do $$
declare
  data_plane_one_id flowid := '111111111111';
  data_plane_two_id flowid := '222222222222';

  alice_uid uuid := '11111111-1111-1111-1111-111111111111';
  data_foo_id flowid :=  '000000000001';
  last_pub_id flowid := '000000000002';
  ops_logs_id flowid := '000000000003';
  ops_stats_id flowid := '000000000004';
  capture_foo_id flowid := '000000000005';
  materialize_bar_id flowid := '000000000006';

begin

  insert into auth.users (id, email) values
    (alice_uid, 'alice@example.com')
  ;
  insert into public.user_grants (user_id, object_role, capability) values
    (alice_uid, 'aliceCo/', 'admin')
  ;
  insert into public.role_grants (subject_role, object_role, capability) values
    ('aliceCo/in/', 'aliceCo/data/', 'write'),
    ('aliceCo/out/', 'aliceCo/data/', 'read'),
    ('aliceCo/', 'ops/dp/public/', 'read')
  ;

  perform internal.create_task(data_foo_id, 1::smallint, '000000000000'::flowid);
  perform internal.create_task(ops_logs_id, 1::smallint, '000000000000'::flowid);
  perform internal.create_task(ops_stats_id, 1::smallint, '000000000000'::flowid);
  perform internal.create_task(capture_foo_id, 1::smallint, '000000000000'::flowid);
  perform internal.create_task(materialize_bar_id, 1::smallint, '000000000000'::flowid);

  insert into public.live_specs (id, controller_task_id, catalog_name, last_pub_id, spec_type, built_spec, data_plane_id) values
    (data_foo_id, data_foo_id, 'aliceCo/data/foo', last_pub_id, 'collection', '{"partitionTemplate":{"name":"aliceCo/data/foo/gen1234"}}', data_plane_one_id),
    (ops_logs_id, ops_logs_id, 'ops/tasks/public/one/logs', last_pub_id, 'collection', '{"partitionTemplate":{"name":"ops/tasks/public/one/logs/gen1234"}}', data_plane_one_id),
    (ops_stats_id, ops_stats_id, 'ops/tasks/public/one/stats', last_pub_id, 'collection', '{"partitionTemplate":{"name":"ops/tasks/public/one/stats/gen1234"}}', data_plane_one_id),
    (capture_foo_id, capture_foo_id, 'aliceCo/in/capture-foo', last_pub_id, 'capture', '{"shardTemplate":{"id":"capture/aliceCo/in/capture-foo/gen5678"}}', data_plane_one_id),
    (materialize_bar_id, materialize_bar_id, 'aliceCo/out/materialize-bar', last_pub_id, 'materialization', '{"shardTemplate":{"id":"materialization/aliceCo/out/materialize-bar/gen9012"}}', data_plane_one_id)
  ;

end
$$;
