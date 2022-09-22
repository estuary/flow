
create function tests.test_task_stats()
returns setof text as $$
begin

  insert into user_grants (user_id, object_role, capability) values
    ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin'),
    ('22222222-2222-2222-2222-222222222222', 'bobCo/', 'admin')
  ;

  -- For each tenant, we explicitly create a partition of `task_stats` for them,
  -- which is then owned by the stats_loader role.
  create table task_stat_partitions.alice_stats partition of task_stats for values in ('aliceCo/');
  create table task_stat_partitions.bob_stats   partition of task_stats for values in ('bobCo/');
  create table task_stat_partitions.carol_stats partition of task_stats for values in ('carolCo/');

  alter table task_stat_partitions.alice_stats owner to stats_loader;
  alter table task_stat_partitions.bob_stats   owner to stats_loader;
  alter table task_stat_partitions.carol_stats owner to stats_loader;

  -- The `stats_loader` user materializes directly into tentant partitions.
  set role stats_loader;
  insert into task_stat_partitions.alice_stats (hourstamp, shard_split, task_name, task_type, flow_document) values
    ('2022-08-29T12:00:00Z', '00000000:00000000', 'aliceCo/hello', 'capture', '{"alice":1}');
  insert into task_stat_partitions.bob_stats (hourstamp, shard_split, task_name, task_type, flow_document) values
    ('2022-08-29T13:00:00Z', '00000000:aabbccdd', 'bobCo/world', 'derivation', '{"bob":1}');
  set role postgres;

  -- We can also load through `task_stats` which will route records appropriatey.
  insert into task_stats (hourstamp, shard_split, task_name, task_type, flow_document) values
    ('2022-08-29T13:00:00Z', '00000000:aabbccdd', 'carolCo/foobar', 'materialization', '{"carol":1}');

  return query select results_eq(
    $i$ select task_name::text, shard_split::text, flow_document::text from task_stat_partitions.alice_stats $i$,
    $i$ values ('aliceCo/hello','00:00:00:00:00:00:00:00','{"alice":1}') $i$,
    'alice stats are in alice partition'
  );
  return query select results_eq(
    $i$ select task_name::text, shard_split::text, flow_document::text from task_stat_partitions.bob_stats $i$,
    $i$ values ('bobCo/world','00:00:00:00:aa:bb:cc:dd','{"bob":1}') $i$,
    'bob stats are in bob partition'
  );
  return query select results_eq(
    $i$ select task_name::text, shard_split::text, flow_document::text from task_stat_partitions.carol_stats $i$,
    $i$ values ('carolCo/foobar','00:00:00:00:aa:bb:cc:dd','{"carol":1}') $i$,
    'carol stats are in carol partition'
  );

  return query select throws_like(
    $i$
    insert into task_stats (hourstamp, shard_split, task_name, task_type, flow_document)
      values ('2022-08-29T12:00:00Z', '00000000:00000000', 'frankCo/whoops', 'capture', '{"frank":1}');
    $i$,
    'no partition of relation "task_stats" found for row',
    'you cannot insert a stat into task_stats without a matching partition table'
  );
  return query select throws_like(
    $i$
    insert into task_stat_partitions.bob_stats (hourstamp, shard_split, task_name, task_type, flow_document)
      values ('2022-08-30T12:00:00Z', '00000000:00000000', 'aliceCo/hello', 'capture', '{"alice":2}');
    $i$,
    'new row for relation "bob_stats" violates partition constraint',
    'you cannot insert an alice stat into the bob stats partition'
  );

  -- Drop priviledge to `authenticated` and authorize as Alice.
  set role authenticated;
  set request.jwt.claim.sub to '11111111-1111-1111-1111-111111111111';

  return query select results_eq(
    $i$ select task_name::text, shard_split::text, flow_document::text from task_stats $i$,
    $i$ values ('aliceCo/hello','00:00:00:00:00:00:00:00','{"alice":1}') $i$,
    'alice can read alice stats only'
  );

end;
$$ language plpgsql;