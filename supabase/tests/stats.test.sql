/*
create function tests.test_catalog_stats()
returns setof text as $$
begin

  insert into user_grants (user_id, object_role, capability) values
    ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin'),
    ('22222222-2222-2222-2222-222222222222', 'bobCo/', 'admin')
  ;

  -- For each tenant, we explicitly create a partition of `catalog_stats` for them.
  create table catalog_stat_partitions.alice_stats partition of catalog_stats for values in ('aliceCo/');
  create table catalog_stat_partitions.bob_stats   partition of catalog_stats for values in ('bobCo/');
  create table catalog_stat_partitions.carol_stats partition of catalog_stats for values in ('carolCo/');

  -- The `stats_loader` user materializes directly into the catalog_stats table.
  set role stats_loader;
  insert into catalog_stats (
    catalog_name, grain, ts, flow_document,
    bytes_written_by_me, docs_written_by_me,
    bytes_read_by_me, docs_read_by_me,
    bytes_written_to_me, docs_written_to_me,
    bytes_read_from_me, docs_read_from_me
  ) values
    (
      'aliceCo/hello', 'hourly', '2022-08-29T13:00:00Z', '{"alice":1}',
      10, 2,
      0, 0,
      5, 1,
      0, 0
    ),
    (
      'bobCo/world', 'daily', '2022-08-29T00:00:00Z', '{"bob":1}',
      0, 0,
      20, 3,
      0, 0,
      10, 2
    );

  -- Cannot insert tenant stats into catalog_stats if no tenant partition exists.
  return query select throws_like(
    $i$
    insert into catalog_stats (
      catalog_name, grain, ts, flow_document,
      bytes_written_by_me, docs_written_by_me,
      bytes_read_by_me, docs_read_by_me,
      bytes_written_to_me, docs_written_to_me,
      bytes_read_from_me, docs_read_from_me
    ) values
      (
        'frankCo/whoops', 'monthly', '2022-08-01T00:00:00Z', '{"frank":1}',
        0, 0, 0, 0, 0, 0, 0, 0
    );
    $i$,
    'no partition of relation "catalog_stats" found for row',
    'you cannot insert a stat into catalog_stats without a matching partition table'
  );

  -- Stats added to catalog_stats are accessible from their partitions.
  set role postgres;
  return query select results_eq(
    $i$ select catalog_name::text, grain::text, flow_document::text from catalog_stat_partitions.alice_stats $i$,
    $i$ values ('aliceCo/hello','hourly','{"alice":1}') $i$,
    'alice stats are in alice partition'
  );
  return query select results_eq(
    $i$ select catalog_name::text, grain::text, flow_document::text from catalog_stat_partitions.bob_stats $i$,
    $i$ values ('bobCo/world','daily','{"bob":1}') $i$,
    'bob stats are in bob partition'
  );

  -- Cannot insert tenant stats into an incorrect partition.
  return query select throws_like(
    $i$
    insert into catalog_stat_partitions.bob_stats (
      catalog_name, grain, ts, flow_document,
      bytes_written_by_me, docs_written_by_me,
      bytes_read_by_me, docs_read_by_me,
      bytes_written_to_me, docs_written_to_me,
      bytes_read_from_me, docs_read_from_me
    ) values
      (
        'aliceCo/hello', 'monthly', '2022-08-01T00:00:00Z', '{"alice":1}',
        0, 0, 0, 0, 0, 0, 0, 0
    );
    $i$,
    'new row for relation "bob_stats" violates partition constraint',
    'you cannot insert an alice stat into the bob stats partition'
  );

  -- Drop priviledge to `authenticated` and authorize as Alice.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  return query select results_eq(
    $i$ select catalog_name::text, grain::text, flow_document::text from catalog_stats $i$,
    $i$ values ('aliceCo/hello','hourly','{"alice":1}') $i$,
    'alice can read alice stats only'
  );

end;
$$ language plpgsql;*/
