create function tests.test_catalog_stats()
returns setof text as $$
begin

  insert into user_grants (user_id, object_role, capability) values
    ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin'),
    ('22222222-2222-2222-2222-222222222222', 'bobCo/', 'admin')
  ;

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

  -- Drop priviledge to `authenticated` and authorize as Alice.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  return query select results_eq(
    $i$ select catalog_name::text, grain::text, flow_document::text from catalog_stats $i$,
    $i$ values ('aliceCo/hello','hourly','{"alice":1}') $i$,
    'alice can read alice stats only'
  );

end;
$$ language plpgsql;
