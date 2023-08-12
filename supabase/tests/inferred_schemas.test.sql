create function tests.test_inferred_schemas()
returns setof text as $$
begin

  insert into user_grants (user_id, object_role, capability) values
    ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin'),
    ('22222222-2222-2222-2222-222222222222', 'bobCo/', 'admin')
  ;

  -- The `stats_loader` user materializes directly into the inferred_schemas table.
  set role stats_loader;
  insert into inferred_schemas (
    collection_name, schema, flow_document
  ) values
    ( 'aliceCo/hello', '{"const": "aliceCo"}', '{"alice":1}' ),
    ( 'bobCo/world',   '{"const": "bobCo"}',   '{"bob":1}' )
  ;

  -- Drop privilege to `authenticated` and authorize as Alice.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  return query select results_eq(
    $i$ select collection_name::text, schema::text, flow_document::text from inferred_schemas $i$,
    $i$ values ('aliceCo/hello', '{"const": "aliceCo"}', '{"alice":1}') $i$,
    'alice can read alice stats only'
  );

end;
$$ language plpgsql;
