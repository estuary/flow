create function tests.test_registered_avro_schemas()
returns setof text as $$

  insert into user_grants (user_id, object_role, capability) values
    ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'read'),
    ('22222222-2222-2222-2222-222222222222', 'bobCo/', 'read')
  ;

  delete from registered_avro_schemas;
  alter sequence registered_avro_schemas_registry_id_seq restart with 1;

  -- Insert schemas as Alice.
  select set_authenticated_context('11111111-1111-1111-1111-111111111111');

  insert into registered_avro_schemas (catalog_name, avro_schema) values
    ('aliceCo/foo', '{"type":"record","name":"hello","fields":[{"name":"alice","type":"int"}]}'),
    ('aliceCo/bar', '{"type":"string"}');

  -- Insert schemas as Bob.
  select set_authenticated_context('22222222-2222-2222-2222-222222222222');

  insert into registered_avro_schemas (catalog_name, avro_schema) values
    ('bobCo/baz', '{"type":"long"}'),
    ('bobCo/bing', '{"type":"string"}');

  -- Assert schemas visible to Alice.
  select set_authenticated_context('11111111-1111-1111-1111-111111111111');

  select results_eq(
    $i$ select catalog_name::text, registry_id, avro_schema_md5 from registered_avro_schemas order by catalog_name $i$,
    $i$ values  ('aliceCo/bar', 2, '2809284b6e54d0d34017715ffe5636bd'),
                ('aliceCo/foo', 1, '6fdea0e6b3acfece5ce250be461f6617')
    $i$,
    'alice schemas'
  );

  -- Assert schemas visible to Bob.
  select set_authenticated_context('22222222-2222-2222-2222-222222222222');

  select results_eq(
    $i$ select catalog_name::text, registry_id, avro_schema_md5 from registered_avro_schemas order by catalog_name $i$,
    $i$ values  ('bobCo/baz',  3, '509e9d5641b97707c7e6f51a91334755'),
                ('bobCo/bing', 4, '2809284b6e54d0d34017715ffe5636bd')
    $i$,
    'bob schemas'
  );

$$ language sql;
