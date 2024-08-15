
create function tests.test_prune_unchanged_draft_specs()
returns setof text as $$
declare
  draft_id flowid;
  collection_spec json = '{"schema":{},"key":["/id"]}'::json;
begin

  insert into user_grants (user_id, object_role, capability) values
    ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin');

  insert into inferred_schemas (collection_name, schema, flow_document) values
    ('aliceCo/collA', '{"description": "collA has a schema"}', '{}'),
    ('aliceCo/collG', '{"description": "collG has a schema"}', '{}');

  insert into live_specs (catalog_name, spec_type, spec, inferred_schema_md5) values
    ('aliceCo/capA', 'capture', '{
      "endpoint": {
        "connector": {
          "image": "does not matter",
            "config": {"some": "config"}
          }
      },
      "bindings": []
    }', null),
    ('aliceCo/capB', 'capture', '{
      "endpoint": {
        "connector": {
          "image": "does not matter",
            "config": {"some": "config"}
          }
      },
      "bindings": []
    }', null),
    ('aliceCo/matA', 'materialization', '{
      "endpoint": {
        "connector": {
          "image": "does not matter",
            "config": {"some": "config"}
          }
      },
      "bindings": []
    }', null),
    ('aliceCo/collA', 'collection', collection_spec, 'different md5 that should be ignored'),
    ('aliceCo/collG', 'collection', collection_spec,
      (select md5 from inferred_schemas where collection_name = 'aliceCo/collG'));


  -- Drop privilege to `authenticated` and authorize as Alice.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  insert into drafts (detail) values ('test pruning') returning id into draft_id;

  insert into draft_specs (draft_id, catalog_name, spec_type, spec) values
    -- should be pruned because it's identical to the live spec
    (draft_id, 'aliceCo/capA', 'capture', '{
      "endpoint": {
        "connector": {
          "image": "does not matter",
            "config": {"some": "config"}
          }
      },
      "bindings": []
    }'),
    -- should be kept because the spec is different
    (draft_id, 'aliceCo/capB', 'capture', '{
      "endpoint": {
        "connector": {
          "image": "does not matter",
            "config": {"some": "NEW CONFIG"}
          }
      },
      "bindings": []
    }'),
    -- should be pruned because spec is identical.
    (draft_id, 'aliceCo/collA', 'collection', collection_spec),
    -- should keep because it is new
    (draft_id, 'aliceCo/collF', 'collection', collection_spec),
    -- should keep because spec changed (whitespace only change, in order to document that behavior)
    (draft_id, 'aliceCo/collG', 'collection', '{
      "schema":{ },
      "key": ["/id"]
    }');

  return query select set_eq(
    $i$ select catalog_name from prune_unchanged_draft_specs('$i$ || draft_id || $i$') $i$,
	'{aliceCo/capA, aliceCo/collA}'::text[]
  );

  return query select results_eq(
    $i$ select catalog_name::text from draft_specs where draft_id = '$i$ || draft_id || $i$' order by catalog_name $i$,
	$i$ values ('aliceCo/capB'),('aliceCo/collF'),('aliceCo/collG') $i$
  );

end;
$$ language plpgsql;
